using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions.Adapter.Clr;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A method of the reflected object as a table macro, and the schema factory that builds a schema from a
    /// model file's operands.
    /// </summary>
    /// <remarks>
    /// The two members of <c>ReflectiveSchema</c> that are not tables: <c>createFunctionMap</c>, which takes
    /// every method returning a table, and <c>Factory</c>, which is how a schema is named in JSON rather than
    /// constructed in code.
    /// </remarks>
    [TestClass]
    public class ClrReflectiveMacroTests
    {

        static ClrReflectiveMacroTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        public record Employee(int Empid, string Name, int Deptno);

        public class HrSchema
        {

            public IReadOnlyList<Employee> Emps { get; } =
            [
                new Employee(100, "Bill", 10),
                new Employee(110, "Theodore", 10),
                new Employee(150, "Sebastian", 20),
                new Employee(200, "Eric", 20),
            ];

            /// <summary>
            /// A method returning a table, which is a macro rather than a table.
            /// </summary>
            public Table Above(int empid)
            {
                return ClrReflectiveSchema.Of(typeof(Employee), Emps.Where(e => e.Empid >= empid).ToList());
            }

            /// <summary>
            /// A method returning something that is not a table, which is not a macro and not an error.
            /// </summary>
            public int Count() => Emps.Count;

            /// <summary>
            /// The instance a model file would name.
            /// </summary>
            public static HrSchema Create() => new();

        }

        [TestMethod]
        public void ShouldExpandAMethodAsATableMacro()
        {
            Run("SELECT \"Name\" FROM TABLE(\"Above\"(150)) ORDER BY \"Name\"", new ClrReflectiveSchema(new HrSchema()))
                .Should().Equal("Eric", "Sebastian");
        }

        /// <summary>
        /// A macro is a macro: its argument is known when the plan is built, so the table it answers is the
        /// one the rest of the plan is built over.
        /// </summary>
        [TestMethod]
        public void ShouldFilterAndJoinTheTableAMacroAnswers()
        {
            Run("SELECT COUNT(*) FROM TABLE(\"Above\"(110)) WHERE \"Deptno\" = 20", new ClrReflectiveSchema(new HrSchema()))
                .Should().Equal("2");
        }

        /// <summary>
        /// A method that does not return a table is not a macro, and does not stop the schema being built.
        /// </summary>
        [TestMethod]
        public void ShouldIgnoreAMethodThatIsNotATable()
        {
            var schema = new ClrReflectiveSchema(new HrSchema());
            var plus = Frameworks.createRootSchema(true).add("HR", schema);

            plus.getFunctions("Above").size().Should().Be(1);
            plus.getFunctions("Count").size().Should().Be(0);
        }

        /// <summary>
        /// The factory builds the same schema from a type name and a static method.
        /// </summary>
        [TestMethod]
        public void ShouldBuildASchemaFromOperands()
        {
            var operand = new java.util.HashMap();
            operand.put("type", typeof(HrSchema).AssemblyQualifiedName);
            operand.put("staticMethod", nameof(HrSchema.Create));

            var schema = new ClrReflectiveSchemaFactory().create(Frameworks.createRootSchema(true), "HR", operand);

            schema.Should().BeOfType<ClrReflectiveSchema>();
            Run("SELECT COUNT(*) FROM \"Emps\"", schema).Should().Equal("4");
        }

        /// <summary>
        /// And without a static method, by constructing the type.
        /// </summary>
        [TestMethod]
        public void ShouldBuildASchemaByConstructingTheType()
        {
            var operand = new java.util.HashMap();
            operand.put("type", typeof(HrSchema).AssemblyQualifiedName);

            var schema = new ClrReflectiveSchemaFactory().create(Frameworks.createRootSchema(true), "HR", operand);

            Run("SELECT COUNT(*) FROM \"Emps\"", schema).Should().Equal("4");
        }

        /// <summary>
        /// A type it cannot find is named, rather than answering an empty schema.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseATypeThatIsNotThere()
        {
            var operand = new java.util.HashMap();
            operand.put("type", "No.Such.Type, Nowhere");

            var create = () => new ClrReflectiveSchemaFactory().create(Frameworks.createRootSchema(true), "HR", operand);

            create.Should().Throw<java.lang.IllegalArgumentException>().WithMessage("*No.Such.Type*");
        }

        static List<string> Run(string sql, org.apache.calcite.schema.Schema schema)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            var added = rootSchema.add("HR", schema);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(added)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, false, false, null, null),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new MacroDataContext(rootSchema, parameters);
            var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            var rows = new List<string>();
            foreach (var row in bindable.Bind(context))
                rows.Add(row is object?[] array ? string.Join("|", array.Select(x => x?.ToString() ?? "null")) : row?.ToString() ?? "null");

            return rows;
        }

        sealed class MacroDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

    }

}
