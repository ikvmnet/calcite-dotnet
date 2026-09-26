using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.DataCursor.Tests
{

    /// <summary>
    /// Runs a plan of this convention under a node of Calcite's, which is the one arrangement that makes
    /// Janino compile a call back into a stashed plan.
    /// </summary>
    /// <remarks>
    /// Every other mixed plan in the suite crosses the other way, with a converter <em>into</em> this
    /// convention reading a block Calcite generated. The first plan to cross out failed to compile twice
    /// over: the generated source declared the stashed plan by a class name linq4j writes with every
    /// <c>$</c> as <c>.</c>, which IKVM's mangled name for a generic instantiation cannot survive, and it
    /// called into a class IKVM exposes as package-private, which Janino silently drops as a candidate.
    /// The plan is stashed as an <c>Object</c> and <c>JavaPlans</c> is public, and this holds both.
    /// </remarks>
    public class ClrDataCursorToEnumerableConverterTests
    {

        static ClrDataCursorToEnumerableConverterTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// Plans a statement to end in Calcite's convention, with this convention's rules beside Calcite's.
        /// </summary>
        static RelNode Plan(string sql, SchemaPlus rootSchema, RelOptRule[]? remove = null)
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrDataCursorRules.Rules())
                rules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrDataCursorRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, remove: remove),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Renders a row so that two plans can be compared without caring which object holds a value.
        /// </summary>
        static string Render(object? row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Runs a plan that ends in Calcite's convention and returns its rows rendered as text.
        /// </summary>
        static List<string> Run(string sql, SchemaPlus rootSchema, out string plan, RelOptRule[]? remove = null)
        {
            var physical = Plan(sql, rootSchema, remove);
            plan = RelOptUtil.toString(physical);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();
            foreach (var row in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// A scan only this convention can read, under an aggregate of Calcite's, compiles and runs.
        /// </summary>
        /// <remarks>
        /// The table is of this project's SPI, which neither <c>EnumerableTableScan.canHandle</c> nor the
        /// bindable scan admits, so the scan can only be ours and the converter has to be in the plan.
        /// </remarks>
        [Fact]
        public void ShouldCarryACursorPlanUnderACalciteNode()
        {
            var sql = "SELECT REGION, SUM(AMOUNT) FROM ASALES WHERE ID > 1 GROUP BY REGION ORDER BY REGION";

            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("ASALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var rows = Run(sql, rootSchema, out var plan);

            plan.Should().Contain("ClrDataCursorToEnumerableConverter").And.Contain("ClrDataCursorTableScan");

            rows.Should().Equal(Run(sql.Replace("ASALES", "SALES"), rootSchema, out _));
        }

        /// <summary>
        /// A sub-plan of this convention under a correlate of Calcite's reads the outer row.
        /// </summary>
        /// <remarks>
        /// The outer row is a parameter of the Java lambda Calcite's correlate generates, and the sub-plan
        /// is compiled apart from that lambda and cannot see it: the converter reads the row's fields
        /// through the getter Calcite registered and hands them in through the context. The scans are ours
        /// because only this project's SPI reads the table; the correlate is Calcite's because this
        /// convention's correlate rule is taken out, so the only correlate the planner can build is
        /// Calcite's over converters out of ours.
        /// </remarks>
        [Fact]
        public void ShouldReadACorrelationVariableUnderCalcitesCorrelate()
        {
            var sql = "SELECT ID FROM ASALES S1 WHERE EXISTS (SELECT 1 FROM ASALES S2 WHERE S2.REGION = S1.REGION AND S2.ID > 3) ORDER BY ID";

            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("ASALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));

            var rows = Run(sql, rootSchema, out var plan, remove: [ClrDataCursorRules.ClrDataCursorCorrelateRule]);

            plan.Should().Contain("EnumerableCorrelate", plan).And.Contain("ClrDataCursorToEnumerableConverter", plan);

            rows.Should().Equal(Run(sql.Replace("ASALES", "SALES"), rootSchema, out _, remove: [ClrDataCursorRules.ClrDataCursorCorrelateRule]));
            rows.Should().NotBeEmpty();
        }

    }

}
