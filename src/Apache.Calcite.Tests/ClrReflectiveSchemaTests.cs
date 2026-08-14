using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions.Adapter.Clr;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs queries over a schema reflected from a .NET object, and requires the same rows as the same data
    /// held by a <see cref="ScannableTable"/>.
    /// </summary>
    /// <remarks>
    /// The oracle is a table of Calcite's own SPI holding the identical rows already broken out into
    /// <c>object?[]</c>, which <c>ClrEnumerableDifferentialTests</c> already checks against Calcite itself. So
    /// what is under test is the route — a row that is a .NET object, a column that is a .NET property — and
    /// not the operators above it.
    ///
    /// <para>Calcite cannot be the oracle directly here: <c>ReflectiveSchema</c> over the same object gives a
    /// schema of no columns, because IKVM shows Java nothing of a property. That is the whole reason this
    /// exists, and <see cref="ClrRecordTypeTests"/> pins it.</para>
    /// </remarks>
    [TestClass]
    public class ClrReflectiveSchemaTests
    {

        static ClrReflectiveSchemaTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A row, written the way a .NET row is written: a positional record of properties.
        /// </summary>
        /// <remarks>
        /// A record because a row of a custom type is built by calling a constructor of every column —
        /// <c>JavaRowFormat.CUSTOM.record</c> emits <c>new_(rowClass, expressions)</c> — and a record is the
        /// .NET type that has one. A type of settable properties and no such constructor can be read from and
        /// cannot be rebuilt, which is a limit worth having a name for before something hits it.
        /// </remarks>
        public record Employee(int Empid, string Name, int Deptno);

        /// <summary>
        /// The object a schema is reflected from.
        /// </summary>
        public class HrSchema
        {

            public IReadOnlyList<Employee> Emps { get; } =
            [
                new Employee(100, "Bill", 10),
                new Employee(110, "Theodore", 10),
                new Employee(150, "Sebastian", 20),
                new Employee(200, "Eric", 20),
            ];

        }

        /// <summary>
        /// The same rows, already broken out, read through Calcite's own SPI.
        /// </summary>
        sealed class EmpsTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows = new HrSchema().Emps
                .Select(e => new object?[] { java.lang.Integer.valueOf(e.Empid), e.Name, java.lang.Integer.valueOf(e.Deptno) })
                .ToArray();

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("Empid", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("Name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("Deptno", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root) => Linq4j.asEnumerable(Rows);

        }

        [TestMethod]
        public void ShouldProjectAColumnThatIsAProperty()
        {
            Both("SELECT \"Empid\" FROM \"Emps\"");
        }

        [TestMethod]
        public void ShouldProjectEveryColumn()
        {
            // reordered, so that it is a projection rather than the identity one below
            Both("SELECT \"Deptno\", \"Name\", \"Empid\" FROM \"Emps\"");
        }

        /// <summary>
        /// A query that projects nothing hands back the objects, and hands back the same ones.
        /// </summary>
        /// <remarks>
        /// The half of this that a table of <c>object?[]</c> cannot give. The row type is the members broken
        /// out, so the statement is ordinary SQL over three columns; the row is the object, so when the
        /// planner works out that the projection is the identity there is no row to build and the sequence
        /// arrives as it left. Reference equality is the assertion worth making — an <see cref="Employee"/>
        /// that merely compares equal would mean a copy was made somewhere, which is the thing this design
        /// exists to avoid.
        /// </remarks>
        [TestMethod]
        public void ShouldHandBackTheObjectsThemselves()
        {
            var schema = new HrSchema();
            var rows = RunRaw("SELECT \"Empid\", \"Name\", \"Deptno\" FROM \"Emps\"", new ClrReflectiveSchema(schema));

            rows.Should().HaveCount(schema.Emps.Count);
            for (int i = 0; i < rows.Count; i++)
                rows[i].Should().BeSameAs(schema.Emps[i]);
        }

        [TestMethod]
        public void ShouldFilterOnAProperty()
        {
            Both("SELECT \"Name\" FROM \"Emps\" WHERE \"Deptno\" = 20");
        }

        [TestMethod]
        public void ShouldSortOnAProperty()
        {
            Both("SELECT \"Name\", \"Empid\" FROM \"Emps\" ORDER BY \"Name\"");
        }

        [TestMethod]
        public void ShouldAggregateOverAProperty()
        {
            Both("SELECT \"Deptno\", COUNT(*), SUM(\"Empid\") FROM \"Emps\" GROUP BY \"Deptno\"");
        }

        /// <summary>
        /// A row of one column, which is the shape that has broken this convention twice.
        /// </summary>
        /// <remarks>
        /// <c>JavaRowFormat.optimize</c> is what makes it interesting: a one field row can leave the scan
        /// wanting a format its element type does not have, and then the scan reformats rather than passing
        /// the sequence along — the one path in <c>ClrEnumerableTableScan</c> that still declares its row
        /// parameter from the element class rather than from the physical type.
        /// </remarks>
        [TestMethod]
        public void ShouldReadARowOfOneColumn()
        {
            var schema = new OneColumnSchema();
            var rows = Run("SELECT \"N\" FROM \"Ones\" ORDER BY \"N\"", new ClrReflectiveSchema(schema));

            rows.Should().Equal("1", "2", "3");
        }

        public record One(int N);

        public class OneColumnSchema
        {

            public IReadOnlyList<One> Ones { get; } = [new One(3), new One(1), new One(2)];

        }

        [TestMethod]
        public void ShouldWindowOverAProperty()
        {
            Both("SELECT \"Name\", SUM(\"Empid\") OVER (PARTITION BY \"Deptno\" ORDER BY \"Empid\") FROM \"Emps\"");
        }

        [TestMethod]
        public void ShouldJoinATableToItself()
        {
            Both("SELECT \"e\".\"Name\", \"m\".\"Name\" FROM \"Emps\" AS \"e\" JOIN \"Emps\" AS \"m\" ON \"e\".\"Deptno\" = \"m\".\"Deptno\" AND \"e\".\"Empid\" < \"m\".\"Empid\"");
        }

        /// <summary>
        /// Runs the same statement over the reflected schema and over the flattened one, and requires the same
        /// rows of both.
        /// </summary>
        /// <param name="sql"></param>
        static void Both(string sql)
        {
            var reflected = Run(sql, new ClrReflectiveSchema(new HrSchema()));
            var flattened = Run(sql, Flat());

            reflected.Should().Equal(flattened);
        }

        /// <summary>
        /// A schema of one table, holding the same rows already broken out.
        /// </summary>
        static Schema Flat()
        {
            var tables = new java.util.HashMap();
            tables.put("Emps", new EmpsTable());

            return new FlatSchema(tables);
        }

        sealed class FlatSchema(java.util.Map map) : AbstractSchema
        {

            protected override java.util.Map getTableMap() => map;

        }

        static List<string> Run(string sql, Schema schema)
        {
            return RunRaw(sql, schema).Select(Render).ToList();
        }

        static List<object> RunRaw(string sql, Schema schema)
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
            var context = new ReflectDataContext(rootSchema, parameters);
            var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            var rows = new List<object>();
            foreach (var row in bindable.Bind(context))
                rows.Add(row);

            return rows;
        }

        static string Render(object? row)
        {
            if (row is object?[] array)
                return string.Join("|", array.Select(x => x?.ToString() ?? "null"));

            return row?.ToString() ?? "null";
        }

        sealed class ReflectDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
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
