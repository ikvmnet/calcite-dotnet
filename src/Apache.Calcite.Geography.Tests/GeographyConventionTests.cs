using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

using Xunit;

using DataContext = org.apache.calcite.DataContext;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The operators run under <c>ClrCursorConvention</c>, read either way, and answer what Calcite's own
    /// engine answers.
    /// </summary>
    /// <remarks>
    /// <see cref="GeographyExecutionTests"/> runs everything through <c>EnumerableConvention</c>, where the
    /// block is Java source compiled by Janino. This convention translates Calcite's tree into
    /// <c>System.Linq.Expressions</c> instead, so nothing about the first run says the second will work — the
    /// two reach a row, a cast and a method call by different machinery.
    ///
    /// <para>The geography type is what makes this worth pinning rather than assuming. Its carrier is a CLR
    /// class, so a scan of a geography column has to type its rows as that class and a call has to reach a
    /// method declared over it; <c>JavaTypeFactoryImpl.getJavaClass</c> answers <c>Geography</c> for the type
    /// because it is an ordinary <c>JavaType</c>, and a type that was not one would have answered
    /// <c>Object</c> and left every operand needing a cast the convention would have had to invent.</para>
    ///
    /// <para>Calcite is the oracle, as everywhere else here: each query is run through both conventions,
    /// the Clr one read both ways, and the rows must match. The comparison is on rendered values rather than objects, because one side
    /// reads through a <c>ResultSet</c> and the other takes the row as the plan built it.</para>
    ///
    /// <para>The package itself references no convention of this repository's — <c>Apache.Calcite.Geography</c> depends on
    /// nothing in this repository, deliberately. This test project references
    /// <c>Apache.Calcite.Extensions</c> so that the claim can be measured rather than argued.</para>
    /// </remarks>
    public class GeographyConventionTests
    {

        /// <summary>
        /// The queries every convention must agree on.
        /// </summary>
        /// <remarks>
        /// One with no table, to reach a constructor and a measurement; one carrying a geography column out of
        /// a scan and into a predicate, which is the case a carrier class could break and a bare geometry
        /// could not; one reading a column into an accessor; and one crossing out to Calcite's own planar
        /// function, which takes the geometry inside rather than the geography.
        /// </remarks>
        static readonly string[] queries =
        [
            "SELECT CLR_ST_GEOG_DISTANCE(CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), CLR_ST_GEOG_GEOMFROMTEXT('POINT(1 0)'))",
            "SELECT ID FROM GEO WHERE CLR_ST_GEOG_DWITHIN(GEOG, CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)",
            "SELECT ID, CLR_ST_GEOG_ASTEXT(GEOG) FROM GEO ORDER BY ID",
            "SELECT ID, CLR_ST_GEOG_X(GEOG), CLR_ST_GEOG_Y(GEOG) FROM GEO ORDER BY ID",
            "SELECT ST_DISTANCE(CLR_ST_GEOG_ASGEOM(GEOG), CLR_ST_GEOG_ASGEOM(GEOG)) FROM GEO WHERE ID = 1",
        ];

        /// <summary>
        /// How many rows the queries answer between them: one, one, two, two, one.
        /// </summary>
        const int ExpectedRows = 7;

        [Fact]
        public void ShouldAgreeWithCalciteInTheEnumerableConvention()
        {
            var differences = new List<string>();
            var rows = 0;

            foreach (var sql in queries)
            {
                var calcite = Rendered(GeographyExecutionTests.Run(sql));
                var ours = RunClr(sql);
                rows += ours.Count;

                if (Differs(calcite, ours, sql, out var difference))
                    differences.Add(difference);
            }

            differences.Should().BeEmpty(string.Join("\n", differences));

            // two empty answers agree with one another, so the row count is what says a comparison happened
            rows.Should().Be(ExpectedRows);
        }

        [Fact]
        public async Task ShouldAgreeWithCalciteWhenTheSamePlanIsAwaited()
        {
            var differences = new List<string>();
            var rows = 0;

            foreach (var sql in queries)
            {
                var calcite = Rendered(GeographyExecutionTests.Run(sql));
                var ours = await RunClrAsync(sql);
                rows += ours.Count;

                if (Differs(calcite, ours, sql, out var difference))
                    differences.Add(difference);
            }

            differences.Should().BeEmpty(string.Join("\n", differences));
            rows.Should().Be(ExpectedRows);
        }

        /// <summary>
        /// A geography column is typed by its carrier class all the way into the plan.
        /// </summary>
        /// <remarks>
        /// The rows the scan yields hold ordinary JTS geometries, and the block the
        /// convention builds reads them as that class. Pinning it here says the type survived planning rather
        /// than only validation.
        /// </remarks>
        [Fact]
        public void ShouldCarryTheCarrierClassThroughAPlan()
        {
            var (physical, _, _) = PlanClr("SELECT GEOG FROM GEO WHERE ID = 1");
            var column = ((RelDataTypeField)physical.getRowType().getFieldList().get(0)).getType();

            Rel.Type.GeographyTypes.IsGeometry(column).Should().BeTrue();
        }

        static bool Differs(List<string[]> calcite, List<string[]> ours, string sql, out string difference)
        {
            var left = string.Join(" | ", calcite.Select(r => string.Join(", ", r)));
            var right = string.Join(" | ", ours.Select(r => string.Join(", ", r)));

            difference = $"{sql}\n  Calcite: {left}\n  ours:    {right}";
            return left != right;
        }

        static List<string[]> Rendered(List<object?[]> rows)
        {
            return rows.Select(r => r.Select(GeographyAccessorTests.Render).ToArray()).ToList();
        }

        static (ClrCursorRel Plan, SchemaPlus Schema, java.util.Map Parameters) PlanClr(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("GEO", new GeographyExecutionTests.GeographyTable());

            var planner = Frameworks.getPlanner(Config(rootSchema, ClrCursorRules.Rules(), ClrCursorRules.CalcRules()));
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var traits = logical.getTraitSet().replace(ClrCursorConvention.Instance).simplify();

            return ((ClrCursorRel)planner.transform(0, traits, logical), rootSchema, new java.util.HashMap());
        }

        static Apache.Calcite.Extensions.Runtime.IClrCursorFactory Implement(ClrCursorRel physical, java.util.Map parameters)
        {
            return new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot(physical, ClrEnumerablePrefer.Array);
        }

        static List<string[]> RunClr(string sql)
        {
            var (physical, rootSchema, parameters) = PlanClr(sql);
            var factory = Implement(physical, parameters);

            var rows = new List<string[]>();
            using var cursor = factory.Open(new TestDataContext(rootSchema, parameters));
            while (cursor.Read())
                rows.Add((cursor.Current as object?[] ?? [cursor.Current]).Select(GeographyAccessorTests.Render).ToArray());

            return rows;
        }

        static async Task<List<string[]>> RunClrAsync(string sql)
        {
            // the same rules and the same plan as the synchronous run above; only the open differs
            var (physical, rootSchema, parameters) = PlanClr(sql);
            var factory = Implement(physical, parameters);

            var rows = new List<string[]>();
            await using var cursor = await factory.OpenAsync(new TestDataContext(rootSchema, parameters), default);
            while (await cursor.ReadAsync(default))
                rows.Add((cursor.Current as object?[] ?? [cursor.Current]).Select(GeographyAccessorTests.Render).ToArray());

            return rows;
        }

        /// <summary>
        /// The configuration a caller driving its own planner needs: the operator table, the convention's
        /// rules on the planner in front, and then <c>Programs.standard</c> with the calc pass after it.
        /// </summary>
        /// <param name="rootSchema"></param>
        /// <param name="rules"></param>
        /// <param name="calcRules"></param>
        /// <returns></returns>
        static FrameworkConfig Config(SchemaPlus rootSchema, IReadOnlyList<RelOptRule> rules, IReadOnlyList<RelOptRule> calcRules)
        {
            var calc = new java.util.ArrayList();
            foreach (var rule in calcRules)
                calc.add(rule);

            return Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .operatorTable(GeographyFixture.OperatorTable())
                .programs(
                    Programs.sequence(
                        new AddRules(rules),
                        Programs.standard(),
                        Programs.hep(calc, true, DefaultRelMetadataProvider.INSTANCE)))
                .build();
        }

        /// <summary>
        /// A pass that puts rules on the planner and changes the plan not at all.
        /// </summary>
        /// <param name="rules"></param>
        /// <remarks>
        /// A <c>Frameworks</c> planner carries Calcite's default rules and has never heard of the Clr
        /// convention, and <c>Programs.standard</c>'s planner pass installs nothing — so the rules go on in a
        /// pass of their own, in front. What <c>ClrPrepareImpl.CreatePlanner</c> does for a prepared
        /// statement, for a caller driving the planner itself.
        /// </remarks>
        sealed class AddRules(IReadOnlyList<RelOptRule> rules) : Program
        {

            public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
            {
                foreach (var rule in rules)
                    planner.addRule(rule);

                return rel;
            }

        }

        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            public SchemaPlus getRootSchema() => rootSchema;

            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            public object get(string name) => parameters.get(name);

        }

    }

}
