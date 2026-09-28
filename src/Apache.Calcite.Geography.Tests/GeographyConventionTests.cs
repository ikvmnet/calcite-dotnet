using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;

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
    /// Runs the operators under <c>ClrCursorConvention</c>, read synchronously and awaited, and requires the
    /// rows Calcite's <c>EnumerableConvention</c> gives.
    /// </summary>
    /// <remarks>
    /// <see cref="GeographyExecutionTests"/> runs through <c>EnumerableConvention</c>, which compiles Java
    /// source with Janino; <c>ClrCursorConvention</c> translates Calcite's tree into
    /// <c>System.Linq.Expressions</c>, so rows, casts and method calls are reached by different code. Values
    /// are compared rendered as text, because one side reads through a <c>ResultSet</c> and the other takes
    /// the row as the plan built it.
    ///
    /// <para><c>Apache.Calcite.Geography</c> references nothing else in this repository; only this test
    /// project references <c>Apache.Calcite.Extensions</c>.</para>
    /// </remarks>
    public class GeographyConventionTests
    {

        /// <summary>
        /// The queries both conventions must agree on.
        /// </summary>
        /// <remarks>
        /// A constructor and a measurement with no table; a geometry column carried from a scan into a
        /// predicate; column values passed to a serializer and to accessors; and a call to Calcite's own
        /// planar <c>ST_DISTANCE</c>.
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
        /// The total number of rows the queries return: one, one, two, two and one.
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

            // Two empty answers would agree, so the row count shows that rows were compared.
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
        /// A geometry column keeps its geometry type in the physical plan, not only after validation.
        /// </summary>
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
            return new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot(physical, ClrCursorPrefer.Array);
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
            // The same plan as RunClr; only the open differs.
            var (physical, rootSchema, parameters) = PlanClr(sql);
            var factory = Implement(physical, parameters);

            var rows = new List<string[]>();
            await using var cursor = await factory.OpenAsync(new TestDataContext(rootSchema, parameters), default);
            while (await cursor.ReadAsync(default))
                rows.Add((cursor.Current as object?[] ?? [cursor.Current]).Select(GeographyAccessorTests.Render).ToArray());

            return rows;
        }

        /// <summary>
        /// Builds a planner configuration over the given schema: the fixture's operator table, and a program
        /// that adds <paramref name="rules"/> to the planner, runs <c>Programs.standard</c>, and then runs
        /// <paramref name="calcRules"/> as a hep pass.
        /// </summary>
        /// <param name="rootSchema">The schema queries are resolved against.</param>
        /// <param name="rules">The rules added to the planner before <c>Programs.standard</c> runs.</param>
        /// <param name="calcRules">The rules run as a hep pass after <c>Programs.standard</c>.</param>
        /// <returns>The configuration to build a <c>Frameworks</c> planner from.</returns>
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
        /// A program that adds rules to the planner and returns the plan unchanged.
        /// </summary>
        /// <remarks>
        /// A <c>Frameworks</c> planner carries only Calcite's default rules, and <c>Programs.standard</c>'s
        /// planner pass adds none, so the convention's rules are added by a pass that runs first.
        /// </remarks>
        /// <param name="rules">The rules added to the planner each time the program runs.</param>
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
