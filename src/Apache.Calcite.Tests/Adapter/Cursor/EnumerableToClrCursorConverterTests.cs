using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Linq4j;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests plans that hold nodes of both this convention and <c>EnumerableConvention</c>, crossing in each
    /// direction.
    /// </summary>
    /// <remarks>
    /// Both conventions ask the same <c>JavaTypeFactory</c> for a field's type, so a row crosses a converter
    /// without being converted.
    /// </remarks>
    public class EnumerableToClrCursorConverterTests
    {

        /// <summary>
        /// A table of three rows.
        /// </summary>
        sealed class PeopleTable : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                list.add(new object[] { java.lang.Integer.valueOf(1), "SMITH" });
                list.add(new object[] { java.lang.Integer.valueOf(2), "JONES" });
                list.add(new object[] { java.lang.Integer.valueOf(3), "BROWN" });

                return org.apache.calcite.linq4j.Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        sealed class TestDataContext(SchemaPlus rootSchema) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => null!;

        }

        /// <summary>
        /// Plans a query with both conventions' rules registered and returns its rows.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="root">The convention the plan's root is requested in.</param>
        /// <returns>The rows, a one-column result wrapped in a one-element array. A root in this convention is
        /// read through its synchronous open; one in <c>EnumerableConvention</c> is bound and enumerated.</returns>
        static List<object[]> Run(string sql, Convention root)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            // both rule sets, so the planner may put any node in either convention and bridge between them
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // both calc rule sets, because neither convention can implement a project that is not rewritten to a calc
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.ofRules(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            var chosen = planner.transform(0, planner.getEmptyTraitSet().replace(root), logical);
            var physical = planner.transform(1, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema);

            var rows = new List<object[]>();
            if (physical is ClrCursorRel clr)
            {
                var factory = new ClrCursorRelImplementor(clr.getCluster().getRexBuilder(), parameters).ImplementRoot(clr, ClrCursorPrefer.Array);

                using var cursor = factory.Open(context);
                while (cursor.Read())
                    rows.Add(cursor.Current as object[] ?? [cursor.Current!]);

                return rows;
            }

            foreach (var current in TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context))
                rows.Add(current as object[] ?? [current]);

            return rows;
        }

        [Fact]
        public void ShouldEndInThisConvention()
        {
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1", ClrCursorConvention.Instance);

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        /// <summary>
        /// Plans a query with Calcite's rules and this convention's converter rule only, so that the whole plan
        /// is in <c>EnumerableConvention</c> under one converter, and returns its rows.
        /// </summary>
        /// <param name="sql">The query, over the <c>PEOPLE</c> table.</param>
        /// <returns>The rows, a one-column result wrapped in a one-element array.</returns>
        /// <remarks>
        /// With both rule sets, as in <see cref="Run"/>, this convention takes nearly every node and the converter
        /// sees only a scan. Here the converter translates the block a generated calc produces, which holds an
        /// anonymous <c>Enumerator</c> of four methods over shared fields rather than a single lambda.
        /// </remarks>
        static List<object[]> RunAcrossConverter(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // only the converter rule of this convention, so no node can be implemented in it
            rules.add(Apache.Calcite.Extensions.Adapter.Cursor.EnumerableToClrCursorConverterRule.Create());

            var calcRules = new java.util.ArrayList();
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.ofRules(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var chosen = planner.transform(0, planner.getEmptyTraitSet().replace(ClrCursorConvention.Instance), logical);
            var physical = planner.transform(1, chosen.getTraitSet(), chosen);

            var rows = new List<object[]>();
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), new java.util.HashMap()).ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

            using var cursor = factory.Open(new TestDataContext(rootSchema));
            while (cursor.Read())
                rows.Add(cursor.Current as object[] ?? [cursor.Current!]);

            return rows;
        }

        [Fact]
        public void ShouldCarryACalcAcrossTheConverter()
        {
            var rows = RunAcrossConverter("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1");

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        [Fact]
        public void ShouldEndInCalcitesConvention()
        {
            // the root is requested in EnumerableConvention, so whatever is planned in this convention is read
            // back across a converter
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1", EnumerableConvention.INSTANCE);

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        /// <summary>
        /// Plans a query so that a projection of this convention sits above a converter out of
        /// <c>EnumerableConvention</c>, the shape an adapter produces when it pushes only part of a projection,
        /// and returns the plan with either its rows or the error implementing it raised.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="ourCalcPass">Whether to run this convention's calc rules after the planner.</param>
        /// <returns>The plan's text, and either its rows or, if implementing or reading the plan threw, the
        /// exception's message with the rows null.</returns>
        /// <remarks>
        /// Only the project rule and the converter rule of this convention are registered, so the scan must stay
        /// in <c>EnumerableConvention</c> while the projection may move, and a converter appears between them.
        ///
        /// <para><c>RelOptRules.CALC_RULES</c> runs either way, as in <c>Programs.standard</c>, but it cannot
        /// rewrite this convention's project: <c>ENUMERABLE_PROJECT_TO_CALC_RULE</c> matches
        /// <c>EnumerableProject</c> and <c>CoreRules.PROJECT_TO_CALC</c> matches <c>LogicalProject</c>. Only
        /// <see cref="ClrCursorRules.CalcRules"/> has a rule that matches <see cref="ClrCursorProject"/>.</para>
        /// </remarks>
        static (string Plan, List<object[]>? Rows, string? Error) PlanResidualOverConverter(string sql, bool ourCalcPass)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // only the projection can move into this convention, so the scan stays in Calcite's
            rules.add(ClrCursorRules.ClrCursorProjectRule);
            rules.add(ClrCursorRules.EnumerableToClrCursorConverterRule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var ourCalcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                ourCalcRules.add(rule);

            var programs = new List<Program>
            {
                Programs.ofRules(rules),
                Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
            };

            if (ourCalcPass)
                programs.Add(Programs.hep(ourCalcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE));

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(programs.ToArray())
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();

            var physical = planner.transform(0, planner.getEmptyTraitSet().replace(ClrCursorConvention.Instance), logical);
            for (int i = 1; i < programs.Count; i++)
                physical = planner.transform(i, physical.getTraitSet(), physical);

            var plan = RelOptUtil.toString(physical);

            try
            {
                var rows = new List<object[]>();
                var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), new java.util.HashMap()).ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);
                using var cursor = factory.Open(new TestDataContext(rootSchema));
                while (cursor.Read())
                    rows.Add(cursor.Current as object[] ?? [cursor.Current!]);

                return (plan, rows, null);
            }
            catch (Exception e)
            {
                return (plan, null, e.Message);
            }
        }

        /// <summary>
        /// A projection of this convention left above a converter out of another is rewritten to a calc by
        /// this convention's calc pass, and the plan then runs.
        /// </summary>
        /// <remarks>
        /// Other mixed-convention plans in the suite either push the whole projection or leave the residual in
        /// Calcite's convention, so this is the only test of a residual projection in this one.
        /// </remarks>
        [Fact]
        public void ShouldRewriteAResidualProjectOverAConverter()
        {
            var (plan, rows, error) = PlanResidualOverConverter(
                "SELECT CAST(\"ID\" AS DOUBLE) AS \"X\" FROM \"PEOPLE\"", ourCalcPass: true);

            plan.Should().Contain("ClrCursorCalc", "the residual is a calc: " + plan);
            plan.Should().NotContain("ClrCursorProject", "nothing unimplementable is left: " + plan);
            plan.Should().Contain("EnumerableToClrCursorConverter", "the converter is still there: " + plan);

            error.Should().BeNull();

            // java.lang.Double, not System.Double: a value keeps the Java boxing Calcite's type factory gives it
            rows!.Select(r => r[0]).Should().AllBeOfType<java.lang.Double>();
            rows!.Select(r => r[0]!.ToString()).Should().BeEquivalentTo(["1.0", "2.0", "3.0"]);
        }

        /// <summary>
        /// Without this convention's calc pass the same plan keeps its projection, and the projection
        /// refuses to implement itself.
        /// </summary>
        /// <remarks>
        /// A caller driving its own planner must therefore run this convention's calc pass; Calcite's calc pass
        /// runs here and matches no node of this convention. <c>EnumerableProject</c> likewise refuses to
        /// implement itself.
        /// </remarks>
        [Fact]
        public void ShouldRefuseAResidualProjectWithoutTheCalcPass()
        {
            var (plan, rows, error) = PlanResidualOverConverter(
                "SELECT CAST(\"ID\" AS DOUBLE) AS \"X\" FROM \"PEOPLE\"", ourCalcPass: false);

            plan.Should().Contain("ClrCursorProject", "the projection survives Calcite's calc pass: " + plan);

            rows.Should().BeNull();
            error.Should().Contain("Unable to implement ClrCursorProject");
        }

    }

}
