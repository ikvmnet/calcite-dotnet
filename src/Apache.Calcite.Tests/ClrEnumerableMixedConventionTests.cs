using System;
using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

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
using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Linq4j;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a plan that holds nodes of both calling conventions.
    /// </summary>
    /// <remarks>
    /// A row crosses the boundary untouched, because both conventions ask the same <c>JavaTypeFactory</c> what
    /// a field is. These run a plan where the two are mixed, in each direction, to say so.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableMixedConventionTests
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

                return Linq4j.asEnumerable(list);
            }

        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema"></param>
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
        /// Plans a query with both conventions available and returns its rows.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="root">Which convention the plan is asked to end in.</param>
        /// <returns></returns>
        static List<object[]> Run(string sql, Convention root)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            // both rule sets, so the planner may put any node in either convention and bridge between them
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // both calc rule sets, because a project of either convention refuses to implement itself
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
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
            var source = physical is ClrEnumerableRel clr
                ? TestRows.Of(ClrEnumerableInterpretable.ToBindable(parameters, clr, ClrEnumerablePrefer.Array), context)
                : TestRows.Of(EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY), context);

            var rows = new List<object[]>();
            foreach (var current in source)
            {
                rows.Add(current as object[] ?? [current]);
            }

            return rows;
        }

        [TestMethod]
        public void ShouldEndInThisConvention()
        {
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1", ClrEnumerableConvention.Instance);

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        /// <summary>
        /// Plans a query with only Calcite's rules, so that the whole of it lands in
        /// <c>EnumerableConvention</c> and the converter has to carry it.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="Run"/> gives the planner both rule sets, and this convention wins nearly everything, so
        /// the converter only ever sees a bare scan there. What it actually has to survive is the block a
        /// generated node produces, and a calc's holds an anonymous <c>Enumerator</c>: four methods over a
        /// field, which is not a lambda and was refused outright until <c>DelegateEnumerator</c> existed.
        /// </remarks>
        static List<object[]> RunAcrossConverter(string sql)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // the one rule of this convention, so a plan of Calcite's can be read as one of ours and nothing
            // else of ours can claim a node
            rules.add(Apache.Calcite.Extensions.Adapter.Enumerable.EnumerableToClrEnumerableConverterRule.Create());

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
            var chosen = planner.transform(0, planner.getEmptyTraitSet().replace(ClrEnumerableConvention.Instance), logical);
            var physical = planner.transform(1, chosen.getTraitSet(), chosen);

            var rows = new List<object[]>();
            var bindable = ClrEnumerableInterpretable.ToBindable(new java.util.HashMap(), (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            foreach (var current in bindable.Bind(new TestDataContext(rootSchema)))
                rows.Add(current as object[] ?? [current]);

            return rows;
        }

        [TestMethod]
        public void ShouldCarryACalcAcrossTheConverter()
        {
            var rows = RunAcrossConverter("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1");

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        [TestMethod]
        public void ShouldEndInCalcitesConvention()
        {
            // the plan is asked to end in EnumerableConvention, so whatever of it lands in this one has to be
            // read back across the boundary
            var rows = Run("SELECT \"ID\", \"NAME\" FROM \"PEOPLE\" WHERE \"ID\" > 1", EnumerableConvention.INSTANCE);

            rows.Select(r => (string)r[1]).Should().BeEquivalentTo(["JONES", "BROWN"]);
        }

        /// <summary>
        /// Plans a query so that a projection of this convention is left sitting above a converter out of
        /// another, which is the shape an adapter that pushes only part of a projection produces.
        /// </summary>
        /// <param name="ourCalcPass">Whether to run this convention's calc rules after the planner.</param>
        /// <remarks>
        /// <see cref="RunAcrossConverter"/> leaves the residual on Calcite's side of the boundary, so it
        /// never produces a <see cref="ClrEnumerableProject"/> at all. Two rules of ours and no more is what
        /// forces the other arrangement: the scan cannot leave <c>EnumerableConvention</c> and the projection
        /// can, so a converter has to appear between them.
        ///
        /// <para><c>RelOptRules.CALC_RULES</c> runs either way, as <c>Programs.standard</c> runs it. It
        /// rewrites Calcite's half and cannot touch ours: <c>ENUMERABLE_PROJECT_TO_CALC_RULE</c> names
        /// <c>EnumerableProject</c> and <c>CoreRules.PROJECT_TO_CALC</c> names <c>LogicalProject</c>. Only
        /// <see cref="ClrEnumerableRules.CalcRules"/> carries a rule that names ours.</para>
        /// </remarks>
        static (string Plan, List<object[]>? Rows, string? Error) PlanResidualOverConverter(string sql, bool ourCalcPass)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("PEOPLE", new PeopleTable());

            var rules = new java.util.ArrayList();
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // the projection may join this convention and nothing else may, so the scan stays Calcite's
            rules.add(ClrEnumerableRules.ClrEnumerableProjectRule);
            rules.add(ClrEnumerableRules.EnumerableToClrEnumerableConverterRule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var ourCalcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
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

            var physical = planner.transform(0, planner.getEmptyTraitSet().replace(ClrEnumerableConvention.Instance), logical);
            for (int i = 1; i < programs.Count; i++)
                physical = planner.transform(i, physical.getTraitSet(), physical);

            var plan = RelOptUtil.toString(physical);

            try
            {
                var rows = new List<object[]>();
                var bindable = ClrEnumerableInterpretable.ToBindable(new java.util.HashMap(), (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
                foreach (var current in bindable.Bind(new TestDataContext(rootSchema)))
                    rows.Add(current as object[] ?? [current]);

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
        /// The shape an adapter produces when it pushes part of a projection and keeps the rest. Nothing
        /// else in the suite reaches it: every other mixed-convention plan here either pushes the whole
        /// projection or leaves the residual on Calcite's side.
        /// </remarks>
        [TestMethod]
        public void ShouldRewriteAResidualProjectOverAConverter()
        {
            var (plan, rows, error) = PlanResidualOverConverter(
                "SELECT CAST(\"ID\" AS DOUBLE) AS \"X\" FROM \"PEOPLE\"", ourCalcPass: true);

            plan.Should().Contain("ClrEnumerableCalc", "the residual is a calc: " + plan);
            plan.Should().NotContain("ClrEnumerableProject", "nothing unimplementable is left: " + plan);
            plan.Should().Contain("EnumerableToClrEnumerableConverter", "the converter is still there: " + plan);

            error.Should().BeNull();

            // java.lang.Double, not System.Double: a row crossing the converter keeps the boxing Calcite's
            // type factory gave it, which is the invariant the whole port is for
            rows!.Select(r => r[0]).Should().AllBeOfType<java.lang.Double>();
            rows!.Select(r => r[0]!.ToString()).Should().BeEquivalentTo(["1.0", "2.0", "3.0"]);
        }

        /// <summary>
        /// Without this convention's calc pass the same plan keeps its projection, and the projection
        /// refuses to implement itself.
        /// </summary>
        /// <remarks>
        /// The companion of <see cref="ShouldRewriteAResidualProjectOverAConverter"/>, and the reason the
        /// calc pass is not optional for a caller driving its own planner. Calcite's own calc pass runs
        /// here and cannot help: none of its rules names a node of this convention.
        /// <c>EnumerableProject</c> refuses in exactly the same way for exactly the same reason.
        /// </remarks>
        [TestMethod]
        public void ShouldRefuseAResidualProjectWithoutTheCalcPass()
        {
            var (plan, rows, error) = PlanResidualOverConverter(
                "SELECT CAST(\"ID\" AS DOUBLE) AS \"X\" FROM \"PEOPLE\"", ourCalcPass: false);

            plan.Should().Contain("ClrEnumerableProject", "the projection survives Calcite's calc pass: " + plan);

            rows.Should().BeNull();
            error.Should().Contain("Unable to implement ClrEnumerableProject");
        }

    }

}
