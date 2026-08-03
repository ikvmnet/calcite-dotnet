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

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Runs the same query through this convention and through Calcite's, and requires the same rows.
    /// </summary>
    /// <remarks>
    /// A node here can be wrong in a way no assertion written by hand would catch, because the expected answer
    /// is whatever Calcite says it is. Asking Calcite is cheap now that both conventions run side by side, and
    /// it is the only check that scales to a node like Window, where a detail wrong gives a wrong answer rather
    /// than a failure.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableDifferentialTests
    {

        /// <summary>
        /// A table with partitions, ties, nulls and an order, so that a window has something to disagree over.
        /// </summary>
        sealed class SalesTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "A"],
                [java.lang.Integer.valueOf(2), "EAST", java.lang.Integer.valueOf(20), "B"],
                [java.lang.Integer.valueOf(3), "EAST", java.lang.Integer.valueOf(20), "C"],
                [java.lang.Integer.valueOf(4), "WEST", java.lang.Integer.valueOf(30), "D"],
                [java.lang.Integer.valueOf(5), "WEST", null, "E"],
                [java.lang.Integer.valueOf(6), "WEST", java.lang.Integer.valueOf(5), "F"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("REGION", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("AMOUNT", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("LABEL", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in Rows)
                    list.add(row);

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
        /// Runs a query in one convention and returns its rows rendered as text.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="clr">Whether to plan into this convention or into Calcite's.</param>
        /// <returns></returns>
        static List<string> Run(string sql, bool clr)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SalesTable());

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            if (clr)
            {
                foreach (var rule in ClrEnumerableRules.Rules())
                    rules.add(rule);
                foreach (var rule in ClrEnumerableRules.CalcRules())
                    calcRules.add(rule);
            }

            // Calcite's rules go in either way: where this convention has no node for something, the planner
            // takes Calcite's and the converters carry the rows across
            foreach (var rule in EnumerableRules.ENUMERABLE_RULES.toArray())
                rules.add(rule);

            // AVG has no implementor of its own; a real program reduces it to SUM over COUNT first, and this
            // rule lives in RelOptRules.BASE_RULES rather than in any convention's set
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

            // a project holding an OVER is refused by both conventions; it becomes a LogicalWindow first, and
            // that rule likewise lives outside any convention's set
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    Programs.ofRules(rules),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = clr ? (Convention)ClrEnumerableConvention.Instance : EnumerableConvention.INSTANCE;
            var chosen = planner.transform(1, planner.getEmptyTraitSet().replace(convention), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var bindable = physical is ClrEnumerableRel node
                ? ClrInterpretable.ToBindable(parameters, node, EnumerableRel.Prefer.ARRAY)
                : EnumerableInterpretable.toBindable(parameters, null, (EnumerableRel)physical, EnumerableRel.Prefer.ARRAY);

            var rows = new List<string>();
            var enumerator = bindable.bind(new TestDataContext(rootSchema)).enumerator();
            while (enumerator.moveNext())
                rows.Add(Render(enumerator.current()));

            return rows;
        }

        /// <summary>
        /// Renders a row so that two conventions can be compared without caring which object holds a value.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions.
        /// </summary>
        /// <param name="sql"></param>
        static void Same(string sql)
        {
            var mine = Run(sql, true);
            var calcite = Run(sql, false);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives", sql);
        }

        [TestMethod]
        public void ShouldAgreeOnAScan() => Same("SELECT \"ID\", \"REGION\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAFilterAndProjection() => Same("SELECT \"ID\", \"AMOUNT\" + 1 FROM \"SALES\" WHERE \"AMOUNT\" > 10 ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnANullableExpression() => Same("SELECT \"ID\", \"AMOUNT\" + \"ID\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSortingWithNulls() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"AMOUNT\", \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSortingDescendingWithNulls() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"AMOUNT\" DESC, \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAGroupBy() => Same("SELECT \"REGION\", COUNT(*), SUM(\"AMOUNT\"), MIN(\"AMOUNT\"), MAX(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAGlobalAggregate() => Same("SELECT COUNT(*), SUM(\"AMOUNT\"), AVG(\"AMOUNT\") FROM \"SALES\"");

        [TestMethod]
        public void ShouldAgreeOnAnInnerJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN (SELECT * FROM \"SALES\" WHERE \"AMOUNT\" > 25) b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAJoinWithAnInequality() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnUnionAll() => Same("SELECT \"ID\" FROM \"SALES\" UNION ALL SELECT \"ID\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnUnionDistinct() => Same("SELECT \"REGION\" FROM \"SALES\" UNION SELECT \"REGION\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnIntersect() => Same("SELECT \"REGION\" FROM \"SALES\" INTERSECT SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnExcept() => Same("SELECT \"REGION\" FROM \"SALES\" EXCEPT SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 4 ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnLimitAndOffset() => Same("SELECT \"ID\" FROM \"SALES\" ORDER BY \"ID\" OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnValues() => Same("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        [TestMethod]
        public void ShouldAgreeOnACorrelatedSubQuery() => Same("SELECT \"ID\" FROM \"SALES\" a WHERE \"AMOUNT\" = (SELECT MAX(\"AMOUNT\") FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAScalarSubQuery() => Same("SELECT \"ID\", (SELECT COUNT(*) FROM \"SALES\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnCaseAndNullHandling() => Same("SELECT \"ID\", CASE WHEN \"AMOUNT\" IS NULL THEN -1 ELSE \"AMOUNT\" END FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnRowNumber() => Same("SELECT \"ID\", ROW_NUMBER() OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnRankOverTies() => Same("SELECT \"ID\", RANK() OVER (ORDER BY \"AMOUNT\"), DENSE_RANK() OVER (ORDER BY \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAPartitionedWindow() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARunningTotal() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARowsFrame() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARangeFrame() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"AMOUNT\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSeveralWindows() => Same("SELECT \"ID\", ROW_NUMBER() OVER (ORDER BY \"ID\"), SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnStringFunctions() => Same("SELECT UPPER(\"LABEL\") || '-' || LOWER(\"REGION\") FROM \"SALES\" ORDER BY 1");

    }

}
