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
        /// A table that says its rows arrive sorted by their first field.
        /// </summary>
        /// <remarks>
        /// A merge join, a merge union and a sorted aggregate are only ever chosen over their hash and
        /// buffering counterparts when the input already carries a collation, and a table is where one comes
        /// from: <c>getStatistic().getCollations()</c> is what both conventions' scans put in their trait set.
        /// <c>SALES</c> advertises none, so with it Calcite picks a hash join, a union and an aggregate, and
        /// the three sorted nodes are unreachable in either convention.
        /// </remarks>
        sealed class SortedTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "A"],
                [java.lang.Integer.valueOf(2), "B"],
                [java.lang.Integer.valueOf(2), "C"],
                [java.lang.Integer.valueOf(4), "D"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("K", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("V", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }

            /// <inheritdoc />
            public override Statistic getStatistic()
            {
                return Statistics.of(Rows.Length,
                    new java.util.ArrayList(),
                    com.google.common.collect.ImmutableList.of(RelCollations.of(0)));
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
        /// A table with a timestamp, which is what a window table function needs and <c>SALES</c> has not.
        /// </summary>
        /// <remarks>
        /// A table of its own rather than a column added to <c>SALES</c>, so that nothing already asserted
        /// changes. A TIMESTAMP arrives as the millisecond count the type factory says it is.
        /// </remarks>
        sealed class EventsTable : AbstractTable, ScannableTable
        {

            const long Hour = 3600000L;
            const long Base = 1704067200000L;

            static readonly object[][] Rows =
            [
                [java.lang.Long.valueOf(Base), java.lang.Integer.valueOf(1)],
                [java.lang.Long.valueOf(Base + (Hour / 6)), java.lang.Integer.valueOf(2)],
                [java.lang.Long.valueOf(Base + Hour), java.lang.Integer.valueOf(3)],
                [java.lang.Long.valueOf(Base + (Hour * 2) + (Hour / 2)), java.lang.Integer.valueOf(4)],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ROWTIME", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
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
        /// <param name="topDown">Whether the planner optimises top down, which is what asks a node to pass a
        /// trait down to its inputs or derive one from them.</param>
        /// <returns></returns>
        static List<string> Run(string sql, bool clr, bool topDown = false, bool planOnly = false)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", new SalesTable());
            rootSchema.add("MY_SUM", AggregateFunctionImpl.create((java.lang.Class)typeof(SumAggregate)));
            rootSchema.add("NUMBERS", TableFunctionImpl.create((java.lang.Class)typeof(NumbersTableFunction), "eval"));
            rootSchema.add("EVENTS", new EventsTable());
            rootSchema.add("SORTED", new SortedTable());

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            if (clr)
            {
                foreach (var rule in ClrEnumerableRules.Rules())
                    rules.add(rule);
                foreach (var rule in ClrEnumerableRules.CalcRules())
                    calcRules.add(rule);
            }

            // Calcite's own rules are registered by DefaultRulesProgram, because RelOptUtil.registerDefaultRules
            // registers ENUMERABLE_RULES itself. Where this convention has no node for something the planner
            // takes Calcite's, and the converters carry the rows across.

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
                    new DefaultRulesProgram(rules, topDown, clr && topDown),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = clr ? (Convention)ClrEnumerableConvention.Instance : EnumerableConvention.INSTANCE;
            // Prepare.getDesiredRootTraitSet: the root's own traits with the convention replaced, then
            // simplified. simplify() is what collapses the composite collation a VALUES of several rows
            // carries; without it the planner casts that trait to a single RelCollation and fails. The
            // collation has to be kept rather than dropped for an empty trait set, or SortRemoveRule — which
            // arrives with Calcite's abstract rules — takes an ORDER BY away as unwanted.
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            if (planOnly)
                return [org.apache.calcite.plan.RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var bindable = physical is ClrEnumerableRel node
                ? ClrEnumerableInterpretable.ToBindable(parameters, node, ClrEnumerablePrefer.Array)
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
        /// Returns the chosen plan, for a question about which nodes a query reaches.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="clr"></param>
        /// <returns></returns>
        internal static string PlanOf(string sql, bool clr)
        {
            return Run(sql, clr, false, true)[0];
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

        /// <summary>
        /// Requires that a query gives the same rows in both conventions when the planner optimises top down.
        /// </summary>
        /// <param name="sql"></param>
        /// <remarks>
        /// Top-down optimisation is the only thing that calls <c>passThroughTraits</c>, <c>deriveTraits</c>
        /// and <c>getDeriveMode</c>. Calcite leaves it off by default, so with it off those three are dead on
        /// both sides and a comparison says nothing about them; with it on, this convention's answers to them
        /// are being compared against <c>EnumerableConvention</c>'s.
        /// </remarks>
        static void SameTopDown(string sql)
        {
            var mine = Run(sql, true, true);
            var calcite = Run(sql, false, true);

            mine.Should().Equal(calcite, "'{0}' should give what EnumerableConvention gives, planned top down", sql);
        }

        /// <summary>
        /// Requires that a query gives the stated rows in this convention.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="expected"></param>
        /// <remarks>
        /// For the queries <c>EnumerableConvention</c> cannot run at all, so there is nothing to ask. Prefer
        /// <see cref="Same"/> everywhere else: an expectation written by hand is one this convention can agree
        /// with while both of them are wrong.
        /// </remarks>
        static void Gives(string sql, params string[] expected)
        {
            Run(sql, true).Should().Equal(expected, "'{0}' should give the rows SQL says it does", sql);
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

        /// <summary>
        /// A GROUP BY with no ORDER BY, which is where the two conventions have to agree on an order neither
        /// was asked for: ours groups in a <see cref="System.Collections.Generic.Dictionary{TKey, TValue}"/>
        /// and Calcite's in a <c>java.util.HashMap</c>.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAGroupBysOwnOrder() => Same("SELECT \"REGION\", COUNT(*) FROM \"SALES\" GROUP BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAGlobalAggregate() => Same("SELECT COUNT(*), SUM(\"AMOUNT\"), AVG(\"AMOUNT\") FROM \"SALES\"");

        // An aggregate call carrying its own ordering, which holds the rows of a group and folds them once
        // the call's ordering has been applied — LazyAggregateLambdaFactory over a SourceSorter per ordered
        // call and a BasicLazyAccumulator per unordered one. The four cover: one ordered call, a global
        // aggregate with no GROUP BY, an ordered and an unordered call in one aggregate, and an ordering on a
        // nullable column.

        [TestMethod]
        public void ShouldAgreeOnAnOrderedAggregateCall() =>
            Same("SELECT \"REGION\", LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\" DESC) FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAGlobalOrderedAggregateCall() =>
            Same("SELECT LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\" DESC) FROM \"SALES\"");

        [TestMethod]
        public void ShouldAgreeOnAnOrderedAndAnUnorderedCallTogether() =>
            Same("SELECT \"REGION\", COUNT(*), LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"ID\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAnAggregateCallOrderedByANullableColumn() =>
            Same("SELECT \"REGION\", LISTAGG(\"LABEL\", ',') WITHIN GROUP (ORDER BY \"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAnInnerJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftJoin() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN (SELECT * FROM \"SALES\" WHERE \"AMOUNT\" > 25) b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAJoinWithAnInequality() => Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnAsofJoin() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftAsofJoin() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT ASOF JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 3) b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnAsofJoinLookingForward() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" > a.\"ID\" ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\"");

        /// <summary>
        /// The order of an ASOF join's rows is the order of the map it indexes the left input by, so a query
        /// with no ORDER BY is the one that says whether ours agrees with linq4j's.
        /// </summary>
        [TestMethod]
        public void ShouldAgreeOnAnAsofJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAnAsofJoinOnASeveralFieldKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"REGION\" = b.\"REGION\" AND a.\"LABEL\" = b.\"LABEL\" ORDER BY a.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftAsofJoinWithANullKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT ASOF JOIN \"SALES\" b MATCH_CONDITION b.\"ID\" <= a.\"ID\" ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\"");

        // A right and a full join with no ORDER BY: the rows of the right input that matched nothing come out
        // at the end, in the order of the lookup the join built.

        [TestMethod]
        public void ShouldAgreeOnARightJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a RIGHT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" AND a.\"LABEL\" = b.\"LABEL\"");

        [TestMethod]
        public void ShouldAgreeOnAFullJoinsOwnOrder() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM (SELECT * FROM \"SALES\" WHERE \"ID\" < 3) a FULL JOIN (SELECT * FROM \"SALES\" WHERE \"ID\" > 1) b ON a.\"LABEL\" = b.\"LABEL\"");

        // A set operation with no ORDER BY, which is the same question as the GROUP BY above: the rows come
        // out in the order of the collection the operator held them in, and Calcite holds them in a
        // java.util.HashSet or a HashMultiset.

        [TestMethod]
        public void ShouldAgreeOnUnionsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" UNION SELECT \"LABEL\" FROM \"SALES\"");

        [TestMethod]
        public void ShouldAgreeOnIntersectsOwnOrder() => Same("SELECT \"LABEL\" FROM \"SALES\" INTERSECT SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" < 5");

        [TestMethod]
        public void ShouldAgreeOnIntersectAllsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" INTERSECT ALL SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" < 5");

        [TestMethod]
        public void ShouldAgreeOnExceptsOwnOrder() => Same("SELECT \"LABEL\" FROM \"SALES\" EXCEPT SELECT \"LABEL\" FROM \"SALES\" WHERE \"ID\" > 4");

        [TestMethod]
        public void ShouldAgreeOnExceptAllsOwnOrder() => Same("SELECT \"REGION\" FROM \"SALES\" EXCEPT ALL SELECT \"REGION\" FROM \"SALES\" WHERE \"ID\" > 4");

        [TestMethod]
        public void ShouldAgreeOnDistinctsOwnOrder() => Same("SELECT DISTINCT \"REGION\" FROM \"SALES\"");

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
        public void ShouldAgreeOnAWindowOverEverything() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER () FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnSeveralPartitionKeys() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"REGION\", \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnANullPartitionKey() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"AMOUNT\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAnEmptyFrame() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnARangeFrameWithAnOffset() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnLeadAndLag() => Same("SELECT \"ID\", LAG(\"AMOUNT\") OVER (ORDER BY \"ID\"), LEAD(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnFirstAndLastValue() => Same("SELECT \"ID\", FIRST_VALUE(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\"), LAST_VALUE(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // a running frame rather than the whole partition, because it is the one that tells the three
        // exclusions apart: over an unbounded frame Calcite reports no row as any other's peer
        [TestMethod]
        public void ShouldAgreeOnExcludingTheCurrentRow() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnExcludingTies() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnExcludingTheGroup() => Same("SELECT \"ID\", COUNT(\"AMOUNT\") OVER (ORDER BY \"AMOUNT\" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnNtile() => Same("SELECT \"ID\", NTILE(2) OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnNthValue() => Same("SELECT \"ID\", NTH_VALUE(\"AMOUNT\", 2) OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // one aggregate whose value survives an intact frame and one that does not, in the same window, so
        // both result lambdas run over the same accumulator
        [TestMethod]
        public void ShouldAgreeOnACachedAndAnUncachedAggregateTogether() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\"), LAG(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // a RANGE frame ending at the current row with more than one ordering key, which is the only shape
        // that reaches the five-argument binary search
        [TestMethod]
        public void ShouldAgreeOnARangeFrameOverSeveralOrderKeys() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"REGION\", \"AMOUNT\" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM \"SALES\" ORDER BY \"ID\"");

        // no ORDER BY, so the rows arrive in the order the partitions do, which is a hash map's. Nothing
        // reproduces that but the map itself: under IKVM a String hashes as .NET hashes it, which is
        // randomised per process, so the order is not even the same run to run — both conventions read the
        // same map in the same process, which is the only reason this can be asserted at all.
        [TestMethod]
        public void ShouldAgreeOnThePartitionOrder() => Same("SELECT \"REGION\", \"ID\", COUNT(*) OVER (PARTITION BY \"REGION\") FROM \"SALES\"");

        // a key that is a primitive, so it has to be boxed the way the type factory says before a map holds it
        [TestMethod]
        public void ShouldAgreeOnAPrimitivePartitionKey() => Same("SELECT \"ID\", COUNT(*) OVER (PARTITION BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // two calls on one implementor instance, which keeps state of its own between getStateType and
        // implementAdd: COUNT(*) takes the frame's row count and COUNT of a nullable column accumulates
        [TestMethod]
        public void ShouldAgreeOnTwoCountsInOneWindow() => Same("SELECT \"ID\", COUNT(*) OVER (ORDER BY \"ID\"), COUNT(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAFrameEntirelyFollowing() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\" ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnMinAndMax() => Same("SELECT \"ID\", MIN(\"AMOUNT\") OVER (PARTITION BY \"REGION\"), MAX(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        // AVG has no implementor, so this only reaches a window at all once it is reduced to SUM over COUNT
        [TestMethod]
        public void ShouldAgreeOnAnAverageOverAWindow() => Same("SELECT \"ID\", AVG(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowOverNoRows() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\") FROM \"SALES\" WHERE \"ID\" < 0 ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowOverAFilteredInput() => Same("SELECT \"ID\", SUM(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" WHERE \"REGION\" = 'EAST' ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnRowNumberWithoutAnOrder() => Same("SELECT \"ID\", ROW_NUMBER() OVER (PARTITION BY \"REGION\") FROM \"SALES\" ORDER BY \"ID\"");

        // an offset and a default, so the window carries more than one constant past its input's own fields
        [TestMethod]
        public void ShouldAgreeOnLagWithAnOffsetAndDefault() => Same("SELECT \"ID\", LAG(\"AMOUNT\", 2, -1) OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // The two below are the only ones asserted by hand, because Calcite cannot answer them. A user-defined
        // function written in C# is a class IKVM names cli.Apache.Calcite.Linq.Tests.SumAggregate;
        // EnumerableConvention writes that name into generated Java source, and Janino does not resolve a
        // cli. name, so its plan fails to compile. This convention holds the method itself rather than its
        // name, so it runs — which makes these the one place a hand-written expectation is the only check
        // available, and the values are SQL's rather than anything either convention produced.

        // running sum over ORDER BY ID, which the default RANGE frame makes a prefix, with the null skipped
        [TestMethod]
        public void ShouldRunAUserDefinedWindowAggregate() =>
            Gives("SELECT \"ID\", MY_SUM(\"AMOUNT\") OVER (ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"",
                "1|10", "2|30", "3|50", "4|80", "5|80", "6|85");

        // EAST is 10 + 20 + 20 and WEST is 30 + 5, the null contributing nothing
        [TestMethod]
        public void ShouldRunAUserDefinedAggregate() =>
            Gives("SELECT \"REGION\", MY_SUM(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"",
                "EAST|50", "WEST|35");

        // SORTED advertises a collation, so Calcite plans these with EnumerableMergeJoin where it plans the
        // same query over SALES with a hash join. This convention has no merge join yet, so what these
        // compare is our hash join against Calcite's merge join — two algorithms, one answer. They become
        // the merge join's own tests the moment the node exists.
        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputs() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnALeftJoinOverSortedInputs() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" LEFT JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S2\".\"V\" <> 'B' ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAGroupByOverASortedInput() =>
            Same("SELECT \"K\", COUNT(*) FROM \"SORTED\" GROUP BY \"K\" ORDER BY 1");

        // A merge union: an ORDER BY directly over a UNION, which is the shape its rule requires. Naming the
        // columns instead of SELECT * puts a projection between the two and the rule never fires — that is
        // what made this node look unreachable for a while.

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionAll() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionDistinct() =>
            Same("SELECT * FROM \"SORTED\" UNION SELECT * FROM \"SORTED\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionWithALimit() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1 FETCH FIRST 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionWithAnOffsetAndALimit() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY");

        [TestMethod]
        public void ShouldAgreeOnAMergeUnionOfThreeInputs() =>
            Same("SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" UNION ALL SELECT * FROM \"SORTED\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAUnionOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" UNION SELECT \"K\", \"V\" FROM \"SORTED\" ORDER BY 1, 2");

        // A merge join is what these plan to, on both sides, over an input that advertises a collation. The
        // four above reach it as well — they were written before the node existed, comparing our hash join
        // against Calcite's merge join, and they now compare the two merge joins. These add the join types
        // and the shapes the algorithm has separate paths for: a run of equal keys on both sides, a key
        // missing from one side, several keys, an extra condition that is not an equality, and a null key,
        // which is where the comparator refuses to call two nulls equal.

        [TestMethod]
        public void ShouldAgreeOnASemiJoinOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" \"S1\" WHERE EXISTS (SELECT 1 FROM \"SORTED\" \"S2\" WHERE \"S2\".\"K\" = \"S1\".\"K\") ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAnAntiJoinOverSortedInputs() =>
            Same("SELECT \"K\", \"V\" FROM \"SORTED\" \"S1\" WHERE NOT EXISTS (SELECT 1 FROM \"SORTED\" \"S2\" WHERE \"S2\".\"K\" = \"S1\".\"K\" AND \"S2\".\"V\" = 'A') ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsMissingKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN (SELECT * FROM \"SORTED\" WHERE \"K\" <> 2) \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnALeftJoinOverSortedInputsMissingKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" LEFT JOIN (SELECT * FROM \"SORTED\" WHERE \"K\" > 2) \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsOnSeveralKeys() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S1\".\"V\" = \"S2\".\"V\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsWithAnExtraCondition() =>
            Same("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" AND \"S1\".\"V\" < \"S2\".\"V\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnAJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnALeftJoinOnANullableKey() =>
            Same("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"AMOUNT\" = b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnStringFunctions() => Same("SELECT UPPER(\"LABEL\") || '-' || LOWER(\"REGION\") FROM \"SALES\" ORDER BY 1");

        // Planned top down, which is the only thing that calls passThroughTraits, deriveTraits and
        // getDeriveMode. Every node whose trait derivation is more than the default is here: a project and a
        // calc (permutation and cast), a filter, a hash join, a nested loop join, a correlate, a scan and a
        // VALUES, each with a collation to push down or derive.

        [TestMethod]
        public void ShouldAgreeOnAProjectionSortedTopDown() =>
            SameTopDown("SELECT \"REGION\", \"ID\" FROM \"SALES\" ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnACastInASortedProjectionTopDown() =>
            SameTopDown("SELECT CAST(\"ID\" AS BIGINT), \"REGION\" FROM \"SALES\" ORDER BY 1");

        [TestMethod]
        public void ShouldAgreeOnAFilterUnderASortTopDown() =>
            SameTopDown("SELECT \"ID\", \"AMOUNT\" FROM \"SALES\" WHERE \"AMOUNT\" > 5 ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnASortedJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnASortedLeftJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a LEFT JOIN \"SALES\" b ON a.\"REGION\" = b.\"REGION\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnASortedNestedLoopJoinTopDown() =>
            SameTopDown("SELECT a.\"ID\", b.\"ID\" FROM \"SALES\" a JOIN \"SALES\" b ON a.\"AMOUNT\" < b.\"AMOUNT\" ORDER BY a.\"ID\", b.\"ID\"");

        [TestMethod]
        public void ShouldAgreeOnAJoinOverSortedInputsTopDown() =>
            SameTopDown("SELECT \"S1\".\"K\", \"S2\".\"V\" FROM \"SORTED\" \"S1\" JOIN \"SORTED\" \"S2\" ON \"S1\".\"K\" = \"S2\".\"K\" ORDER BY 1, 2");

        [TestMethod]
        public void ShouldAgreeOnACorrelatedSubQueryTopDown() =>
            SameTopDown("SELECT \"ID\" FROM \"SALES\" a WHERE \"AMOUNT\" > (SELECT MIN(\"AMOUNT\") FROM \"SALES\" b WHERE b.\"REGION\" = a.\"REGION\") ORDER BY \"ID\"");

        [TestMethod]
        public void ShouldAgreeOnValuesTopDown() =>
            SameTopDown("SELECT * FROM (VALUES (1, 'A'), (2, 'B'), (3, 'C')) AS t(\"N\", \"L\") ORDER BY \"N\"");

        [TestMethod]
        public void ShouldAgreeOnAnAggregateTopDown() =>
            SameTopDown("SELECT \"REGION\", SUM(\"AMOUNT\") FROM \"SALES\" GROUP BY \"REGION\" ORDER BY \"REGION\"");

        [TestMethod]
        public void ShouldAgreeOnAWindowTopDown() =>
            SameTopDown("SELECT \"ID\", SUM(\"AMOUNT\") OVER (PARTITION BY \"REGION\" ORDER BY \"ID\") FROM \"SALES\" ORDER BY \"ID\"");

        // MATCH_RECOGNIZE. The pattern is a plain sequence of symbols in every one of these, because that is
        // all EnumerableMatch generates: implementPattern handles a literal and a concatenation and throws on
        // anything else, so *, + and | never reach a plan in either convention.

        // A table function is a class too, so the same thing holds as for MY_SUM: Janino cannot name a CLR
        // class, so EnumerableConvention has no plan for these and there is nothing to compare against. The
        // function yields one to n, which is what is asserted.
        [TestMethod]
        public void ShouldRunATableFunction() => Gives("SELECT * FROM TABLE(NUMBERS(3))", "1", "2", "3");

        [TestMethod]
        public void ShouldRunATableFunctionInAJoin() =>
            Gives("SELECT \"S\".\"ID\" FROM \"SALES\" AS \"S\", TABLE(NUMBERS(2)) AS \"N\" WHERE \"S\".\"ID\" = \"N\".\"N\" ORDER BY 1", "1", "2");

        [TestMethod]
        public void ShouldRunATableFunctionUnderAnAggregate() =>
            Gives("SELECT COUNT(*), SUM(\"N\") FROM TABLE(NUMBERS(4))", "4|10");

        // TUMBLE still has no test. The rule refuses it, and the converter now carries far more than it did
        // but not this: translating EnumUtils.tumblingWindowSelector leaves an Object[] "_input" referenced
        // from no scope that declares it, by whichever route the tree is built. See TODO.md. EVENTS stays
        // because it is what a test needs the moment that is understood.

        // MATCH_RECOGNIZE has no test because it does not run in either convention over this fixture. See
        // TODO.md: EnumerableMatch builds the measures row with Expressions.new_ on the row's Java type, and
        // where the input format is ARRAY that type is Object[], so it emits "new Object[]()" — which is not
        // Java, and not something a translator can complete either, because the length is only implied by the
        // assignments that follow.

    }

}
