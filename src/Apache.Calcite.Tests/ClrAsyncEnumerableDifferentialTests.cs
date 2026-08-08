using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs the same query through the asynchronous convention and the synchronous one, and requires the
    /// same rows.
    /// </summary>
    /// <remarks>
    /// The comparison is against <see cref="ClrEnumerableConvention"/> rather than against Calcite, and that
    /// is not a weaker oracle: the synchronous convention is checked against Calcite query by query in
    /// <see cref="ClrEnumerableDifferentialTests"/>, so agreeing with it is agreeing with Calcite. It is also
    /// the only comparison available — the asynchronous convention reads a table Calcite has no SPI for, so
    /// its schema is its own and a three-way comparison would be comparing two different sets of rows.
    ///
    /// <para>Both sides read <see cref="AsyncTestRows"/>, one copy, for exactly that reason.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerableDifferentialTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static ClrAsyncEnumerableDifferentialTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.util.Smalls).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        static org.apache.calcite.schema.SchemaPlus Schema(bool async)
        {
            var rootSchema = Frameworks.createRootSchema(true);

            if (async)
            {
                rootSchema.add("SALES", new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
                rootSchema.add("SORTED", new AsyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
            }
            else
            {
                rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
                rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
            }

            // a table function, which this convention has no node for: Calcite plans it and the converter
            // carries its rows
            rootSchema.add("NUMBERS", org.apache.calcite.schema.impl.TableFunctionImpl.create((java.lang.Class)typeof(NumbersTableFunction), "eval"));

            return rootSchema;
        }

        /// <summary>
        /// Plans a statement in one convention and returns its rows, rendered.
        /// </summary>
        static async Task<List<string>> Run(string sql, bool async, bool planOnly = false, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool markJoin = false)
        {
            var rootSchema = Schema(async);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
            {
                // dropped on both sides together, or the comparison is between two different plans
                if (excludeMergeJoin && rule == (async ? ClrAsyncEnumerableRules.ClrAsyncEnumerableMergeJoinRule : ClrEnumerableRules.ClrEnumerableMergeJoinRule))
                    continue;

                rules.add(rule);
            }

            // the three rules each convention declares as fields and leaves out of its default list; a
            // caller turns one on, and each side registers its own
            if (sortedAggregate)
                rules.add(async ? ClrAsyncEnumerableRules.ClrAsyncEnumerableSortedAggregateRule : ClrEnumerableRules.ClrEnumerableSortedAggregateRule);
            if (batchNestedLoopJoin)
                rules.add(async ? ClrAsyncEnumerableRules.ClrAsyncEnumerableBatchNestedLoopJoinRule : ClrEnumerableRules.ClrEnumerableBatchNestedLoopJoinRule);
            if (limitSort)
                rules.add(async ? ClrAsyncEnumerableRules.ClrAsyncEnumerableLimitSortRule : ClrEnumerableRules.ClrEnumerableLimitSortRule);
            foreach (var rule in async ? ClrAsyncEnumerableRules.CalcRules() : ClrEnumerableRules.CalcRules())
                calcRules.add(rule);

            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    markJoin ? MarkJoinSubQueryProgram() : Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, excludeMergeJoin, excludeHashJoin, null, null),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var convention = async ? (Convention)ClrAsyncEnumerableConvention.Instance : ClrEnumerableConvention.Instance;
            var chosen = planner.transform(1, expanded.getTraitSet().replace(convention).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            if (async)
            {
                var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, null, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);
                await foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }
            else
            {
                var bindable = ClrEnumerableInterpretable.ToBindable(parameters, null, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
                foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }

            return rows;
        }

        /// <summary>
        /// The context a plan of either convention is bound with.
        /// </summary>
        /// <remarks>
        /// Its own rather than the synchronous harness's, which is nested and private there. It answers
        /// <c>get</c> from the map the plan stashed into, because a plan reads its compile-time values back
        /// through the context and answering null to one is how a query fails in a way no assertion here
        /// would explain.
        /// </remarks>
        sealed class TestDataContext(org.apache.calcite.schema.SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public org.apache.calcite.schema.SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// The sub-query pass that rewrites an EXISTS or an IN into a mark correlate rather than a join.
        /// </summary>
        /// <remarks>
        /// The same program the synchronous harness uses. Without it the mark join paths are unreachable
        /// from SQL: the ordinary pass turns an EXISTS into a semi join and the marked variants never
        /// appear.
        /// </remarks>
        static Program MarkJoinSubQueryProgram()
        {
            var rules = new java.util.ArrayList();
            rules.add(org.apache.calcite.rel.rules.CoreRules.FILTER_SUB_QUERY_TO_MARK_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_SUB_QUERY_TO_MARK_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE);

            var builder = org.apache.calcite.plan.hep.HepProgram.builder();
            builder.addRuleCollection(rules);

            return Programs.of(builder.build(), true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE);
        }

        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions.
        /// </summary>
        static async Task Same(string sql, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool markJoin = false)
        {
            var async = await Run(sql, true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, markJoin: markJoin);
            var sync = await Run(sql, false, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, markJoin: markJoin);

            async.Should().Equal(sync, "'{0}' should give what ClrEnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires the same rows, and that the asynchronous convention really planned the node aimed at.
        /// </summary>
        /// <remarks>
        /// The plan assertion is what stops a test from comparing something against itself. It matters more
        /// here than in the synchronous harness, not less: there is no converter to fall through to, so a
        /// rule that fails to fire does not quietly produce a plan of the other convention — it produces no
        /// plan at all — but a node reached by a route nobody intended still looks like a pass.
        /// </remarks>
        static async Task SameThrough(string node, string sql, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false)
        {
            (await Run(sql, true, planOnly: true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin))[0]
                .Should().Contain(node, "'{0}' should be planned through {1}", sql, node);

            await Same(sql, sortedAggregate, batchNestedLoopJoin, limitSort, excludeHashJoin, excludeMergeJoin);
        }

        [TestMethod]
        public Task ShouldAgreeOnAScan() => Same("SELECT * FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAOneColumnScan() => Same("SELECT ID FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAFilter() => SameThrough("ClrAsyncEnumerableCalc", "SELECT * FROM SALES WHERE AMOUNT > 10");

        [TestMethod]
        public Task ShouldAgreeOnAProjection() => Same("SELECT ID, REGION FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAnExpression() => Same("SELECT ID + 1, UPPER(REGION) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnANullableColumn() => Same("SELECT AMOUNT FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnASort() => SameThrough("ClrAsyncEnumerableSort", "SELECT * FROM SALES ORDER BY AMOUNT");

        [TestMethod]
        public Task ShouldAgreeOnASortWithLimit() => Same("SELECT * FROM SALES ORDER BY ID OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY");

        [TestMethod]
        public Task ShouldAgreeOnValues() => SameThrough("ClrAsyncEnumerableValues", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        [TestMethod]
        public Task ShouldAgreeOnAnAggregate() => SameThrough("ClrAsyncEnumerableAggregate", "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION");

        [TestMethod]
        public Task ShouldAgreeOnACountOverEverything() => Same("SELECT COUNT(*) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAGrandTotal() => Same("SELECT SUM(AMOUNT), MIN(ID), MAX(ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnDistinct() => Same("SELECT DISTINCT REGION FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAUnion() => Same("SELECT ID FROM SALES UNION SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAUnionAll() => Same("SELECT ID FROM SALES UNION ALL SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAnIntersect() => Same("SELECT ID FROM SALES INTERSECT SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAMinus() => Same("SELECT ID FROM SALES EXCEPT SELECT K FROM SORTED");

        [TestMethod]
        public Task ShouldAgreeOnAJoin() => Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID = t.K");

        [TestMethod]
        public Task ShouldAgreeOnALeftJoin() => Same("SELECT s.ID, t.V FROM SALES s LEFT JOIN SORTED t ON s.ID = t.K");

        [TestMethod]
        public Task ShouldAgreeOnANestedLoopJoin() => Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID > t.K");

        [TestMethod]
        public Task ShouldAgreeOnASemiJoin() => Same("SELECT ID FROM SALES WHERE ID IN (SELECT K FROM SORTED)");

        [TestMethod]
        public Task ShouldAgreeOnAWindow() => Same("SELECT ID, SUM(AMOUNT) OVER (PARTITION BY REGION ORDER BY ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnARowNumber() => Same("SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnACorrelatedSubQuery() =>
            Same("SELECT ID, (SELECT COUNT(*) FROM SORTED t WHERE t.K = s.ID) FROM SALES s");

        /// <remarks>
        /// A merge join is only ever chosen over a hash join where both inputs carry a collation, and only
        /// where the hash join is not available to be cheaper — VolcanoCost compares the row count and
        /// nothing else, so the planner keeps whichever it saw first.
        /// </remarks>
        [TestMethod]
        public Task ShouldAgreeOnAMergeJoin() =>
            SameThrough("ClrAsyncEnumerableMergeJoin", "SELECT a.K, b.V FROM SORTED a JOIN SORTED b ON a.K = b.K", excludeHashJoin: true);

        [TestMethod]
        public Task ShouldAgreeOnAMergeJoinWithTies() =>
            Same("SELECT a.K, a.V, b.V FROM SORTED a JOIN SORTED b ON a.K = b.K ORDER BY a.K, a.V, b.V", excludeHashJoin: true);

        [TestMethod]
        public Task ShouldAgreeOnALeftMergeJoin() =>
            Same("SELECT a.K, b.V FROM SORTED a LEFT JOIN SORTED b ON a.K = b.K", excludeHashJoin: true);

        [TestMethod]
        public Task ShouldAgreeOnAMergeUnion() =>
            Same("SELECT K FROM SORTED UNION SELECT K FROM SORTED ORDER BY 1");

        [TestMethod]
        public Task ShouldAgreeOnASortedAggregate() =>
            Same("SELECT K, COUNT(*) FROM SORTED GROUP BY K ORDER BY K", sortedAggregate: true);

        [TestMethod]
        public Task ShouldAgreeOnABatchNestedLoopJoin() =>
            Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID > t.K", batchNestedLoopJoin: true);

        [TestMethod]
        public Task ShouldAgreeOnALimitSort() =>
            Same("SELECT * FROM SALES ORDER BY AMOUNT FETCH NEXT 2 ROWS ONLY", limitSort: true);

        [TestMethod]
        public Task ShouldAgreeOnGroupingSets() =>
            Same("SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY GROUPING SETS ((REGION), ()) ORDER BY 1");

        [TestMethod]
        public Task ShouldAgreeOnACube() =>
            Same("SELECT REGION, LABEL, COUNT(*) FROM SALES GROUP BY CUBE(REGION, LABEL) ORDER BY 1, 2");

        [TestMethod]
        public Task ShouldAgreeOnAnAntiJoin() =>
            Same("SELECT ID FROM SALES WHERE ID NOT IN (SELECT K FROM SORTED WHERE K IS NOT NULL)");

        [TestMethod]
        public Task ShouldAgreeOnAnExists() =>
            Same("SELECT ID FROM SALES s WHERE EXISTS (SELECT 1 FROM SORTED t WHERE t.K = s.ID)");

        [TestMethod]
        public Task ShouldAgreeOnAMultiset() =>
            Same("SELECT REGION, COLLECT(AMOUNT) FROM SALES GROUP BY REGION ORDER BY 1");

        [TestMethod]
        public Task ShouldAgreeOnAnUncollect() =>
            Same("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(x)");

        [TestMethod]
        public Task ShouldAgreeOnACaseAndCoalesce() =>
            Same("SELECT ID, CASE WHEN AMOUNT IS NULL THEN -1 ELSE AMOUNT END, COALESCE(AMOUNT, 0) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAggregatesOverNulls() =>
            Same("SELECT REGION, COUNT(AMOUNT), COUNT(*), SUM(AMOUNT), AVG(AMOUNT), MIN(AMOUNT), MAX(AMOUNT) FROM SALES GROUP BY REGION ORDER BY 1");

        [TestMethod]
        public Task ShouldAgreeOnAWindowWithFraming() =>
            Same("SELECT ID, SUM(AMOUNT) OVER (ORDER BY ID ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnRankAndLag() =>
            Same("SELECT ID, RANK() OVER (PARTITION BY REGION ORDER BY AMOUNT), LAG(AMOUNT) OVER (ORDER BY ID) FROM SALES");

        [TestMethod]
        public Task ShouldAgreeOnAnEmptyResult() =>
            Same("SELECT * FROM SALES WHERE 1 = 0");

        [TestMethod]
        public Task ShouldAgreeOnASelfJoinProducingNoRows() =>
            Same("SELECT a.ID FROM SALES a JOIN SALES b ON a.ID = b.ID + 1000");


        // ASOF join. Its rule is in both conventions' default lists, so nothing has to be turned on; what it
        // needs is a match condition and a key, and SALES has both.

        [TestMethod]
        public Task ShouldAgreeOnAnAsofJoin() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        [TestMethod]
        public Task ShouldAgreeOnALeftAsofJoin() =>
            Same("SELECT a.ID, b.ID FROM SALES a LEFT ASOF JOIN (SELECT * FROM SALES WHERE ID > 3) b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        [TestMethod]
        public Task ShouldAgreeOnAnAsofJoinLookingForward() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID > a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        /// <remarks>
        /// The order an ASOF join produces is the order of the map it indexes its left input by, so a query
        /// without an ORDER BY compares that order too.
        /// </remarks>
        [TestMethod]
        public Task ShouldAgreeOnAnAsofJoinsOwnOrder() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION");

        [TestMethod]
        public Task ShouldAgreeOnALeftAsofJoinWithANullKey() =>
            Same("SELECT a.ID, b.ID FROM SALES a LEFT ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.AMOUNT = b.AMOUNT ORDER BY a.ID");

        // the mark join paths, which the ordinary sub-query pass never reaches.

        [TestMethod]
        public Task ShouldAgreeOnAMarkedExists() =>
            Same("SELECT ID FROM SALES WHERE EXISTS (SELECT 1 FROM SALES S2 WHERE S2.ID > 4) ORDER BY ID", markJoin: true);

        [TestMethod]
        public Task ShouldAgreeOnAMarkedIn() =>
            Same("SELECT ID FROM SALES WHERE AMOUNT IN (SELECT AMOUNT FROM SALES WHERE ID > 3) ORDER BY ID", markJoin: true);

        [TestMethod]
        public Task ShouldAgreeOnAMarkedCorrelatedExists() =>
            Same("SELECT ID FROM SALES S1 WHERE EXISTS (SELECT 1 FROM SALES S2 WHERE S2.REGION = S1.REGION AND S2.ID > 3) ORDER BY ID", markJoin: true);

        // what the converter into this convention unlocked. None of these has a node here, and before the
        // converter each one made the whole query unplannable; now Calcite plans that part and the rest of
        // the query is still asynchronous.

        [TestMethod]
        public Task ShouldAgreeOnARecursiveQuery() =>
            Same("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [TestMethod]
        public Task ShouldAgreeOnARecursiveQueryOfSeveralColumns() =>
            Same("WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        [TestMethod]
        public Task ShouldAgreeOnATableFunction() =>
            Same("SELECT * FROM TABLE(NUMBERS(3))");

        /// <summary>
        /// A table function joined to a table is refused, as it is in the synchronous convention.
        /// </summary>
        /// <remarks>
        /// The same defect of Calcite's that <c>ShouldRefuseATableFunctionInAJoin</c> records:
        /// <c>EnumerableSort</c> optimises the scan's ARRAY format to SCALAR and hands the <c>Object[]</c>
        /// rows on unchanged, so the sequence carries arrays where its row type says
        /// <c>java.lang.Integer</c>.
        ///
        /// <para>It surfaces later here, and worse. In the synchronous convention the sort is our node, so
        /// <c>RequireRowType</c> catches it while the plan is being implemented and names the node. Here the
        /// whole subtree is Calcite's under one converter, and the converter believes what
        /// <c>result.physType.getFormat()</c> tells it — which is the thing that is wrong. So the mismatch
        /// is not visible statically and arrives as a cast at the first row read.</para>
        ///
        /// <para>Nothing to fix on this side: the convention does what Calcite does, and the check that
        /// would catch it is a check on Calcite's own answer about its own rows.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldRefuseATableFunctionJoinedToATable()
        {
            var act = async () => await Run("SELECT s.ID FROM SALES s, TABLE(NUMBERS(6)) n WHERE s.ID = n.N ORDER BY 1", true);

            await act.Should().ThrowAsync<InvalidCastException>();
        }

        /// <summary>
        /// MATCH_RECOGNIZE over an asynchronous table cannot be planned at all.
        /// </summary>
        /// <remarks>
        /// The design boundary, arriving where it was predicted to. Neither convention can write a
        /// MATCH_RECOGNIZE — <c>PassedRowsInputGetter</c> and <c>PrevInputGetter</c> are package-private
        /// types Calcite casts to by name — so the node has to be Calcite's. But Calcite's node needs its
        /// input in <c>EnumerableConvention</c>, and its input here is an
        /// <see cref="Schema.IClrAsyncScannableTable"/>, which Calcite cannot read by any route. The
        /// converter goes the other way.
        ///
        /// <para>So there is no plan, and the planner says so. That is the right answer rather than a gap:
        /// the only thing that would make it plan is a converter out of this convention, which would block
        /// once per row for a query nobody asked to have answered that way.</para>
        ///
        /// <para>The same query over a synchronous table plans, because then the whole subtree is
        /// Calcite's — which is what <c>ShouldPlanASyncOnlyTableThroughTheConverter</c> exercises the
        /// general form of.</para>
        /// </remarks>
        [TestMethod]
        public async Task ShouldRefuseAMatchRecognizeOverAnAsyncTable()
        {
            var act = async () => await Run("SELECT * FROM SALES MATCH_RECOGNIZE (ORDER BY ID MEASURES CLASSIFIER() AS cl PATTERN (a b) DEFINE a AS a.AMOUNT > 0, b AS b.AMOUNT > 0)", true);

            await act.Should().ThrowAsync<org.apache.calcite.plan.RelOptPlanner.CannotPlanException>();
        }

    }

}
