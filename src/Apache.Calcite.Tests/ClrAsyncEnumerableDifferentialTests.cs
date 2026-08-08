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

            return rootSchema;
        }

        /// <summary>
        /// Plans a statement in one convention and returns its rows, rendered.
        /// </summary>
        static async Task<List<string>> Run(string sql, bool async, bool planOnly = false, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false)
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
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
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

        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Requires that a query gives the same rows in both conventions.
        /// </summary>
        static async Task Same(string sql, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false)
        {
            var async = await Run(sql, true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin);
            var sync = await Run(sql, false, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin);

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

    }

}
