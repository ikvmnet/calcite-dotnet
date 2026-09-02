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
                rootSchema.add("WIDE", new AsyncRowsTable(AsyncTestRows.Wide, AsyncTestRows.WideRowType, false));
                rootSchema.add("ANYS", new AsyncRowsTable(AsyncTestRows.Anys, AsyncTestRows.AnysRowType, false));
                rootSchema.add("CASTS", new AsyncRowsTable(AsyncTestRows.Casts, AsyncTestRows.CastsRowType, false));
            }
            else
            {
                rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
                rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
                rootSchema.add("WIDE", new SyncRowsTable(AsyncTestRows.Wide, AsyncTestRows.WideRowType, false));
                rootSchema.add("ANYS", new SyncRowsTable(AsyncTestRows.Anys, AsyncTestRows.AnysRowType, false));
                rootSchema.add("CASTS", new SyncRowsTable(AsyncTestRows.Casts, AsyncTestRows.CastsRowType, false));
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
                var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);
                await foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }
            else
            {
                var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
                foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }

            return rows;
        }

        /// <summary>
        /// Plans a hand-built rel in one convention and returns its rows, rendered.
        /// </summary>
        /// <param name="build"></param>
        /// <param name="async"></param>
        /// <param name="planOnly"></param>
        /// <param name="add"></param>
        /// <param name="remove"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>ClrEnumerableDifferentialTests.RunRel</c> for this convention: the same planning, and the rule
        /// registration of <see cref="Run"/> next door. It exists because SQL cannot reach every shape. The
        /// one that forced it is a recursive query whose step aggregates the working table — standard SQL
        /// will not put an aggregate in a recursive term, and that shape is the only thing that tells
        /// <c>repeatUnion</c>'s termination test apart from "stop after an empty round".
        ///
        /// <para>Each side builds against its own schema, as <see cref="Run"/> does, so the asynchronous side
        /// reads asynchronous tables and the synchronous side reads synchronous ones. The rel is therefore
        /// built twice rather than shared, which is also what makes the builder's own state safe.</para>
        ///
        /// <para>No parser, so no validator and no sub-query program: a rel built here is already the shape
        /// the planner is given. That is the point of the route and also its limit — nothing here exercises
        /// how SQL becomes a rel.</para>
        /// </remarks>
        static async Task<List<string>> RunRel(Func<RelBuilder, RelNode> build, bool async, bool planOnly = false, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var rootSchema = Schema(async);

            var rules = new java.util.ArrayList();
            foreach (var rule in async ? ClrAsyncEnumerableRules.Rules() : ClrEnumerableRules.Rules())
                rules.add(rule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in async ? ClrAsyncEnumerableRules.CalcRules() : ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var logical = build(RelBuilder.create(config));

            var planner = (org.apache.calcite.plan.volcano.VolcanoPlanner)logical.getCluster().getPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

            var convention = async ? (Convention)ClrAsyncEnumerableConvention.Instance : ClrEnumerableConvention.Instance;
            var empty = new java.util.ArrayList();

            var chosen = new DefaultRulesProgram(rules, false, false, false, add, remove)
                .run(planner, logical, logical.getTraitSet().replace(convention).simplify(), empty, empty);

            var physical = Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)
                .run(planner, chosen, chosen.getTraitSet(), empty, empty);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            if (async)
            {
                var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);
                await foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }
            else
            {
                var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);
                foreach (var row in bindable.Bind(context))
                    rows.Add(Render(row));
            }

            return rows;
        }

        /// <summary>
        /// Requires that a hand-built rel gives the same rows in both conventions.
        /// </summary>
        static async Task SameRel(Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var async = await RunRel(build, true, add: add, remove: remove);
            var sync = await RunRel(build, false, add: add, remove: remove);

            async.Should().Equal(sync, "the plan should give what ClrEnumerableConvention gives");
        }

        /// <summary>
        /// Requires the same rows, and that the asynchronous convention really planned the node aimed at.
        /// </summary>
        /// <remarks>
        /// The reason <see cref="SameThrough"/> gives, and it applies here with more force: a rel built by
        /// hand does not have a parser's opinion about which node it wants, so a rule that fires on some
        /// other shape than the one intended still produces rows that agree.
        /// </remarks>
        static async Task SameRelThrough(string node, Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            (await RunRel(build, true, planOnly: true, add: add, remove: remove))[0]
                .Should().Contain(node, "the plan should be planned through {0}", node);

            await SameRel(build, add, remove);
        }

        /// <summary>
        /// A boxed integer, which is what a literal of a hand-built rel takes.
        /// </summary>
        static java.lang.Integer I(int value) => java.lang.Integer.valueOf(value);

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
        /// here than in the synchronous harness, not less: this harness registers one convention's rules
        /// only, so there is nothing for a converter to carry and a rule that fails to fire does not quietly
        /// produce a plan of the other convention — it produces no plan at all — but a node reached by a
        /// route nobody intended still looks like a pass.
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

        // MIN, MAX, SUM and AVG over a column of type ANY, whose Java class is Object. Neither convention
        // gets these from Calcite — ClrAnyAggImplementors says why and ClrEnumerableDifferentialTests asserts
        // the answers — so what is checked here is the thing this harness is for: that the asynchronous
        // convention gives what the synchronous one gives, mixed numeric types, strings and an empty group
        // included.

        [TestMethod]
        public Task ShouldAgreeOnAggregatingAnAnyColumn() => SameThrough("ClrAsyncEnumerableAggregate", "SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS");

        [TestMethod]
        public Task ShouldAgreeOnAGroupedAggregateOverAnAnyColumn() => SameThrough("ClrAsyncEnumerableAggregate", "SELECT K, MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS GROUP BY K ORDER BY K");

        [TestMethod]
        public Task ShouldAgreeOnAggregatingAnAnyColumnOfStrings() => Same("SELECT MIN(S), MAX(S) FROM ANYS");

        [TestMethod]
        public Task ShouldAgreeOnAggregatingAnEmptyAnyColumn() => Same("SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS WHERE K = 'NORTH'");

        [TestMethod]
        public Task ShouldAgreeOnWindowingAnAggregateOverAnAnyColumn() => SameThrough("ClrAsyncEnumerableWindow", "SELECT ID, MIN(V) OVER (PARTITION BY K), MAX(V) OVER (PARTITION BY K), SUM(V) OVER (PARTITION BY K) FROM ANYS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnARunningTotalOverAnAnyColumn() => SameThrough("ClrAsyncEnumerableWindow", "SELECT ID, SUM(V) OVER (ORDER BY ID) FROM ANYS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnTakingAnyValueOfAnAnyColumn() => SameThrough("ClrAsyncEnumerableAggregate", "SELECT ANY_VALUE(V), ANY_VALUE(S) FROM ANYS");

        [TestMethod]
        public Task ShouldAgreeOnDeviatingOverAnAnyColumn() => Same("SELECT VAR_POP(V), VAR_SAMP(V) FROM ANYS");

        [TestMethod]
        public Task ShouldAgreeOnFilteringAnAggregateOverAnAnyColumn() => Same("SELECT MIN(V) FILTER (WHERE ID > 1), SUM(V) FILTER (WHERE K = 'EAST') FROM ANYS");

        // and the same column read every way that already worked

        [TestMethod]
        public Task ShouldAgreeOnScanningAnAnyColumn() => Same("SELECT K, V, S FROM ANYS");

        [TestMethod]
        public Task ShouldAgreeOnCountingAnAnyColumn() => Same("SELECT K, COUNT(V), COUNT(*) FROM ANYS GROUP BY K ORDER BY K");

        [TestMethod]
        public Task ShouldAgreeOnAggregatingACastAnyColumn() => Same("SELECT MIN(CAST(V AS INTEGER)), MAX(CAST(V AS INTEGER)), SUM(CAST(V AS INTEGER)), AVG(CAST(V AS INTEGER)) FROM ANYS");

        [TestMethod]
        public Task ShouldAgreeOnAGroupedAggregateOverACastAnyColumn() => Same("SELECT K, MIN(CAST(V AS INTEGER)), SUM(CAST(V AS INTEGER)) FROM ANYS GROUP BY K ORDER BY K");

        [TestMethod]
        public Task ShouldAgreeOnCastingAnAnyColumnToVarchar() => Same("SELECT ID, CAST(G AS VARCHAR) FROM CASTS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnCastingAnAnyColumnToANumber() => Same("SELECT ID, CAST(N AS INTEGER), CAST(N AS DECIMAL(10, 2)) FROM CASTS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnCastingAnAnyColumnOfMillisToATimestamp() => Same("SELECT ID, CAST(M AS TIMESTAMP) FROM CASTS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnCastingAnAnyColumnToUuidChangingNothing() => Same("SELECT ID, CAST(G AS UUID) FROM CASTS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnCastingAnAnyColumnThroughVarcharToUuid() => Same("SELECT ID, CAST(CAST(G AS VARCHAR) AS UUID) FROM CASTS ORDER BY ID");

        [TestMethod]
        public Task ShouldAgreeOnCastingAnAnyColumnThroughVarcharToATimestamp() => Same("SELECT ID, CAST(CAST(T AS VARCHAR) AS TIMESTAMP) FROM CASTS ORDER BY ID");

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

        // A right and a full join over twelve build-side keys with no ORDER BY. The rows that matched nothing
        // come out at the end, and twelve is the one size at which the collection they are walked from
        // decides their order: the lookup is a table of 16 and the HashSet copied from its key set is a table
        // of 32.

        [TestMethod]
        public Task ShouldAgreeOnARightJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrAsyncEnumerableHashJoin", "SELECT a.N, b.K FROM (SELECT * FROM WIDE WHERE N < 3) a RIGHT JOIN WIDE b ON a.K = b.K");

        [TestMethod]
        public Task ShouldAgreeOnAFullJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrAsyncEnumerableHashJoin", "SELECT a.N, b.K FROM (SELECT * FROM WIDE WHERE N < 3) a FULL JOIN WIDE b ON a.K = b.K");

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

        // Eight key fields, which is the arity that builds the key through FlatLists.copyOf over an array of
        // Comparable. See ClrEnumerableDifferentialTests.ShouldAgreeOnARollupOverEveryColumn.

        [TestMethod]
        public Task ShouldAgreeOnARollupOverEveryColumn() =>
            Same("SELECT ID, REGION, AMOUNT, LABEL, COUNT(*) FROM SALES GROUP BY ROLLUP(ID, REGION, AMOUNT, LABEL) ORDER BY 1, 2, 3, 4, 5");

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

        // ------------------------------------------------------------------ built by hand
        //
        // Shapes SQL cannot express, through RunRel. Each has a twin in ClrEnumerableRelTests, which checks
        // the synchronous convention against Calcite; these check this convention against that one.

        /// <summary>
        /// A recursive query whose step aggregates the working table rather than reading it row by row.
        /// </summary>
        /// <remarks>
        /// The shape this harness was built for, and the only one that tells <c>repeatUnion</c>'s termination
        /// test apart from "stop after a round that produced nothing": the sentinel is never restored across
        /// the seed/iteration boundary, so a seed that emitted a row leaves the first empty round
        /// non-terminating. An aggregate step makes the extra round visible, because <c>COUNT(*)</c> yields a
        /// row over no rows; a step that reads the table row by row cannot see it.
        ///
        /// <para>UNION rather than UNION ALL, and that is what makes it terminate — the spool is cleared by
        /// the round that wrote nothing, so the step oscillates, and deduplication is what ends it. Under
        /// UNION ALL this runs forever in every convention including Calcite's.</para>
        /// </remarks>
        [TestMethod]
        public Task ShouldAgreeOnARecursiveQueryWhoseStepAggregates() =>
            SameRel(builder => builder
                .values(["i"], I(1))
                .transientScan("EMPTY_FIRST")
                .aggregate(builder.groupKey(), builder.count(false, "C"))
                .filter(builder.equals(builder.field(0), builder.literal(java.lang.Long.valueOf(0))))
                .project(builder.literal(I(99)))
                .repeatUnion("EMPTY_FIRST", false)
                .build());

        /// <summary>
        /// A recursive query whose step reads the working table a row at a time.
        /// </summary>
        /// <remarks>
        /// The ordinary shape, which SQL can express and this suite already runs as
        /// <c>ShouldAgreeOnARecursiveQuery</c>. Here to show the two routes agree on it, so that a failure of
        /// the one above is read as being about the aggregate rather than about the harness.
        /// </remarks>
        [TestMethod]
        public Task ShouldAgreeOnARecursiveQueryBuiltByHand() =>
            SameRel(builder => builder
                .values(["i"], I(1))
                .transientScan("DELTA")
                .filter(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, builder.field(0), builder.literal(I(4))))
                .project(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS, builder.field(0), builder.literal(I(1))))
                .repeatUnion("DELTA", true)
                .build());

        /// <summary>
        /// A scan and a filter built by hand, planned through this convention's own nodes.
        /// </summary>
        /// <remarks>
        /// The harness proving itself: that a rel built rather than parsed reaches this convention at all,
        /// and reaches it through <c>ClrAsyncEnumerableCalc</c> rather than through a converter. Without this
        /// a failure anywhere above is ambiguous between the shape and the route.
        /// </remarks>
        [TestMethod]
        public Task ShouldPlanAHandBuiltScanThroughThisConvention() =>
            SameRelThrough("ClrAsyncEnumerableCalc", builder => builder
                .scan("SORTED")
                .filter(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, builder.field(0), builder.literal(I(1))))
                .build());

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
