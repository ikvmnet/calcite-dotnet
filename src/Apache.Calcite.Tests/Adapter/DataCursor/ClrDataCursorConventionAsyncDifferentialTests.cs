using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.DataCursor.Tests
{

    /// <summary>
    /// Runs the same query with its plan implemented asynchronously and synchronously, and requires the same
    /// rows.
    /// </summary>
    /// <remarks>
    /// The comparison is against the same plan read synchronously rather than against Calcite, and that is
    /// not a weaker oracle: a plan of this convention is checked against Calcite query by query in
    /// <see cref="ClrDataCursorConventionDifferentialTests"/>, so agreeing with it is agreeing with Calcite. It is also
    /// the only comparison available — the asynchronous side reads a table Calcite has no SPI for, so its
    /// schema is its own and a three-way comparison would be comparing two different sets of rows.
    ///
    /// <para>Both sides read <see cref="AsyncTestRows"/>, one copy, for exactly that reason.</para>
    ///
    /// <para><b>What this suite compares changed with the conventions.</b> It used to run two plans, one per
    /// convention, over two schemas. It now runs one plan per schema and implements it twice, so a
    /// disagreement is a disagreement between the two operator sets rather than between two node
    /// hierarchies — which is what is left to get wrong.</para>
    /// </remarks>
    public class ClrDataCursorConventionAsyncDifferentialTests
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static ClrDataCursorConventionAsyncDifferentialTests()
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
                rootSchema.add("EVENTS", new AsyncRowsTable(AsyncTestRows.Events, AsyncTestRows.EventsRowType, false));
                rootSchema.add("DOCS", new AsyncRowsTable(AsyncTestRows.Docs, AsyncTestRows.DocsRowType, false));
            }
            else
            {
                rootSchema.add("SALES", new SyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
                rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));
                rootSchema.add("WIDE", new SyncRowsTable(AsyncTestRows.Wide, AsyncTestRows.WideRowType, false));
                rootSchema.add("ANYS", new SyncRowsTable(AsyncTestRows.Anys, AsyncTestRows.AnysRowType, false));
                rootSchema.add("CASTS", new SyncRowsTable(AsyncTestRows.Casts, AsyncTestRows.CastsRowType, false));
                rootSchema.add("EVENTS", new SyncRowsTable(AsyncTestRows.Events, AsyncTestRows.EventsRowType, false));
                rootSchema.add("DOCS", new SyncRowsTable(AsyncTestRows.Docs, AsyncTestRows.DocsRowType, false));
            }

            // a table function the schema defines, whose call yields the sequence: ClrDataCursorTableFunctionScan
            // takes it, and there is no input for either body to read
            rootSchema.add("NUMBERS", org.apache.calcite.schema.impl.TableFunctionImpl.create((java.lang.Class)typeof(NumbersTableFunction), "eval"));

            return rootSchema;
        }

        /// <summary>
        /// Plans a statement in one convention and returns its rows, rendered.
        /// </summary>
        static async Task<List<string>> Run(string sql, bool async, bool planOnly = false, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool markJoin = false, RelOptRule[]? remove = null)
        {
            var rootSchema = Schema(async);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in ClrEnumerableRules.Rules())
            {
                // dropped on both sides together, or the comparison is between two different plans
                if (excludeMergeJoin && rule == ClrEnumerableRules.ClrEnumerableMergeJoinRule)
                    continue;

                rules.add(rule);
            }

            // and the cursor convention's, which is the root
            foreach (var rule in ClrDataCursorRules.Rules())
                rules.add(rule);

            // the three rules the convention declares as fields and leaves out of its default list; a caller
            // turns one on
            if (sortedAggregate)
            {
                rules.add(ClrEnumerableRules.ClrEnumerableSortedAggregateRule);
                rules.add(ClrDataCursorRules.ClrDataCursorSortedAggregateRule);
            }
            if (batchNestedLoopJoin)
                rules.add(ClrEnumerableRules.ClrEnumerableBatchNestedLoopJoinRule);
            if (limitSort)
            {
                rules.add(ClrEnumerableRules.ClrEnumerableLimitSortRule);
                rules.add(ClrDataCursorRules.ClrDataCursorLimitSortRule);
            }
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in ClrDataCursorRules.CalcRules())
                if (calcRules.contains(rule) == false)
                    calcRules.add(rule);

            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    markJoin ? MarkJoinSubQueryProgram() : Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, excludeMergeJoin, excludeHashJoin, null, remove),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrDataCursorConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            // one factory, opened the way the side asks: with await over the asynchronous tables and read
            // with the awaiting advance, synchronously over the synchronous ones and read with the other
            var factory = new ClrDataCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters)
                .ImplementRoot((ClrDataCursorRel)physical, ClrEnumerablePrefer.Array);

            if (async)
            {
                await using var cursor = await factory.OpenAsync(context, System.Threading.CancellationToken.None);
                while (await cursor.ReadAsync(System.Threading.CancellationToken.None))
                    rows.Add(Render(cursor.Current));
            }
            else
            {
                using var cursor = factory.Open(context);
                while (cursor.Read())
                    rows.Add(Render(cursor.Current));
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
        /// <c>ClrDataCursorConventionDifferentialTests.RunRel</c> for this convention: the same planning, and the rule
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
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrDataCursorRules.Rules())
                rules.add(rule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in ClrDataCursorRules.CalcRules())
                if (calcRules.contains(rule) == false)
                    calcRules.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var logical = build(RelBuilder.create(config));

            var planner = (org.apache.calcite.plan.volcano.VolcanoPlanner)logical.getCluster().getPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

            var empty = new java.util.ArrayList();

            var chosen = new DefaultRulesProgram(rules, false, false, false, add, remove)
                .run(planner, logical, logical.getTraitSet().replace(ClrDataCursorConvention.Instance).simplify(), empty, empty);

            var physical = Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE)
                .run(planner, chosen, chosen.getTraitSet(), empty, empty);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            // one factory, opened the way the side asks: with await over the asynchronous tables and read
            // with the awaiting advance, synchronously over the synchronous ones and read with the other
            var factory = new ClrDataCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters)
                .ImplementRoot((ClrDataCursorRel)physical, ClrEnumerablePrefer.Array);

            if (async)
            {
                await using var cursor = await factory.OpenAsync(context, System.Threading.CancellationToken.None);
                while (await cursor.ReadAsync(System.Threading.CancellationToken.None))
                    rows.Add(Render(cursor.Current));
            }
            else
            {
                using var cursor = factory.Open(context);
                while (cursor.Read())
                    rows.Add(Render(cursor.Current));
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
        static async Task Same(string sql, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool markJoin = false, RelOptRule[]? remove = null)
        {
            var async = await Run(sql, true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, markJoin: markJoin, remove: remove);
            var sync = await Run(sql, false, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, markJoin: markJoin, remove: remove);

            async.Should().Equal(sync, "'{0}' should give what ClrEnumerableConvention gives", sql);
        }

        /// <summary>
        /// Requires that a statement fails the same way whichever half of the convention reads it.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="message">Part of the message the pulled half fails with.</param>
        /// <remarks>
        /// <see cref="Same"/> for a statement that throws. The awaiting half has to fail where the pulled
        /// half fails and for the same reason — a refusal is an answer, and the two bodies of a node are
        /// required to agree on it as much as on rows. Whether that refusal is Calcite's is the sync
        /// suite's question, which compares against <c>EnumerableConvention</c>.
        /// </remarks>
        static async Task SameFailure(string sql, string message)
        {
            static async Task<string> Failure(string sql, bool async)
            {
                try
                {
                    await Run(sql, async);
                    return "<no failure>";
                }
                catch (Exception e)
                {
                    while (e.InnerException is not null)
                        e = e.InnerException;

                    return $"{e.GetType().Name}: {e.Message}";
                }
            }

            var awaited = await Failure(sql, true);
            var pulled = await Failure(sql, false);

            pulled.Should().Contain(message, "'{0}' should fail this way when pulled", sql);
            awaited.Should().Be(pulled, "'{0}' should fail the way the pulled half fails", sql);
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
        static async Task SameThrough(string node, string sql, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, RelOptRule[]? remove = null)
        {
            (await Run(sql, true, planOnly: true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, remove: remove))[0]
                .Should().Contain(node, "'{0}' should be planned through {1}", sql, node);

            await Same(sql, sortedAggregate, batchNestedLoopJoin, limitSort, excludeHashJoin, excludeMergeJoin, remove: remove);
        }

        [Fact]
        public Task ShouldAgreeOnAScan() => Same("SELECT * FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAOneColumnScan() => Same("SELECT ID FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAFilter() => SameThrough("ClrDataCursorCalc", "SELECT * FROM SALES WHERE AMOUNT > 10");

        [Fact]
        public Task ShouldAgreeOnAProjection() => Same("SELECT ID, REGION FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAnExpression() => Same("SELECT ID + 1, UPPER(REGION) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnANullableColumn() => Same("SELECT AMOUNT FROM SALES");

        [Fact]
        public Task ShouldAgreeOnASort() => SameThrough("ClrDataCursorSort", "SELECT * FROM SALES ORDER BY AMOUNT");

        [Fact]
        public Task ShouldAgreeOnASortWithLimit() => Same("SELECT * FROM SALES ORDER BY ID OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public Task ShouldAgreeOnValues() => SameThrough("ClrDataCursorValues", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        [Fact]
        public Task ShouldAgreeOnAnAggregate() => SameThrough("ClrDataCursorAggregate", "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION");

        [Fact]
        public Task ShouldAgreeOnACountOverEverything() => Same("SELECT COUNT(*) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAGrandTotal() => Same("SELECT SUM(AMOUNT), MIN(ID), MAX(ID) FROM SALES");

        // MIN, MAX, SUM and AVG over a column of type ANY, whose Java class is Object. Neither convention
        // gets these from Calcite — ClrAnyAggImplementors says why and ClrDataCursorConventionDifferentialTests asserts
        // the answers — so what is checked here is the thing this harness is for: that the asynchronous
        // convention gives what the synchronous one gives, mixed numeric types, strings and an empty group
        // included.

        [Fact]
        public Task ShouldAgreeOnAggregatingAnAnyColumn() => SameThrough("ClrDataCursorAggregate", "SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnAGroupedAggregateOverAnAnyColumn() => SameThrough("ClrDataCursorAggregate", "SELECT K, MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS GROUP BY K ORDER BY K");

        [Fact]
        public Task ShouldAgreeOnAggregatingAnAnyColumnOfStrings() => Same("SELECT MIN(S), MAX(S) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnAggregatingAnEmptyAnyColumn() => Same("SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS WHERE K = 'NORTH'");

        [Fact]
        public Task ShouldAgreeOnWindowingAnAggregateOverAnAnyColumn() => SameThrough("ClrDataCursorWindow", "SELECT ID, MIN(V) OVER (PARTITION BY K), MAX(V) OVER (PARTITION BY K), SUM(V) OVER (PARTITION BY K) FROM ANYS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnARunningTotalOverAnAnyColumn() => SameThrough("ClrDataCursorWindow", "SELECT ID, SUM(V) OVER (ORDER BY ID) FROM ANYS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnTakingAnyValueOfAnAnyColumn() => SameThrough("ClrDataCursorAggregate", "SELECT ANY_VALUE(V), ANY_VALUE(S) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnDeviatingOverAnAnyColumn() => Same("SELECT VAR_POP(V), VAR_SAMP(V) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnFilteringAnAggregateOverAnAnyColumn() => Same("SELECT MIN(V) FILTER (WHERE ID > 1), SUM(V) FILTER (WHERE K = 'EAST') FROM ANYS");

        // and the same column read every way that already worked

        [Fact]
        public Task ShouldAgreeOnScanningAnAnyColumn() => Same("SELECT K, V, S FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnCountingAnAnyColumn() => Same("SELECT K, COUNT(V), COUNT(*) FROM ANYS GROUP BY K ORDER BY K");

        [Fact]
        public Task ShouldAgreeOnAggregatingACastAnyColumn() => Same("SELECT MIN(CAST(V AS INTEGER)), MAX(CAST(V AS INTEGER)), SUM(CAST(V AS INTEGER)), AVG(CAST(V AS INTEGER)) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnAGroupedAggregateOverACastAnyColumn() => Same("SELECT K, MIN(CAST(V AS INTEGER)), SUM(CAST(V AS INTEGER)) FROM ANYS GROUP BY K ORDER BY K");

        [Fact]
        public Task ShouldAgreeOnCastingAnAnyColumnToVarchar() => Same("SELECT ID, CAST(G AS VARCHAR) FROM CASTS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnCastingAnAnyColumnToANumber() => Same("SELECT ID, CAST(N AS INTEGER), CAST(N AS DECIMAL(10, 2)) FROM CASTS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnCastingAnAnyColumnOfMillisToATimestamp() => Same("SELECT ID, CAST(M AS TIMESTAMP) FROM CASTS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnRefusingAUuidCastOfAnAnyColumn() =>
            SameFailure("SELECT ID, CAST(G AS UUID) FROM CASTS ORDER BY ID", "to type 'org.apache.calcite.util.UuidValue'");

        [Fact]
        public Task ShouldAgreeOnCastingAnAnyColumnThroughVarcharToUuid() => Same("SELECT ID, CAST(CAST(G AS VARCHAR) AS UUID) FROM CASTS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnCastingAnAnyColumnThroughVarcharToATimestamp() => Same("SELECT ID, CAST(CAST(T AS VARCHAR) AS TIMESTAMP) FROM CASTS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnDistinct() => Same("SELECT DISTINCT REGION FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAUnion() => Same("SELECT ID FROM SALES UNION SELECT K FROM SORTED");

        [Fact]
        public Task ShouldAgreeOnAUnionAll() => Same("SELECT ID FROM SALES UNION ALL SELECT K FROM SORTED");

        [Fact]
        public Task ShouldAgreeOnAnIntersect() => Same("SELECT ID FROM SALES INTERSECT SELECT K FROM SORTED");

        [Fact]
        public Task ShouldAgreeOnAMinus() => Same("SELECT ID FROM SALES EXCEPT SELECT K FROM SORTED");

        [Fact]
        public Task ShouldAgreeOnAJoin() => Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID = t.K");

        [Fact]
        public Task ShouldAgreeOnALeftJoin() => Same("SELECT s.ID, t.V FROM SALES s LEFT JOIN SORTED t ON s.ID = t.K");

        [Fact]
        public Task ShouldAgreeOnANestedLoopJoin() => Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID > t.K");

        // A right and a full join over twelve build-side keys with no ORDER BY. The rows that matched nothing
        // come out at the end, and twelve is the one size at which the collection they are walked from
        // decides their order: the lookup is a table of 16 and the HashSet copied from its key set is a table
        // of 32.

        [Fact]
        public Task ShouldAgreeOnARightJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrEnumerableHashJoin", "SELECT a.N, b.K FROM (SELECT * FROM WIDE WHERE N < 3) a RIGHT JOIN WIDE b ON a.K = b.K");

        [Fact]
        public Task ShouldAgreeOnAFullJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrEnumerableHashJoin", "SELECT a.N, b.K FROM (SELECT * FROM WIDE WHERE N < 3) a FULL JOIN WIDE b ON a.K = b.K");

        [Fact]
        public Task ShouldAgreeOnASemiJoin() => Same("SELECT ID FROM SALES WHERE ID IN (SELECT K FROM SORTED)");

        [Fact]
        public Task ShouldAgreeOnAWindow() => Same("SELECT ID, SUM(AMOUNT) OVER (PARTITION BY REGION ORDER BY ID) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnARowNumber() => Same("SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnACorrelatedSubQuery() =>
            Same("SELECT ID, (SELECT COUNT(*) FROM SORTED t WHERE t.K = s.ID) FROM SALES s");

        /// <remarks>
        /// A merge join is only ever chosen over a hash join where both inputs carry a collation, and only
        /// where the hash join is not available to be cheaper — VolcanoCost compares the row count and
        /// nothing else, so the planner keeps whichever it saw first.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAMergeJoin() =>
            SameThrough("ClrEnumerableMergeJoin", "SELECT a.K, b.V FROM SORTED a JOIN SORTED b ON a.K = b.K", excludeHashJoin: true);

        [Fact]
        public Task ShouldAgreeOnAMergeJoinWithTies() =>
            Same("SELECT a.K, a.V, b.V FROM SORTED a JOIN SORTED b ON a.K = b.K ORDER BY a.K, a.V, b.V", excludeHashJoin: true);

        [Fact]
        public Task ShouldAgreeOnALeftMergeJoin() =>
            Same("SELECT a.K, b.V FROM SORTED a LEFT JOIN SORTED b ON a.K = b.K", excludeHashJoin: true);

        [Fact]
        public Task ShouldAgreeOnAMergeUnion() =>
            Same("SELECT K FROM SORTED UNION SELECT K FROM SORTED ORDER BY 1");

        [Fact]
        public Task ShouldAgreeOnASortedAggregate() =>
            Same("SELECT K, COUNT(*) FROM SORTED GROUP BY K ORDER BY K", sortedAggregate: true);

        [Fact]
        public Task ShouldAgreeOnABatchNestedLoopJoin() =>
            Same("SELECT s.ID, t.V FROM SALES s JOIN SORTED t ON s.ID > t.K", batchNestedLoopJoin: true);

        [Fact]
        public Task ShouldAgreeOnALimitSort() =>
            Same("SELECT * FROM SALES ORDER BY AMOUNT FETCH NEXT 2 ROWS ONLY", limitSort: true);

        [Fact]
        public Task ShouldAgreeOnGroupingSets() =>
            Same("SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY GROUPING SETS ((REGION), ()) ORDER BY 1");

        [Fact]
        public Task ShouldAgreeOnACube() =>
            Same("SELECT REGION, LABEL, COUNT(*) FROM SALES GROUP BY CUBE(REGION, LABEL) ORDER BY 1, 2");

        // Eight key fields, which is the arity that builds the key through FlatLists.copyOf over an array of
        // Comparable. See ClrDataCursorConventionDifferentialTests.ShouldAgreeOnARollupOverEveryColumn.

        [Fact]
        public Task ShouldAgreeOnARollupOverEveryColumn() =>
            Same("SELECT ID, REGION, AMOUNT, LABEL, COUNT(*) FROM SALES GROUP BY ROLLUP(ID, REGION, AMOUNT, LABEL) ORDER BY 1, 2, 3, 4, 5");

        [Fact]
        public Task ShouldAgreeOnAnAntiJoin() =>
            Same("SELECT ID FROM SALES WHERE ID NOT IN (SELECT K FROM SORTED WHERE K IS NOT NULL)");

        [Fact]
        public Task ShouldAgreeOnAnExists() =>
            Same("SELECT ID FROM SALES s WHERE EXISTS (SELECT 1 FROM SORTED t WHERE t.K = s.ID)");

        [Fact]
        public Task ShouldAgreeOnAMultiset() =>
            Same("SELECT REGION, COLLECT(AMOUNT) FROM SALES GROUP BY REGION ORDER BY 1");

        [Fact]
        public Task ShouldAgreeOnAnUncollect() =>
            Same("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(x)");

        // UNNEST over a column of type ANY, whose element type is not known until a row is read. Neither
        // convention gets this from Calcite — ClrDataCursorUncollect says why and
        // ClrDataCursorConventionDifferentialTests asserts the answers by hand — so what is checked here is that the
        // asynchronous convention gives what the synchronous one gives, through its own node. The plan is a
        // correlate over the uncollect in both, because decorrelation cannot take an UNNEST of a correlation
        // variable apart. The correlate is the sequence convention's, so its uncollect would be too unless
        // that rule is taken away — and Calcite's with it, which otherwise wins once ours is gone and fails
        // at implement over ANY. With both gone the uncollect is this convention's, under the converter
        // out, and the correlation variable reaches it because that converter replays the ones in scope.

        static readonly RelOptRule[] TheirUncollect = [org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_UNCOLLECT_RULE, ClrEnumerableRules.ClrEnumerableUncollectRule];

        [Fact]
        public Task ShouldAgreeOnUncollectingAnAnyColumn() =>
            SameThrough("ClrDataCursorUncollect", "SELECT d.ID, t.X FROM DOCS d, UNNEST(d.TAGS) AS t(X)", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnUncollectingAnAnyColumnOfMixedNumericTypes() =>
            SameThrough("ClrDataCursorUncollect", "SELECT d.ID, t.X FROM DOCS d, UNNEST(d.NUMS) AS t(X)", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnOuterUncollectingAnAnyColumn() =>
            SameThrough("ClrDataCursorUncollect", "SELECT d.ID, t.X FROM DOCS d LEFT JOIN UNNEST(d.TAGS) AS t(X) ON TRUE", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnUncollectingAnAnyColumnWithOrdinality() =>
            SameThrough("ClrDataCursorUncollect", "SELECT d.ID, t.X FROM DOCS d, UNNEST(d.TAGS) WITH ORDINALITY AS t(X)", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnAggregatingOverAnUncollectedAnyColumn() =>
            SameThrough("ClrDataCursorUncollect", "SELECT d.ID, COUNT(*), MIN(t.X), MAX(t.X) FROM DOCS d, UNNEST(d.NUMS) AS t(X) GROUP BY d.ID ORDER BY 1", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnFilteringTheOuterRowOfAnUncollectedAnyColumn() =>
            SameThrough("ClrDataCursorUncollect", "SELECT t.X FROM DOCS d, UNNEST(d.TAGS) AS t(X) WHERE d.ID = 1", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnACaseAndCoalesce() =>
            Same("SELECT ID, CASE WHEN AMOUNT IS NULL THEN -1 ELSE AMOUNT END, COALESCE(AMOUNT, 0) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAggregatesOverNulls() =>
            Same("SELECT REGION, COUNT(AMOUNT), COUNT(*), SUM(AMOUNT), AVG(AMOUNT), MIN(AMOUNT), MAX(AMOUNT) FROM SALES GROUP BY REGION ORDER BY 1");

        [Fact]
        public Task ShouldAgreeOnAWindowWithFraming() =>
            Same("SELECT ID, SUM(AMOUNT) OVER (ORDER BY ID ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnRankAndLag() =>
            Same("SELECT ID, RANK() OVER (PARTITION BY REGION ORDER BY AMOUNT), LAG(AMOUNT) OVER (ORDER BY ID) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAnEmptyResult() =>
            Same("SELECT * FROM SALES WHERE 1 = 0");

        [Fact]
        public Task ShouldAgreeOnASelfJoinProducingNoRows() =>
            Same("SELECT a.ID FROM SALES a JOIN SALES b ON a.ID = b.ID + 1000");


        // ASOF join. Its rule is in both conventions' default lists, so nothing has to be turned on; what it
        // needs is a match condition and a key, and SALES has both.

        [Fact]
        public Task ShouldAgreeOnAnAsofJoin() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        [Fact]
        public Task ShouldAgreeOnALeftAsofJoin() =>
            Same("SELECT a.ID, b.ID FROM SALES a LEFT ASOF JOIN (SELECT * FROM SALES WHERE ID > 3) b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        [Fact]
        public Task ShouldAgreeOnAnAsofJoinLookingForward() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID > a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        /// <remarks>
        /// The order an ASOF join produces is the order of the map it indexes its left input by, so a query
        /// without an ORDER BY compares that order too.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAnAsofJoinsOwnOrder() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION");

        [Fact]
        public Task ShouldAgreeOnALeftAsofJoinWithANullKey() =>
            Same("SELECT a.ID, b.ID FROM SALES a LEFT ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.AMOUNT = b.AMOUNT ORDER BY a.ID");

        // the mark join paths, which the ordinary sub-query pass never reaches.

        [Fact]
        public Task ShouldAgreeOnAMarkedExists() =>
            Same("SELECT ID FROM SALES WHERE EXISTS (SELECT 1 FROM SALES S2 WHERE S2.ID > 4) ORDER BY ID", markJoin: true);

        [Fact]
        public Task ShouldAgreeOnAMarkedIn() =>
            Same("SELECT ID FROM SALES WHERE AMOUNT IN (SELECT AMOUNT FROM SALES WHERE ID > 3) ORDER BY ID", markJoin: true);

        [Fact]
        public Task ShouldAgreeOnAMarkedCorrelatedExists() =>
            Same("SELECT ID FROM SALES S1 WHERE EXISTS (SELECT 1 FROM SALES S2 WHERE S2.REGION = S1.REGION AND S2.ID > 3) ORDER BY ID", markJoin: true);

        // A recursive query: the repeat union and the table spool are this convention's own, and only the
        // transient scan is Calcite's, under the converter in, because no scan of either Clr convention
        // reads a transient table (CALCITE-3673) and this harness does not add the interpreter rule. The
        // iterative part is opened afresh each round, inside whichever advance started the round, so this
        // is the query that exercises both openers of a deferred input. SameThrough is what says the node
        // is ours; without it these would pass on a plan carried wholly by Calcite too.

        [Fact]
        public Task ShouldAgreeOnARecursiveQuery() =>
            SameThrough("ClrDataCursorRepeatUnion", "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [Fact]
        public Task ShouldAgreeOnARecursiveQueryOfSeveralColumns() =>
            SameThrough("ClrDataCursorTableSpool", "WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        // ------------------------------------------------------------------ built by hand
        //
        // Shapes SQL cannot express, through RunRel. Each has a twin in ClrEnumerableConventionRelTests, which checks
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
        [Fact]
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
        [Fact]
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
        /// and reaches it through <c>ClrEnumerableCalc</c> rather than through a converter. Without this
        /// a failure anywhere above is ambiguous between the shape and the route.
        /// </remarks>
        [Fact]
        public Task ShouldPlanAHandBuiltScanThroughThisConvention() =>
            SameRelThrough("ClrDataCursorCalc", builder => builder
                .scan("SORTED")
                .filter(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, builder.field(0), builder.literal(I(1))))
                .build());

        [Fact]
        public Task ShouldAgreeOnATableFunction() =>
            Same("SELECT * FROM TABLE(NUMBERS(3))");

        /// <summary>
        /// A window table function over a table whose rows are awaited.
        /// </summary>
        /// <remarks>
        /// The one node whose two bodies differ in <em>what they do with the input</em> rather than in which
        /// operator set they name. Everything that builds a window is Calcite's and is linq4j, and a linq4j
        /// <c>Enumerable</c> has nowhere to suspend, so the awaiting body pulls its input and blocks a thread
        /// per row before handing it over. The rows above the node are awaited again, which is why this is a
        /// differential test like the rest rather than a plan assertion.
        ///
        /// <para>Written because the pull was lost once. The node inherited the default
        /// <c>ImplementAsync</c>, which handed <c>JavaSequences.ToJava</c> an <c>IAsyncEnumerable</c>, and
        /// <c>Expression.Call</c> refused it. Nothing in the suite reached a window table function over an
        /// awaited input, so the whole suite stayed green over it.</para>
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAWindowTableFunction() =>
            Same("SELECT \"ID\", \"window_start\", \"window_end\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) ORDER BY \"ID\"");

        /// <summary>
        /// A window table function whose rows are then aggregated.
        /// </summary>
        /// <remarks>
        /// The window's output crosses back to awaited and an aggregate of this convention reads it, so the
        /// crossing is exercised in both directions in one plan.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAnAggregateOverAWindowTableFunction() =>
            Same("SELECT \"window_start\", COUNT(*) FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) GROUP BY \"window_start\" ORDER BY 1");

        /// <summary>
        /// A table function joined to a table is refused while the plan is implemented, and named.
        /// </summary>
        /// <remarks>
        /// The same defect of Calcite's that <c>ShouldRefuseATableFunctionInAJoin</c> records:
        /// <c>EnumerableSort</c> optimises the scan's ARRAY format to SCALAR and hands the <c>Object[]</c>
        /// rows on unchanged, so the sequence carries arrays where its row type says
        /// <c>java.lang.Integer</c>.
        ///
        /// <para><b>It surfaces here exactly as it does in a plan read synchronously, and that is the
        /// change.</b> While there were two conventions this one had no table function rule, so the whole
        /// subtree went to <c>EnumerableConvention</c> under a converter, the converter believed
        /// <c>result.physType.getFormat()</c> — the thing that is wrong — and the mismatch arrived as an
        /// <c>InvalidCastException</c> at the first row. The sort is now our node in either mode, so
        /// <c>RequireRowType</c> catches it while the plan is being implemented and says which node handed
        /// up what.</para>
        ///
        /// <para>Nothing to fix on this side: the convention does what Calcite does, and the check that
        /// would catch it is a check on Calcite's own answer about its own rows.</para>
        /// </remarks>
        [Fact]
        public async Task ShouldRefuseATableFunctionJoinedToATable()
        {
            var act = async () => await Run("SELECT s.ID FROM SALES s, TABLE(NUMBERS(6)) n WHERE s.ID = n.N ORDER BY 1", true);

            (await act.Should().ThrowAsync<java.lang.IllegalStateException>())
                .WithInnerException<java.lang.IllegalStateException>()
                .WithMessage("*ClrEnumerableSort handed up a sequence of System.Object[] where its row type is java.lang.Integer*");
        }

        /// <summary>
        /// MATCH_RECOGNIZE over a table that only yields its rows asynchronously now plans, and runs.
        /// </summary>
        /// <remarks>
        /// <b>A capability the unification bought, and a blocking one.</b> This query could not be planned at
        /// all while there were two conventions: nothing here can write a MATCH_RECOGNIZE —
        /// <c>PassedRowsInputGetter</c> and <c>PrevInputGetter</c> are package-private types Calcite casts to
        /// by name — so the node has to be Calcite's, Calcite's node needs its input in
        /// <c>EnumerableConvention</c>, and the asynchronous convention had no converter out for it to arrive
        /// by. The planner said so and the query failed.
        ///
        /// <para>Now the scan is a node of the one convention and the converter out is the one that always
        /// existed. Calcite compiles its side with Janino and generated Java cannot await, so the sub-plan
        /// under that converter is implemented synchronously and the asynchronous leaf inside it is read
        /// across, <b>blocking a thread per row</b>. That is the cost, it is paid only by a query of this
        /// shape, and the alternative it replaces is the query not running.</para>
        /// </remarks>
        [Fact]
        public async Task ShouldRunAMatchRecognizeOverAnAsyncTable()
        {
            var rows = await Run("SELECT * FROM SALES MATCH_RECOGNIZE (ORDER BY ID MEASURES CLASSIFIER() AS cl PATTERN (a b) DEFINE a AS a.AMOUNT > 0, b AS b.AMOUNT > 0)", true);

            rows.Should().NotBeEmpty();
        }

    }

}
