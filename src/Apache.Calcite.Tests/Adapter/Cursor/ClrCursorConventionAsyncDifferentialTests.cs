using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Runs each query over tables that produce rows only asynchronously, opened and read with await, and over
    /// synchronous tables holding the same rows, opened and read synchronously, and requires the same rows.
    /// </summary>
    /// <remarks>
    /// The expected answer is this convention's synchronous reading rather than Calcite's, because Calcite
    /// cannot read the asynchronous tables. The synchronous reading is checked against Calcite in
    /// <see cref="ClrCursorConventionDifferentialTests"/>. Both schemas hold the rows of
    /// <see cref="AsyncTestRows"/>, so a difference comes from the awaiting operators rather than from the data.
    /// </remarks>
    public class ClrCursorConventionAsyncDifferentialTests
    {

        /// <summary>
        /// Puts Calcite's test and JDBC assemblies on the boot class path.
        /// </summary>
        static ClrCursorConventionAsyncDifferentialTests()
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

            // a table function whose call yields the rows: ClrCursorTableFunctionScan implements it, with no
            // input for either body to read
            rootSchema.add("NUMBERS", org.apache.calcite.schema.impl.TableFunctionImpl.create((java.lang.Class)typeof(NumbersTableFunction), "eval"));

            return rootSchema;
        }

        /// <summary>
        /// Plans a statement over the asynchronous or the synchronous schema and returns its rows, rendered.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="async">Whether to plan over the asynchronous tables and read with await, rather than over the
        /// synchronous tables and read synchronously.</param>
        /// <param name="planOnly">Whether to return the plan's text instead of its rows.</param>
        /// <param name="sortedAggregate">Whether to add the convention's sorted aggregate rule.</param>
        /// <param name="batchNestedLoopJoin">Whether to add the convention's batch nested loop join rule.</param>
        /// <param name="limitSort">Whether to add the convention's limit sort rule.</param>
        /// <param name="excludeHashJoin">Whether to remove both conventions' hash join rules.</param>
        /// <param name="excludeMergeJoin">Whether to remove both conventions' merge join rules.</param>
        /// <param name="markJoin">Whether to rewrite sub-queries into mark correlates with <see cref="MarkJoinSubQueryProgram"/>.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The rows rendered as text, or a single element holding the plan's text when
        /// <paramref name="planOnly"/> is set.</returns>
        static async Task<List<string>> Run(string sql, bool async, bool planOnly = false, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool markJoin = false, RelOptRule[]? remove = null)
        {
            var rootSchema = Schema(async);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();

            foreach (var rule in ClrCursorRules.Rules())
            {
                // removed for both sides alike, so that both plan the same shape
                if (excludeMergeJoin && rule == ClrCursorRules.ClrCursorMergeJoinRule)
                    continue;

                // likewise the hash join; DefaultRulesProgram removes only Calcite's
                if (excludeHashJoin && rule == ClrCursorRules.ClrCursorJoinRule)
                    continue;

                rules.add(rule);
            }

            // three rules the convention declares but leaves out of Rules(); a caller adds them explicitly
            if (sortedAggregate)
                rules.add(ClrCursorRules.ClrCursorSortedAggregateRule);
            if (batchNestedLoopJoin)
                rules.add(ClrCursorRules.ClrCursorBatchNestedLoopJoinRule);
            if (limitSort)
                rules.add(ClrCursorRules.ClrCursorLimitSortRule);
            foreach (var rule in ClrCursorRules.CalcRules())
                if (calcRules.contains(rule) == false)
                    calcRules.add(rule);

            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            rules.add(org.apache.calcite.rel.rules.CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    markJoin ? MarkJoinSubQueryProgram() : Programs.subQuery(Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider),
                    new DefaultRulesProgram(rules, false, excludeMergeJoin, excludeHashJoin, null, remove),
                    Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);

            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrCursorConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            // over the asynchronous tables the plan is opened and read with await; over the synchronous ones,
            // opened and read synchronously
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters)
                .ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

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
        /// Plans a rel built by <paramref name="build"/> over the asynchronous or the synchronous schema and
        /// returns its rows, rendered.
        /// </summary>
        /// <param name="build">Builds the logical plan against a builder over the chosen schema.</param>
        /// <param name="async">Whether to plan over the asynchronous tables and read with await, rather than over the
        /// synchronous tables and read synchronously.</param>
        /// <param name="planOnly">Whether to return the plan's text instead of its rows.</param>
        /// <param name="add">Rules to register alongside this convention's.</param>
        /// <param name="remove">Rules to remove once everything is registered.</param>
        /// <returns>The rows rendered as text, or a single element holding the plan's text when
        /// <paramref name="planOnly"/> is set.</returns>
        /// <remarks>
        /// The counterpart of <c>ClrCursorConventionDifferentialTests.RunRel</c>, for shapes SQL cannot reach,
        /// such as a recursive query whose step aggregates the working table: standard SQL does not allow an
        /// aggregate in a recursive term.
        ///
        /// <para>Each side builds the rel against its own schema, so it is built twice rather than shared.
        /// There is no parser, validator or sub-query program: the built rel is what the planner receives.</para>
        /// </remarks>
        static async Task<List<string>> RunRel(Func<RelBuilder, RelNode> build, bool async, bool planOnly = false, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var rootSchema = Schema(async);

            var rules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.Rules())
                rules.add(rule);

            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
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
                .run(planner, logical, logical.getTraitSet().replace(ClrCursorConvention.Instance).simplify(), empty, empty);

            var physical = Programs.hep(calcRules, true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider)
                .run(planner, chosen, chosen.getTraitSet(), empty, empty);

            if (planOnly)
                return [RelOptUtil.toString(physical)];

            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);

            var rows = new List<string>();

            // over the asynchronous tables the plan is opened and read with await; over the synchronous ones,
            // opened and read synchronously
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters)
                .ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

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
        /// Requires that a hand-built rel gives the same rows opened with await as opened synchronously.
        /// </summary>
        /// <param name="build">Builds the logical plan; it is called once for each schema.</param>
        /// <param name="add">Rules to register alongside this convention's, on both sides.</param>
        /// <param name="remove">Rules to remove once everything is registered, on both sides.</param>
        /// <returns>A task that completes when both readings have been compared.</returns>
        static async Task SameRel(Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            var async = await RunRel(build, true, add: add, remove: remove);
            var sync = await RunRel(build, false, add: add, remove: remove);

            async.Should().Equal(sync, "the plan should give what its synchronous open gives");
        }

        /// <summary>
        /// Requires the same rows, and that the plan contains the node named by <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node the asynchronous side's plan must contain.</param>
        /// <param name="build">Builds the logical plan; it is called once for each schema and once more for the plan text.</param>
        /// <param name="add">Rules to register alongside this convention's, on both sides.</param>
        /// <param name="remove">Rules to remove once everything is registered, on both sides.</param>
        /// <returns>A task that completes when the plan has been checked and both readings compared.</returns>
        /// <remarks>
        /// As for <see cref="SameThrough"/>: rows that agree do not show that the intended node was planned.
        /// </remarks>
        static async Task SameRelThrough(string node, Func<RelBuilder, RelNode> build, RelOptRule[]? add = null, RelOptRule[]? remove = null)
        {
            (await RunRel(build, true, planOnly: true, add: add, remove: remove))[0]
                .Should().Contain(node, "the plan should be planned through {0}", node);

            await SameRel(build, add, remove);
        }

        /// <summary>
        /// Boxes an integer as a <c>java.lang.Integer</c>, as a literal of a hand-built rel requires.
        /// </summary>
        /// <param name="value">The value to box.</param>
        /// <returns>The value as a Java <c>Integer</c>, which <c>RelBuilder.literal</c> reads as an INTEGER literal.</returns>
        static java.lang.Integer I(int value) => java.lang.Integer.valueOf(value);

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        /// <param name="rootSchema">The schema the plan was planned against.</param>
        /// <param name="parameters">The map the implementor stashed values into, which <c>get</c> answers from.</param>
        /// <remarks>
        /// <c>get</c> answers from the map the implementor stashed values into, because a plan reads those
        /// values back through the context at run time.
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
        /// <returns>A hep program applying Calcite's mark-correlate sub-query rules, costed with this convention's
        /// metadata provider.</returns>
        /// <remarks>
        /// The same program the synchronous harness uses. Without it the mark join paths are unreachable from
        /// SQL: the ordinary pass turns an EXISTS into a semi join.
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

            return Programs.of(builder.build(), true, Apache.Calcite.Extensions.Rel.Metadata.ClrCursorRelMetadata.Provider);
        }

        static string Render(object row)
        {
            if (row is object[] array)
                return string.Join("|", array.Select(Render));

            return row?.ToString() ?? "<null>";
        }

        /// <summary>
        /// Requires that a query gives the same rows opened with await as opened synchronously.
        /// </summary>
        /// <param name="sql">The query.</param>
        /// <param name="sortedAggregate">Whether to add the convention's sorted aggregate rule.</param>
        /// <param name="batchNestedLoopJoin">Whether to add the convention's batch nested loop join rule.</param>
        /// <param name="limitSort">Whether to add the convention's limit sort rule.</param>
        /// <param name="excludeHashJoin">Whether to remove both conventions' hash join rules.</param>
        /// <param name="excludeMergeJoin">Whether to remove both conventions' merge join rules.</param>
        /// <param name="markJoin">Whether to rewrite sub-queries into mark correlates.</param>
        /// <param name="remove">Rules to remove once everything is registered, on both sides.</param>
        /// <returns>A task that completes when both readings have been compared.</returns>
        static async Task Same(string sql, bool sortedAggregate = false, bool batchNestedLoopJoin = false, bool limitSort = false, bool excludeHashJoin = false, bool excludeMergeJoin = false, bool markJoin = false, RelOptRule[]? remove = null)
        {
            var async = await Run(sql, true, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, markJoin: markJoin, remove: remove);
            var sync = await Run(sql, false, sortedAggregate: sortedAggregate, batchNestedLoopJoin: batchNestedLoopJoin, limitSort: limitSort, excludeHashJoin: excludeHashJoin, excludeMergeJoin: excludeMergeJoin, markJoin: markJoin, remove: remove);

            async.Should().Equal(sync, "'{0}' should give what its synchronous open gives", sql);
        }

        /// <summary>
        /// Requires that a statement fails with the same innermost exception whether it is read with await or
        /// synchronously.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="message">Part of the message the synchronous reading must fail with.</param>
        /// <returns>A task that completes when both failures have been compared.</returns>
        /// <remarks>
        /// <see cref="Same"/> for a statement that throws. Whether the failure matches Calcite's is checked in
        /// <see cref="ClrCursorConventionDifferentialTests"/>, which compares against <c>EnumerableConvention</c>.
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
        /// Requires the same rows, and that the plan contains the node named by <paramref name="node"/>.
        /// </summary>
        /// <param name="node">The node the asynchronous side's plan must contain.</param>
        /// <param name="sql">The query.</param>
        /// <param name="sortedAggregate">Whether to add the convention's sorted aggregate rule.</param>
        /// <param name="batchNestedLoopJoin">Whether to add the convention's batch nested loop join rule.</param>
        /// <param name="limitSort">Whether to add the convention's limit sort rule.</param>
        /// <param name="excludeHashJoin">Whether to remove both conventions' hash join rules.</param>
        /// <param name="excludeMergeJoin">Whether to remove both conventions' merge join rules.</param>
        /// <param name="remove">Rules to remove once everything is registered, on both sides.</param>
        /// <returns>A task that completes when the plan has been checked and both readings compared.</returns>
        /// <remarks>
        /// Rows that agree do not show which node produced them; a query planned through some other node than
        /// the one intended would pass <see cref="Same"/> without testing it.
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
        public Task ShouldAgreeOnAFilter() => SameThrough("ClrCursorCalc", "SELECT * FROM SALES WHERE AMOUNT > 10");

        [Fact]
        public Task ShouldAgreeOnAProjection() => Same("SELECT ID, REGION FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAnExpression() => Same("SELECT ID + 1, UPPER(REGION) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnANullableColumn() => Same("SELECT AMOUNT FROM SALES");

        [Fact]
        public Task ShouldAgreeOnASort() => SameThrough("ClrCursorSort", "SELECT * FROM SALES ORDER BY AMOUNT");

        [Fact]
        public Task ShouldAgreeOnASortWithLimit() => Same("SELECT * FROM SALES ORDER BY ID OFFSET 1 ROWS FETCH NEXT 3 ROWS ONLY");

        [Fact]
        public Task ShouldAgreeOnValues() => SameThrough("ClrCursorValues", "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y)");

        [Fact]
        public Task ShouldAgreeOnAnAggregate() => SameThrough("ClrCursorAggregate", "SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION");

        [Fact]
        public Task ShouldAgreeOnACountOverEverything() => Same("SELECT COUNT(*) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnAGrandTotal() => Same("SELECT SUM(AMOUNT), MIN(ID), MAX(ID) FROM SALES");

        // MIN, MAX, SUM and AVG over a column of type ANY, whose Java class is Object. These are implemented
        // by ClrAnyAggImplementors rather than Calcite, and ClrCursorConventionDifferentialTests asserts their
        // answers; here the awaiting reading must match the synchronous one, including mixed numeric types,
        // strings and an empty group.

        [Fact]
        public Task ShouldAgreeOnAggregatingAnAnyColumn() => SameThrough("ClrCursorAggregate", "SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnAGroupedAggregateOverAnAnyColumn() => SameThrough("ClrCursorAggregate", "SELECT K, MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS GROUP BY K ORDER BY K");

        [Fact]
        public Task ShouldAgreeOnAggregatingAnAnyColumnOfStrings() => Same("SELECT MIN(S), MAX(S) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnAggregatingAnEmptyAnyColumn() => Same("SELECT MIN(V), MAX(V), SUM(V), AVG(V) FROM ANYS WHERE K = 'NORTH'");

        [Fact]
        public Task ShouldAgreeOnWindowingAnAggregateOverAnAnyColumn() => SameThrough("ClrCursorWindow", "SELECT ID, MIN(V) OVER (PARTITION BY K), MAX(V) OVER (PARTITION BY K), SUM(V) OVER (PARTITION BY K) FROM ANYS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnARunningTotalOverAnAnyColumn() => SameThrough("ClrCursorWindow", "SELECT ID, SUM(V) OVER (ORDER BY ID) FROM ANYS ORDER BY ID");

        [Fact]
        public Task ShouldAgreeOnTakingAnyValueOfAnAnyColumn() => SameThrough("ClrCursorAggregate", "SELECT ANY_VALUE(V), ANY_VALUE(S) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnDeviatingOverAnAnyColumn() => Same("SELECT VAR_POP(V), VAR_SAMP(V) FROM ANYS");

        [Fact]
        public Task ShouldAgreeOnFilteringAnAggregateOverAnAnyColumn() => Same("SELECT MIN(V) FILTER (WHERE ID > 1), SUM(V) FILTER (WHERE K = 'EAST') FROM ANYS");

        // the same column read without an ANY aggregate implementor: scanned, counted and cast

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

        // A right and a full join over twelve build-side keys with no ORDER BY. The unmatched rows come last,
        // and at twelve keys their order depends on which collection they are read from: the lookup has 16
        // buckets and a HashSet copied from its key set has 32.

        [Fact]
        public Task ShouldAgreeOnARightJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrCursorHashJoin", "SELECT a.N, b.K FROM (SELECT * FROM WIDE WHERE N < 3) a RIGHT JOIN WIDE b ON a.K = b.K");

        [Fact]
        public Task ShouldAgreeOnAFullJoinsOwnOrderOverTwelveKeys() =>
            SameThrough("ClrCursorHashJoin", "SELECT a.N, b.K FROM (SELECT * FROM WIDE WHERE N < 3) a FULL JOIN WIDE b ON a.K = b.K");

        [Fact]
        public Task ShouldAgreeOnASemiJoin() => Same("SELECT ID FROM SALES WHERE ID IN (SELECT K FROM SORTED)");

        [Fact]
        public Task ShouldAgreeOnAWindow() => Same("SELECT ID, SUM(AMOUNT) OVER (PARTITION BY REGION ORDER BY ID) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnARowNumber() => Same("SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM SALES");

        [Fact]
        public Task ShouldAgreeOnACorrelatedSubQuery() =>
            Same("SELECT ID, (SELECT COUNT(*) FROM SORTED t WHERE t.K = s.ID) FROM SALES s");

        /// <summary>
        /// A merge join over two sorted inputs gives the same rows read either way.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The hash join rule is removed, because otherwise the planner may keep the hash join: a merge join
        /// needs both inputs sorted, and <c>VolcanoCost</c> compares row counts only, so of two equal plans the
        /// planner keeps the one it registered first.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAMergeJoin() =>
            SameThrough("ClrCursorMergeJoin", "SELECT a.K, b.V FROM SORTED a JOIN SORTED b ON a.K = b.K", excludeHashJoin: true);

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

        // Enough key fields that the key is built through FlatLists.copyOf over an array of Comparable. See
        // ClrCursorConventionDifferentialTests.ShouldAgreeOnARollupOverEveryColumn.

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

        // UNNEST over a column of type ANY, whose element type is not known until a row is read. Calcite
        // cannot run this (ClrCursorUncollect explains why) and ClrCursorConventionDifferentialTests asserts
        // the answers by hand; here the awaiting reading must match the synchronous one through this
        // convention's node. The plan is a correlate over the uncollect, because decorrelation cannot remove
        // an UNNEST of a correlation variable. Calcite's uncollect rule is removed, because otherwise the
        // planner may keep Calcite's node, which fails to implement over ANY.

        static readonly RelOptRule[] TheirUncollect = [org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_UNCOLLECT_RULE];

        [Fact]
        public Task ShouldAgreeOnUncollectingAnAnyColumn() =>
            SameThrough("ClrCursorUncollect", "SELECT d.ID, t.X FROM DOCS d, UNNEST(d.TAGS) AS t(X)", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnUncollectingAnAnyColumnOfMixedNumericTypes() =>
            SameThrough("ClrCursorUncollect", "SELECT d.ID, t.X FROM DOCS d, UNNEST(d.NUMS) AS t(X)", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnOuterUncollectingAnAnyColumn() =>
            SameThrough("ClrCursorUncollect", "SELECT d.ID, t.X FROM DOCS d LEFT JOIN UNNEST(d.TAGS) AS t(X) ON TRUE", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnUncollectingAnAnyColumnWithOrdinality() =>
            SameThrough("ClrCursorUncollect", "SELECT d.ID, t.X, t.O FROM DOCS d, UNNEST(d.TAGS) WITH ORDINALITY AS t(X, O)", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnAggregatingOverAnUncollectedAnyColumn() =>
            SameThrough("ClrCursorUncollect", "SELECT d.ID, COUNT(*), MIN(t.X), MAX(t.X) FROM DOCS d, UNNEST(d.NUMS) AS t(X) GROUP BY d.ID ORDER BY 1", remove: TheirUncollect);

        [Fact]
        public Task ShouldAgreeOnFilteringTheOuterRowOfAnUncollectedAnyColumn() =>
            SameThrough("ClrCursorUncollect", "SELECT t.X FROM DOCS d, UNNEST(d.TAGS) AS t(X) WHERE d.ID = 1", remove: TheirUncollect);

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


        // ASOF join. Its rule is in both conventions' default rules, so none needs adding.

        [Fact]
        public Task ShouldAgreeOnAnAsofJoin() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        [Fact]
        public Task ShouldAgreeOnALeftAsofJoin() =>
            Same("SELECT a.ID, b.ID FROM SALES a LEFT ASOF JOIN (SELECT * FROM SALES WHERE ID > 3) b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        [Fact]
        public Task ShouldAgreeOnAnAsofJoinLookingForward() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID > a.ID ON a.REGION = b.REGION ORDER BY a.ID");

        /// <summary>
        /// An ASOF join with no ORDER BY gives the same rows in the same order read either way.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// An ASOF join emits rows in the order of the map it indexes its left input by, so with no ORDER BY
        /// this compares that order too.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAnAsofJoinsOwnOrder() =>
            Same("SELECT a.ID, b.ID FROM SALES a ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.REGION = b.REGION");

        [Fact]
        public Task ShouldAgreeOnALeftAsofJoinWithANullKey() =>
            Same("SELECT a.ID, b.ID FROM SALES a LEFT ASOF JOIN SALES b MATCH_CONDITION b.ID <= a.ID ON a.AMOUNT = b.AMOUNT ORDER BY a.ID");

        // the mark join paths, which only MarkJoinSubQueryProgram reaches

        [Fact]
        public Task ShouldAgreeOnAMarkedExists() =>
            Same("SELECT ID FROM SALES WHERE EXISTS (SELECT 1 FROM SALES S2 WHERE S2.ID > 4) ORDER BY ID", markJoin: true);

        [Fact]
        public Task ShouldAgreeOnAMarkedIn() =>
            Same("SELECT ID FROM SALES WHERE AMOUNT IN (SELECT AMOUNT FROM SALES WHERE ID > 3) ORDER BY ID", markJoin: true);

        [Fact]
        public Task ShouldAgreeOnAMarkedCorrelatedExists() =>
            Same("SELECT ID FROM SALES S1 WHERE EXISTS (SELECT 1 FROM SALES S2 WHERE S2.REGION = S1.REGION AND S2.ID > 3) ORDER BY ID", markJoin: true);

        // A join on two cross-input inequalities. Calcite's IE join rule is removed, because registerDefaultRules
        // registers it and the planner keeps whichever equal-cost node it registered first.

        static readonly RelOptRule[] TheirIeJoin = [org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_IE_JOIN_RULE];

        [Fact]
        public Task ShouldAgreeOnAnIeJoin() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.ID, b.ID FROM SALES a JOIN SALES b ON a.ID < b.ID AND a.AMOUNT > b.AMOUNT ORDER BY 1, 2", remove: TheirIeJoin);

        /// <summary>
        /// With no ORDER BY, the IE join's own row order agrees; that order comes from its two sorts, which run
        /// at the open after both inputs are drained, whichever open is used.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public Task ShouldAgreeOnAnIeJoinsOwnOrder() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.ID, b.ID FROM SALES a JOIN SALES b ON a.ID < b.ID AND a.AMOUNT > b.AMOUNT", remove: TheirIeJoin);

        [Fact]
        public Task ShouldAgreeOnAnIeJoinWithAResidualInequality() =>
            SameThrough("ClrCursorIEJoin", "SELECT a.ID, b.ID FROM SALES a JOIN SALES b ON a.ID < b.ID AND a.AMOUNT > b.AMOUNT AND a.LABEL < b.LABEL ORDER BY 1, 2", remove: TheirIeJoin);

        // A recursive query: the repeat union and the table spool are this convention's, and only the
        // transient scan is Calcite's, under a converter, because this convention's scan does not read a
        // transient table and this harness does not add the interpreter rule. The iterative part is opened
        // afresh each round inside whichever advance started the round, so both openers of a deferred input
        // are exercised. SameThrough checks that the nodes are this convention's.

        [Fact]
        public Task ShouldAgreeOnARecursiveQuery() =>
            SameThrough("ClrCursorRepeatUnion", "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 4) SELECT n FROM t ORDER BY 1");

        [Fact]
        public Task ShouldAgreeOnARecursiveQueryOfSeveralColumns() =>
            SameThrough("ClrCursorTableSpool", "WITH RECURSIVE t(n, m) AS (VALUES (1, 10) UNION ALL SELECT n + 1, m + 10 FROM t WHERE n < 4) SELECT n, m FROM t ORDER BY 1");

        // ------------------------------------------------------------------ built by hand
        //
        // Shapes SQL cannot express, through RunRel. Each has a twin in ClrCursorConventionRelTests, which checks
        // the synchronous open against Calcite; these check the awaiting open against the synchronous one.

        /// <summary>
        /// A recursive query whose step aggregates the working table rather than reading it row by row.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>repeatUnion</c> does not restore its sentinel between the seed and the first round, so after a
        /// seed that emitted a row the first empty round does not stop the sequence. An aggregate step makes
        /// the extra round visible, because <c>COUNT(*)</c> yields a row over no rows; a step that reads the
        /// table row by row cannot show it.
        ///
        /// <para>The query must be a UNION rather than a UNION ALL to terminate: the spool is cleared by a round
        /// that wrote nothing, so the step oscillates, and deduplication ends it. Under UNION ALL it never
        /// ends, under Calcite as well.</para>
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
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The ordinary shape, which SQL can express and <c>ShouldAgreeOnARecursiveQuery</c> runs. Built by hand
        /// here so that a failure of the aggregating test above can be attributed to the aggregate rather than
        /// to building by hand.
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
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// Checks the hand-built route itself: a rel built rather than parsed is planned into this convention,
        /// through <c>ClrCursorCalc</c> rather than a converter.
        /// </remarks>
        [Fact]
        public Task ShouldPlanAHandBuiltScanThroughThisConvention() =>
            SameRelThrough("ClrCursorCalc", builder => builder
                .scan("SORTED")
                .filter(builder.call(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, builder.field(0), builder.literal(I(1))))
                .build());

        [Fact]
        public Task ShouldAgreeOnATableFunction() =>
            Same("SELECT * FROM TABLE(NUMBERS(3))");

        /// <summary>
        /// A window table function over a table whose rows are awaited.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The window table function scan's two bodies differ in how they read their input rather than only in
        /// which operators they call. The window is built by Calcite's linq4j code, and a linq4j
        /// <c>Enumerable</c> cannot suspend, so the awaiting body reads its input synchronously, blocking a
        /// thread per row, before handing it over; the rows above the node are awaited again. The node
        /// therefore needs its own <c>ImplementAsync</c> rather than the default.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAWindowTableFunction() =>
            Same("SELECT \"ID\", \"window_start\", \"window_end\" FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) ORDER BY \"ID\"");

        /// <summary>
        /// A window table function whose rows are then aggregated.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The window's output is awaited again by an aggregate of this convention, so one plan crosses between
        /// awaiting and synchronous reading in both directions.
        /// </remarks>
        [Fact]
        public Task ShouldAgreeOnAnAggregateOverAWindowTableFunction() =>
            Same("SELECT \"window_start\", COUNT(*) FROM TABLE(TUMBLE(TABLE \"EVENTS\", DESCRIPTOR(\"ROWTIME\"), INTERVAL '1' HOUR)) GROUP BY \"window_start\" ORDER BY 1");

        /// <summary>
        /// A table function joined to a table, sorted for a merge join, is refused while the plan is
        /// implemented, naming the sort and both types.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// This is the Calcite defect <c>ShouldRefuseATableFunctionInAJoin</c> records: <c>EnumerableSort</c>
        /// optimises the scan's ARRAY format to SCALAR but passes the <c>Object[]</c> rows on unchanged, so the
        /// rows are arrays where the row type says <c>java.lang.Integer</c>. The implementor's row type check
        /// catches it here exactly as it does for a plan opened synchronously. The convention reproduces
        /// Calcite; the fix belongs in <c>EnumerableSort</c>.
        /// </remarks>
        [Fact]
        public async Task ShouldRefuseATableFunctionJoinedToATable()
        {
            // the hash join rule is removed; with it the planner chooses a hash join and there is no sort
            var act = async () => await Run("SELECT s.ID FROM SALES s, TABLE(NUMBERS(6)) n WHERE s.ID = n.N ORDER BY 1", true, excludeHashJoin: true);

            (await act.Should().ThrowAsync<java.lang.IllegalStateException>())
                .WithInnerException<java.lang.IllegalStateException>()
                .WithMessage("*ClrCursorSort handed up an open of System.Object[] where its row type is java.lang.Integer*");
        }

        /// <summary>
        /// MATCH_RECOGNIZE over a table that only yields its rows asynchronously plans through
        /// <see cref="ClrCursorMatch"/>, and gives the rows the same table gives read synchronously.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// This used to run as Calcite's node under a converter, whose generated Java cannot await, so the
        /// asynchronous leaf under it was read across a thread blocked per row. The awaiting body of
        /// <see cref="ClrCursorMatch"/> awaits its input like every other node's.
        /// </remarks>
        [Fact]
        public async Task ShouldRunAMatchRecognizeOverAnAsyncTable()
        {
            const string sql = "SELECT * FROM SALES MATCH_RECOGNIZE (ORDER BY ID MEASURES CLASSIFIER() AS cl PATTERN (a b) DEFINE a AS a.AMOUNT > 0, b AS b.AMOUNT > 0)";

            (await Run(sql, true, planOnly: true))[0].Should().Contain("ClrCursorMatch");

            var rows = await Run(sql, true);
            rows.Should().NotBeEmpty();
            rows.Should().Equal(await Run(sql, false));
        }

    }

}
