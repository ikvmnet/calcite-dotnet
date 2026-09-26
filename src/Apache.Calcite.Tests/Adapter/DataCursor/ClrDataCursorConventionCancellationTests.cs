using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;
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
    /// Cancellation through a cursor: the open's token reaches the leaf, and each advance's token reaches
    /// the advance.
    /// </summary>
    /// <remarks>
    /// The sequence convention's cancellation tests read a token off the <c>DataContext</c> and wrapped the
    /// root in it, because an <c>IAsyncEnumerable</c> takes a token once and the plan had nowhere else to
    /// put one. A cursor takes a token at the open and at every advance, so nothing here goes through the
    /// context: the token is an argument.
    /// </remarks>
    public class ClrDataCursorConventionCancellationTests
    {

        static ClrDataCursorConventionCancellationTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        static object[][] Many(int count)
        {
            var rows = new object[count][];
            for (int i = 0; i < count; i++)
                rows[i] = [java.lang.Integer.valueOf(i), "R" + (i % 4)];

            return rows;
        }

        static object[][] ManyAnys(int count)
        {
            var rows = new object[count][];
            for (int i = 0; i < count; i++)
                rows[i] = [
                    java.lang.Integer.valueOf(i),
                    "R" + (i % 4),
                    i % 4 == 0 ? java.lang.Double.valueOf(i + 0.5) : java.lang.Integer.valueOf(i),
                    "S" + (i % 7)];

            return rows;
        }

        static (ClrDataCursorFactory Factory, DataContext Context, AsyncRowsTable Leaf) PlanAny(string sql, int rowCount)
        {
            return Plan(sql, "ANYS", new AsyncRowsTable(ManyAnys(rowCount), AsyncTestRows.AnysRowType, false));
        }

        static (ClrDataCursorFactory Factory, DataContext Context, AsyncRowsTable Leaf) Plan(string sql, int rowCount)
        {
            return Plan(sql, "SORTED", new AsyncRowsTable(Many(rowCount), AsyncTestRows.SortedRowType, false));
        }

        static (ClrDataCursorFactory Factory, DataContext Context, AsyncRowsTable Leaf) Plan(string sql, string name, AsyncRowsTable leaf)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add(name, leaf);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrDataCursorRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrDataCursorRules.CalcRules())
                calcRules.add(rule);
            foreach (var rule in ClrEnumerableRules.CalcRules())
                if (calcRules.contains(rule) == false)
                    calcRules.add(rule);
            rules.add(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRules.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(rules, false, false, false, null, null),
                    Programs.hep(calcRules, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrDataCursorConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var factory = new ClrDataCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot((ClrDataCursorRel)physical, ClrEnumerablePrefer.Array);

            return (factory, new PlanDataContext(rootSchema, parameters), leaf);
        }

        sealed class PlanDataContext(org.apache.calcite.schema.SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            public org.apache.calcite.schema.SchemaPlus getRootSchema() => rootSchema;

            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// Opens with the token and reads every row under it, cancelling at the row the caller names.
        /// </summary>
        static async Task<(bool Cancelled, int Read)> ReadUntilCancelled(ClrDataCursorFactory factory, DataContext context, CancellationTokenSource cancellation, int cancelAt)
        {
            var read = 0;

            try
            {
                await using var cursor = await factory.OpenAsync(context, cancellation.Token);
                while (await cursor.ReadAsync(cancellation.Token))
                    if (++read == cancelAt)
                        cancellation.Cancel();
            }
            catch (OperationCanceledException)
            {
                return (true, read);
            }

            return (false, read);
        }

        [Fact]
        public async Task ShouldCancelAStreamingPlan()
        {
            var (factory, context, leaf) = Plan("SELECT K, V FROM SORTED WHERE K >= 0", 10_000);

            using var cancellation = new CancellationTokenSource();
            var (cancelled, _) = await ReadUntilCancelled(factory, context, cancellation, 5);

            cancelled.Should().BeTrue();
            leaf.SawCancellableToken.Should().BeTrue("the leaf must receive the caller's token, not the default one");
            leaf.Produced.Should().BeLessThan(100, "the leaf must stop producing once the caller has cancelled, not run to 10,000");
        }

        [Fact]
        public async Task ShouldCancelWhileABlockingOperatorIsStillReading()
        {
            var (factory, context, leaf) = Plan("SELECT K, V FROM SORTED ORDER BY V, K", 10_000);

            using var cancellation = new CancellationTokenSource();

            // cancelled at a known point in the input rather than after a delay, so that the sort is
            // certainly still reading when it happens -- and the sort drains inside the open, so it is
            // the open that is cancelled
            leaf.OnRow = n => { if (n == 50) cancellation.Cancel(); };

            var (cancelled, _) = await ReadUntilCancelled(factory, context, cancellation, int.MaxValue);

            cancelled.Should().BeTrue("a sort that ignored the token would read all 10,000 rows and then succeed");
            leaf.Produced.Should().BeLessThan(10_000);
        }

        [Fact]
        public async Task ShouldCancelWhileAnAggregateIsStillFolding()
        {
            var (factory, context, leaf) = Plan("SELECT COUNT(*) FROM SORTED", 10_000);

            using var cancellation = new CancellationTokenSource();

            leaf.OnRow = n => { if (n == 50) cancellation.Cancel(); };

            var (cancelled, _) = await ReadUntilCancelled(factory, context, cancellation, int.MaxValue);

            cancelled.Should().BeTrue();
            leaf.Produced.Should().BeLessThan(10_000);
        }

        [Fact]
        public async Task ShouldCancelWhileAnAggregateOverAnAnyColumnIsStillFolding()
        {
            var (factory, context, leaf) = PlanAny("SELECT K, MIN(V), MAX(V), SUM(V), AVG(V), MIN(S) FROM ANYS GROUP BY K", 10_000);

            using var cancellation = new CancellationTokenSource();

            leaf.OnRow = n => { if (n == 50) cancellation.Cancel(); };

            var (cancelled, _) = await ReadUntilCancelled(factory, context, cancellation, int.MaxValue);

            cancelled.Should().BeTrue();
            leaf.SawCancellableToken.Should().BeTrue("the leaf must receive the caller's token through the aggregate");
            leaf.Produced.Should().BeLessThan(10_000);
        }

        [Fact]
        public async Task ShouldFoldAnAnyColumnWithoutBlocking()
        {
            var (factory, context, leaf) = PlanAny("SELECT MIN(V), MAX(V), SUM(V) FROM ANYS", 5_000);

            // the aggregate folds inside its awaiting open, as Calcite's folds once at bind: the open is
            // what suspends, and the one row is in hand by the time the cursor is
            var opening = factory.OpenAsync(context, CancellationToken.None);
            opening.IsCompleted.Should().BeFalse("a fold over a leaf that suspends per row cannot finish synchronously");

            await using var cursor = await opening;
            leaf.Produced.Should().Be(5_000, "the whole input should have been folded by the time the cursor is handed back");

            var reading = cursor.ReadAsync(CancellationToken.None);
            reading.IsCompleted.Should().BeTrue("the row was folded at the open and the read has nothing to wait for");
            (await reading).Should().BeTrue();
        }

        [Fact]
        public async Task ShouldStopTheLeafWhenThePlanIsAbandoned()
        {
            var (factory, context, leaf) = Plan("SELECT K, V FROM SORTED WHERE K >= 0", 10_000);

            var read = 0;

            await using (var cursor = await factory.OpenAsync(context, CancellationToken.None))
                while (await cursor.ReadAsync(CancellationToken.None))
                    if (++read == 5)
                        break;

            var atBreak = leaf.Produced;

            // give anything still running a chance to show itself
            await Task.Delay(100);

            leaf.Produced.Should().Be(atBreak, "the leaf must not still be producing after the plan was abandoned");
            leaf.Produced.Should().BeLessThan(100);
        }

        /// <summary>
        /// The open's token is the leaf's for the whole read, so cancelling it stops an advance given no
        /// token of its own.
        /// </summary>
        [Fact]
        public async Task ShouldCancelFromTheOpensToken()
        {
            using var cancellation = new CancellationTokenSource();
            var (factory, context, leaf) = Plan("SELECT k, v FROM SORTED", 5000);

            var read = 0;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await using var cursor = await factory.OpenAsync(context, cancellation.Token);
                while (await cursor.ReadAsync(CancellationToken.None))
                    if (++read == 10)
                        cancellation.Cancel();
            });

            read.Should().Be(10, "the rows stopped where the cancellation was asked for");
            leaf.Produced.Should().BeLessThan(5000, "the leaf was not run to the end");
        }

        /// <summary>
        /// A cursor opened with no token is still cancelled by the token given to an advance.
        /// </summary>
        [Fact]
        public async Task ShouldCancelFromAnAdvancesToken()
        {
            var (factory, context, leaf) = Plan("SELECT k, v FROM SORTED WHERE k >= 0", 5000);

            await using var cursor = factory.Open(context);

            (await cursor.ReadAsync(CancellationToken.None)).Should().BeTrue();

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await cursor.ReadAsync(cancellation.Token));
            leaf.Produced.Should().BeLessThan(100);
        }

    }

}
