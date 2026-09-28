using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;
using Apache.Calcite.Tests;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests reading plans over both kinds of leaf: an awaiting leaf opened and read synchronously, a
    /// synchronous leaf opened and read with await, and the two joined.
    /// </summary>
    /// <remarks>
    /// A cursor has both a synchronous and an awaiting advance, so these check that the same rows arrive
    /// whichever way the plan is opened and read, and that a synchronous read over an awaiting leaf does not
    /// deadlock under a synchronization context that never runs posted callbacks.
    /// </remarks>
    public class ClrCursorRelImplementorCrossingTests
    {

        static ClrCursorRelImplementorCrossingTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        static SchemaPlus Schema(AsyncRowsTable? sales = null)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", sales ?? new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));

            return rootSchema;
        }

        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            public SchemaPlus getRootSchema() => rootSchema;

            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            public object get(string name) => parameters.get(name);

        }

        static RelNode Plan(string sql, SchemaPlus rootSchema)
        {
            var ruleList = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.Rules())
                ruleList.add(rule);

            var calcRuleList = new java.util.ArrayList();
            foreach (var rule in ClrCursorRules.CalcRules())
                calcRuleList.add(rule);
            foreach (var rule in RelOptRules.CALC_RULES.toArray())
                calcRuleList.add(rule);

            var config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .programs(
                    Programs.subQuery(org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE),
                    new DefaultRulesProgram(ruleList, false, false, false, null, null),
                    Programs.hep(calcRuleList, true, org.apache.calcite.rel.metadata.DefaultRelMetadataProvider.INSTANCE))
                .build();

            var planner = Frameworks.getPlanner(config);
            var logical = planner.rel(planner.validate(planner.parse(sql))).project();
            var expanded = planner.transform(0, logical.getTraitSet(), logical);
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrCursorConvention.Instance).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        static (ClrCursorFactory Factory, DataContext Context) Implement(string sql, SchemaPlus rootSchema)
        {
            var physical = Plan(sql, rootSchema);
            var parameters = new java.util.HashMap();
            var factory = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters).ImplementRoot((ClrCursorRel)physical, ClrCursorPrefer.Array);

            return (factory, new TestDataContext(rootSchema, parameters));
        }

        static List<string> RunSync(string sql, SchemaPlus rootSchema)
        {
            var (factory, context) = Implement(sql, rootSchema);

            var rows = new List<string>();
            using var cursor = factory.Open(context);
            while (cursor.Read())
                rows.Add(Render(cursor.Current));

            return rows;
        }

        static async Task<List<string>> RunAsync(string sql, SchemaPlus rootSchema)
        {
            var (factory, context) = Implement(sql, rootSchema);

            var rows = new List<string>();
            await using var cursor = await factory.OpenAsync(context, CancellationToken.None);
            while (await cursor.ReadAsync(CancellationToken.None))
                rows.Add(Render(cursor.Current));

            return rows;
        }

        static string Render(object? row)
        {
            return row is object?[] array
                ? string.Join("|", array.Select(v => v?.ToString() ?? "NULL"))
                : row?.ToString() ?? "NULL";
        }

        [Fact]
        public void ShouldReadAnAsynchronousLeafSynchronously()
        {
            var rows = RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema());

            rows.Should().Equal(["4|D", "5|E", "6|F"]);
        }

        [Fact]
        public async Task ShouldReadASynchronousLeafAsynchronously()
        {
            var rows = await RunAsync("SELECT K, V FROM SORTED WHERE K >= 2", Schema());

            rows.Should().Equal(["2|B", "2|C", "4|D"]);
        }

        [Fact]
        public void ShouldReadAnAsynchronousLeafSynchronouslyUnderASynchronizationContext()
        {
            List<string>? rows = null;
            Exception? error = null;

            var thread = new Thread(() =>
            {
                try
                {
                    SynchronizationContext.SetSynchronizationContext(new NonPumpingContext());
                    rows = RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema());
                }
                catch (Exception e)
                {
                    error = e;
                }
            });

            thread.IsBackground = true;
            thread.Start();
            thread.Join(TimeSpan.FromSeconds(30)).Should().BeTrue(
                "a deadlock here means a continuation was captured by the context before it was suppressed");

            error.Should().BeNull();
            rows.Should().Equal(["4|D", "5|E", "6|F"]);
        }

        /// <summary>
        /// A synchronization context that discards every posted callback, so a continuation captured by it
        /// never runs.
        /// </summary>
        sealed class NonPumpingContext : SynchronizationContext
        {

            public override void Post(SendOrPostCallback d, object? state)
            {

            }

            public override SynchronizationContext CreateCopy() => this;

        }

        [Fact]
        public async Task ShouldJoinAcrossTheTwoKindsOfLeaf()
        {
            const string sql = "SELECT S.LABEL, T.V FROM SALES S JOIN SORTED T ON S.ID = T.K";

            var sync = RunSync(sql, Schema());
            var async = await RunAsync(sql, Schema());

            sync.Should().BeEquivalentTo(["A|A", "B|B", "B|C", "D|D"]);
            async.Should().BeEquivalentTo(sync);
        }

        [Fact]
        public async Task ShouldCarryAGeneratedCalcOverTheCrossing()
        {
            RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema())
                .Should().Equal(["4|D", "5|E", "6|F"]);

            (await RunAsync("SELECT K, V FROM SORTED WHERE K >= 2", Schema()))
                .Should().Equal(["2|B", "2|C", "4|D"]);
        }

        [Fact]
        public void ShouldDisposeTheAsynchronousLeafWhenTheReaderStops()
        {
            var sales = new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false);
            var (factory, context) = Implement("SELECT ID, LABEL FROM SALES", Schema(sales));

            using (var cursor = factory.Open(context))
            {
                cursor.Read().Should().BeTrue();
                Render(cursor.Current).Should().Be("1|A");
            }

            sales.Produced.Should().BeLessThan(AsyncTestRows.Sales.Length);
            sales.DisposedAsynchronously.Should().BeTrue("a synchronous disposal still reaches the awaited part of the leaf's, by blocking for it");
        }

        /// <summary>
        /// <c>Awaited</c> over the synchronous body and <c>Pulled</c> over the awaiting body each produce an
        /// open that returns the same rows as the body it wraps.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldCrossAResultEitherWay()
        {
            var rootSchema = Schema();
            var physical = (ClrCursorRel)Plan("SELECT ID, LABEL FROM SALES WHERE ID > 3", rootSchema);
            var parameters = new java.util.HashMap();
            var context = new TestDataContext(rootSchema, parameters);
            var implementor = new ClrCursorRelImplementor(physical.getCluster().getRexBuilder(), parameters);

            var awaited = implementor.Awaited(implementor.VisitChild(null, 0, physical, ClrCursorPrefer.Array));
            var openAwaited = System.Linq.Expressions.Expression.Lambda<Func<DataContext, CancellationToken, ValueTask<IClrCursor<object[]>>>>(
                awaited.Expression, implementor.Root, implementor.CancellationToken).Compile();

            var rows = new List<string>();
            await using (var cursor = await openAwaited(context, CancellationToken.None))
                while (await cursor.ReadAsync(CancellationToken.None))
                    rows.Add(Render(cursor.Current));

            rows.Should().Equal(["4|D", "5|E", "6|F"]);

            var pulled = implementor.Pulled(implementor.VisitChildAsync(null, 0, physical, ClrCursorPrefer.Array));
            var openPulled = System.Linq.Expressions.Expression.Lambda<Func<DataContext, IClrCursor<object[]>>>(pulled.Expression, implementor.Root).Compile();

            rows.Clear();
            using (var cursor = openPulled(context))
                while (cursor.Read())
                    rows.Add(Render(cursor.Current));

            rows.Should().Equal(["4|D", "5|E", "6|F"]);
        }

    }

}
