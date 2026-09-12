using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.schema;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs one plan whose leaf yields the kind of sequence the plan is not being built from.
    /// </summary>
    /// <remarks>
    /// There is one convention and one plan; what differs is the implementor, and a leaf that can only build
    /// one kind of sequence is read across by <c>ClrEnumerableRelImplementor</c>. The rows cross untouched,
    /// and that is not an argument about type factories agreeing: the physical type is the same object either
    /// way, so a row already is a row. Only the sequence around it changes, and the two directions do not
    /// cost the same — reading a synchronous leaf asynchronously never suspends, and reading an asynchronous
    /// one synchronously blocks a thread once per row.
    ///
    /// <para><b>The schema is what forces a crossing.</b> <c>SALES</c> is an <c>IClrAsyncScannableTable</c>
    /// and nothing else, so its rows arrive awaited; <c>SORTED</c> is a Calcite <c>ScannableTable</c>, read
    /// through linq4j and pulled. A query over either, implemented the other way, has to cross, and a query
    /// naming both crosses whichever way it is implemented.</para>
    ///
    /// <para>These used to be tests about two conventions and the four converters between them. Three of
    /// those converters are gone: the crossing is no longer a node the planner chooses and costs, but a call
    /// the implementor adds where a node hands up the sequence it can build. What is asserted is the same
    /// behaviour, which is the point of keeping them.</para>
    /// </remarks>
    [TestClass]
    public class ClrConventionCrossingTests
    {

        static ClrConventionCrossingTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A schema whose two tables can each be scanned in one convention only.
        /// </summary>
        static SchemaPlus Schema(AsyncRowsTable? sales = null)
        {
            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SALES", sales ?? new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false));
            rootSchema.add("SORTED", new SyncRowsTable(AsyncTestRows.Sorted, AsyncTestRows.SortedRowType, true));

            return rootSchema;
        }

        /// <summary>
        /// The context a plan is bound with.
        /// </summary>
        sealed class TestDataContext(SchemaPlus rootSchema, java.util.Map parameters) : DataContext
        {

            /// <inheritdoc />
            public SchemaPlus getRootSchema() => rootSchema;

            /// <inheritdoc />
            public org.apache.calcite.adapter.java.JavaTypeFactory getTypeFactory() => new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

            /// <inheritdoc />
            public org.apache.calcite.linq4j.QueryProvider getQueryProvider() => null!;

            /// <inheritdoc />
            public object get(string name) => parameters.get(name);

        }

        /// <summary>
        /// Plans a statement to the given root convention with the given rules.
        /// </summary>
        static RelNode Plan(string sql, SchemaPlus rootSchema, Convention root, IReadOnlyList<RelOptRule> rules, IReadOnlyList<RelOptRule> calcRules)
        {
            var ruleList = new java.util.ArrayList();
            foreach (var rule in rules)
                ruleList.add(rule);

            var calcRuleList = new java.util.ArrayList();
            foreach (var rule in calcRules)
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
            var chosen = planner.transform(1, expanded.getTraitSet().replace(root).simplify(), expanded);

            return planner.transform(2, chosen.getTraitSet(), chosen);
        }

        /// <summary>
        /// Both rule sets, so the planner may put a node in either convention and bridge between them.
        /// </summary>
        static (IReadOnlyList<RelOptRule> Rules, IReadOnlyList<RelOptRule> CalcRules) Both()
        {
            var rules = new List<RelOptRule>(ClrEnumerableRules.Rules());
            var calcRules = new List<RelOptRule>(ClrEnumerableRules.CalcRules());

            return (rules, calcRules);
        }

        /// <summary>
        /// Runs a plan rooted in the synchronous convention, blocking as its caller would.
        /// </summary>
        static List<string> RunSync(string sql, SchemaPlus rootSchema, IReadOnlyList<RelOptRule> rules, IReadOnlyList<RelOptRule> calcRules)
        {
            var physical = Plan(sql, rootSchema, ClrEnumerableConvention.Instance, rules, calcRules);
            var parameters = new java.util.HashMap();
            var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            var rows = new List<string>();
            foreach (var row in bindable.Bind(new TestDataContext(rootSchema, parameters)))
                rows.Add(Render(row));

            return rows;
        }

        /// <summary>
        /// Runs a plan rooted in the asynchronous convention.
        /// </summary>
        static async Task<List<string>> RunAsync(string sql, SchemaPlus rootSchema, IReadOnlyList<RelOptRule> rules, IReadOnlyList<RelOptRule> calcRules)
        {
            var physical = Plan(sql, rootSchema, ClrEnumerableConvention.Instance, rules, calcRules);
            var parameters = new java.util.HashMap();
            var bindable = ClrEnumerableInterpretable.ToAsyncBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            var rows = new List<string>();
            await foreach (var row in bindable.Bind(new TestDataContext(rootSchema, parameters)))
                rows.Add(Render(row));

            return rows;
        }

        static string Render(object? row)
        {
            return row is object?[] array
                ? string.Join("|", array.Select(v => v?.ToString() ?? "NULL"))
                : row?.ToString() ?? "NULL";
        }

        /// <summary>
        /// A plan asked for synchronously over a table only the asynchronous convention can scan still runs,
        /// and returns every row in order.
        /// </summary>
        /// <remarks>
        /// <c>AsyncRowsTable</c> awaits on every row, so each <c>MoveNextAsync</c> here really is incomplete
        /// when it is blocked on. A fixture that completed synchronously would exercise the fast path only
        /// and say nothing about the wait.
        /// </remarks>
        /// <summary>
        /// A table whose rows are only produced asynchronously is read by a plan implemented synchronously.
        /// </summary>
        /// <remarks>
        /// <c>AsyncRowsTable</c> awaits on every row, so each <c>MoveNextAsync</c> here really is incomplete
        /// when it is blocked on. A fixture that completed synchronously would exercise the fast path only
        /// and say nothing about the wait.
        /// </remarks>
        [TestMethod]
        public void ShouldReadAnAsynchronousLeafSynchronously()
        {
            var (rules, calcRules) = Both();
            var rows = RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema(), rules, calcRules);

            rows.Should().Equal(["4|D", "5|E", "6|F"]);
        }

        /// <summary>
        /// A table whose rows are pulled is read by a plan implemented asynchronously.
        /// </summary>
        [TestMethod]
        public async Task ShouldReadASynchronousLeafAsynchronously()
        {
            var (rules, calcRules) = Both();
            var rows = await RunAsync("SELECT K, V FROM SORTED WHERE K >= 2", Schema(), rules, calcRules);

            rows.Should().Equal(["2|B", "2|C", "4|D"]);
        }

        /// <summary>
        /// The blocking crossing completes on a thread carrying a synchronization context.
        /// </summary>
        /// <remarks>
        /// The regression test for <c>ClrSequences</c>' suppression placement. The operators await without
        /// <c>ConfigureAwait(false)</c>, and a continuation captures the context at the moment of
        /// suspension — inside <c>MoveNextAsync</c>'s synchronous phase, before the converter has anything
        /// to wait on. Measured with exactly this shape of context, one that queues and cannot pump while
        /// its thread is blocked: nulling the context only around the wait deadlocked, nulling it before
        /// each call completes. <c>AsyncRowsTable</c> awaits on every row, so every capture here would be
        /// real.
        ///
        /// <para>The read runs on a dedicated background thread with a join timeout, so a regression fails
        /// rather than hanging the suite.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldReadAnAsynchronousLeafSynchronouslyUnderASynchronizationContext()
        {
            var (rules, calcRules) = Both();

            List<string>? rows = null;
            Exception? error = null;

            var thread = new Thread(() =>
            {
                try
                {
                    SynchronizationContext.SetSynchronizationContext(new NonPumpingContext());
                    rows = RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema(), rules, calcRules);
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
        /// A context that accepts posts and can never run them while its thread is blocked — the observable
        /// shape of a blocked UI thread.
        /// </summary>
        sealed class NonPumpingContext : SynchronizationContext
        {

            /// <summary>
            /// Queued to a loop nobody pumps: a continuation posted here never runs, exactly as one posted
            /// to a blocked UI thread's message loop never runs.
            /// </summary>
            public override void Post(SendOrPostCallback d, object? state)
            {

            }

            /// <inheritdoc />
            public override SynchronizationContext CreateCopy() => this;

        }

        /// <summary>
        /// A join whose two sides produce different kinds of sequence runs, implemented either way.
        /// </summary>
        [TestMethod]
        public async Task ShouldJoinAcrossTheTwoKindsOfLeaf()
        {
            var (rules, calcRules) = Both();

            const string sql = "SELECT S.LABEL, T.V FROM SALES S JOIN SORTED T ON S.ID = T.K";

            var sync = RunSync(sql, Schema(), rules, calcRules);
            var async = await RunAsync(sql, Schema(), rules, calcRules);

            sync.Should().BeEquivalentTo(["A|A", "B|B", "B|C", "D|D"]);
            async.Should().BeEquivalentTo(sync);
        }

        /// <summary>
        /// A generated calc sits above the crossing, in both directions, rather than the crossing being all
        /// there is.
        /// </summary>
        /// <remarks>
        /// The crossing is at the leaf and the calc above it is implemented in the mode being built, so a
        /// real generated node reads a bridged sequence. Reaching that case used to need the rules
        /// registered by hand, because with both conventions loaded the planner put everything on the root's
        /// side and the converter only ever saw a bare scan.
        /// </remarks>
        [TestMethod]
        public async Task ShouldCarryAGeneratedCalcOverTheCrossing()
        {
            var (rules, calcRules) = Both();

            RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema(), rules, calcRules)
                .Should().Equal(["4|D", "5|E", "6|F"]);

            (await RunAsync("SELECT K, V FROM SORTED WHERE K >= 2", Schema(), rules, calcRules))
                .Should().Equal(["2|B", "2|C", "4|D"]);
        }

        /// <summary>
        /// A synchronous caller that stops reading disposes the asynchronous leaf under it, and waits for
        /// the disposal rather than dropping it.
        /// </summary>
        /// <remarks>
        /// <c>AsyncRowsTable</c> sets <c>DisposedAsynchronously</c> after an <c>await</c> in its
        /// <c>finally</c>, so the flag is only set if the awaited part of the disposal ran to completion. A
        /// converter that called <c>DisposeAsync</c> and discarded the <c>ValueTask</c> would leave it false.
        /// </remarks>
        [TestMethod]
        public void ShouldDisposeTheAsynchronousLeafWhenTheReaderStops()
        {
            var (rules, calcRules) = Both();
            var sales = new AsyncRowsTable(AsyncTestRows.Sales, AsyncTestRows.SalesRowType, false);
            var rootSchema = Schema(sales);

            var physical = Plan("SELECT ID, LABEL FROM SALES", rootSchema, ClrEnumerableConvention.Instance, rules, calcRules);
            var parameters = new java.util.HashMap();
            var bindable = ClrEnumerableInterpretable.ToBindable(parameters, (ClrEnumerableRel)physical, ClrEnumerablePrefer.Array);

            using (var enumerator = bindable.Bind(new TestDataContext(rootSchema, parameters)).GetEnumerator())
            {
                enumerator.MoveNext().Should().BeTrue();
                Render(enumerator.Current).Should().Be("1|A");
            }

            sales.Produced.Should().BeLessThan(AsyncTestRows.Sales.Length);
            sales.DisposedAsynchronously.Should().BeTrue();
        }

    }

}
