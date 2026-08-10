using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
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
    /// Runs a plan that holds nodes of both Clr calling conventions.
    /// </summary>
    /// <remarks>
    /// A row crosses untouched, and here that is not an argument about type factories agreeing: the two
    /// conventions share <c>ClrPhysType</c>, so a row of one already is a row of the other. Only the sequence
    /// around it changes, and the two directions do not cost the same —
    /// <c>ClrEnumerableToClrAsyncEnumerableConverter</c> never suspends, and
    /// <c>ClrAsyncEnumerableToClrEnumerableConverter</c> blocks a thread once per row.
    ///
    /// <para><b>The schema is what forces a crossing.</b> <c>SALES</c> is an
    /// <c>IClrAsyncScannableTable</c> and nothing else, so only the asynchronous convention has a scan rule
    /// that matches it; <c>SORTED</c> is a Calcite <c>ScannableTable</c>, which only the synchronous one
    /// reads. A query naming either against the wrong root convention has to cross, and a query naming both
    /// has to cross whichever root it is given.</para>
    ///
    /// <para>The <c>AcrossConverter</c> tests exist for the reason
    /// <c>ClrEnumerableMixedConventionTests.ShouldCarryACalcAcrossTheConverter</c> gives: with both rule sets
    /// registered the planner puts nearly everything on the root's side, so the converter only ever sees a
    /// bare scan. Registering one convention's rules plus the single converter rule is what makes the
    /// converter meet a real generated node.</para>
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
            var rules = new List<RelOptRule>();
            rules.AddRange(ClrEnumerableRules.Rules());
            rules.AddRange(ClrAsyncEnumerableRules.Rules());

            var calcRules = new List<RelOptRule>();
            calcRules.AddRange(ClrEnumerableRules.CalcRules());
            calcRules.AddRange(ClrAsyncEnumerableRules.CalcRules());

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
            var physical = Plan(sql, rootSchema, ClrAsyncEnumerableConvention.Instance, rules, calcRules);
            var parameters = new java.util.HashMap();
            var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);

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
        /// A convention is reached when the only route to it runs through a second converter.
        /// </summary>
        /// <remarks>
        /// The two bridges registered make <c>Enumerable -&gt; ClrAsyncEnumerable -&gt; ClrEnumerable</c> the
        /// only route to the root convention: the direct <c>Enumerable -&gt; ClrEnumerable</c> rule is left
        /// out, so the second hop would have to be applied to the converter the first produced, and
        /// <c>ConverterRule</c>'s operand refuses to stack a converter on a converter of the same trait def.
        /// What is left is <c>ConventionTraitDef</c>'s conversion graph, which a rule enters only by
        /// answering <c>isGuaranteed</c> — so without that this statement cannot be planned at all.
        /// </remarks>
        [TestMethod]
        public void ShouldBridgeThroughASecondConverter()
        {
            var rules = new List<RelOptRule>
            {
                ClrAsyncEnumerableRules.EnumerableToClrAsyncEnumerableConverterRule,
                ClrEnumerableRules.ClrAsyncEnumerableToClrEnumerableConverterRule,
            };

            var rows = RunSync("SELECT K, V FROM SORTED WHERE K >= 2", Schema(), rules, []);

            rows.Should().Equal(["2|B", "2|C", "4|D"]);
        }

        [TestMethod]
        public void ShouldReadAnAsynchronousPlanSynchronously()
        {
            var (rules, calcRules) = Both();
            var rows = RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema(), rules, calcRules);

            rows.Should().Equal(["4|D", "5|E", "6|F"]);
        }

        /// <summary>
        /// A plan asked for asynchronously over a table only the synchronous convention can scan still runs.
        /// </summary>
        [TestMethod]
        public async Task ShouldReadASynchronousPlanAsynchronously()
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
        public void ShouldReadAnAsynchronousPlanSynchronouslyUnderASynchronizationContext()
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
        /// A join whose two sides can only be scanned in different conventions runs, whichever convention the
        /// root is asked for.
        /// </summary>
        [TestMethod]
        public async Task ShouldJoinAcrossTheConventions()
        {
            var (rules, calcRules) = Both();

            const string sql = "SELECT S.LABEL, T.V FROM SALES S JOIN SORTED T ON S.ID = T.K";

            var sync = RunSync(sql, Schema(), rules, calcRules);
            var async = await RunAsync(sql, Schema(), rules, calcRules);

            sync.Should().BeEquivalentTo(["A|A", "B|B", "B|C", "D|D"]);
            async.Should().BeEquivalentTo(sync);
        }

        /// <summary>
        /// The blocking converter carries a generated node of the asynchronous convention, not merely a scan.
        /// </summary>
        /// <remarks>
        /// Only the asynchronous rules are registered, plus the one converter that produces a synchronous
        /// node, so the whole plan below the root is asynchronous and the calc under the converter is a
        /// <c>ClrAsyncEnumerableCalc</c>.
        /// </remarks>
        [TestMethod]
        public void ShouldCarryAnAsynchronousCalcAcrossTheConverter()
        {
            var rules = new List<RelOptRule>(ClrAsyncEnumerableRules.Rules()) { ClrEnumerableRules.ClrAsyncEnumerableToClrEnumerableConverterRule };
            var calcRules = new List<RelOptRule>(ClrAsyncEnumerableRules.CalcRules());

            var rows = RunSync("SELECT ID, LABEL FROM SALES WHERE ID > 3", Schema(), rules, calcRules);

            rows.Should().Equal(["4|D", "5|E", "6|F"]);
        }

        /// <summary>
        /// The non-suspending converter carries a generated node of the synchronous convention.
        /// </summary>
        [TestMethod]
        public async Task ShouldCarryASynchronousCalcAcrossTheConverter()
        {
            var rules = new List<RelOptRule>(ClrEnumerableRules.Rules()) { ClrAsyncEnumerableRules.ClrEnumerableToClrAsyncEnumerableConverterRule };
            var calcRules = new List<RelOptRule>(ClrEnumerableRules.CalcRules());

            var rows = await RunAsync("SELECT K, V FROM SORTED WHERE K >= 2", Schema(), rules, calcRules);

            rows.Should().Equal(["2|B", "2|C", "4|D"]);
        }

        /// <summary>
        /// A synchronous caller that stops reading disposes the asynchronous plan under the converter, and
        /// waits for the disposal rather than dropping it.
        /// </summary>
        /// <remarks>
        /// <c>AsyncRowsTable</c> sets <c>DisposedAsynchronously</c> after an <c>await</c> in its
        /// <c>finally</c>, so the flag is only set if the awaited part of the disposal ran to completion. A
        /// converter that called <c>DisposeAsync</c> and discarded the <c>ValueTask</c> would leave it false.
        /// </remarks>
        [TestMethod]
        public void ShouldDisposeTheAsynchronousPlanWhenTheReaderStops()
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
