using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

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
    /// Requires that a cancellation reaches the leaf of a real compiled plan, and that abandoning one
    /// disposes it.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrAsyncEnumerableCancellationTests"/> establishes the same thing over three operators
    /// composed by hand, which is what had to be true before any of the convention was written. This asks it
    /// of a plan the planner chose and the implementor compiled, over a table that really suspends — the
    /// difference being that a node can drop a token in a way a hand-built tree cannot, by composing an
    /// operator that never received one.
    ///
    /// <para>It matters most for the operators that read their whole input before yielding. A sort, an
    /// aggregate or a hash join that ignored cancellation would run the leaf to exhaustion after the caller
    /// had given up, and every differential test would still pass — they compare rows, and a plan that
    /// finishes has the right rows.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerablePlanCancellationTests
    {

        static ClrAsyncEnumerablePlanCancellationTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
        }

        /// <summary>
        /// A table of many rows, so that a cancellation part way through is distinguishable from the
        /// sequence simply ending.
        /// </summary>
        static object[][] Many(int count)
        {
            var rows = new object[count][];
            for (int i = 0; i < count; i++)
                rows[i] = [java.lang.Integer.valueOf(i), "R" + (i % 4)];

            return rows;
        }

        static (IAsyncEnumerable<object> Rows, AsyncRowsTable Leaf) Plan(string sql, int rowCount)
        {
            var leaf = new AsyncRowsTable(Many(rowCount), AsyncTestRows.SortedRowType, false);

            var rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("SORTED", leaf);

            var rules = new java.util.ArrayList();
            var calcRules = new java.util.ArrayList();
            foreach (var rule in ClrAsyncEnumerableRules.Rules())
                rules.add(rule);
            foreach (var rule in ClrAsyncEnumerableRules.CalcRules())
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
            var chosen = planner.transform(1, expanded.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance).simplify(), expanded);
            var physical = planner.transform(2, chosen.getTraitSet(), chosen);

            var parameters = new java.util.HashMap();
            var bindable = ClrAsyncEnumerableInterpretable.ToBindable(parameters, (ClrAsyncEnumerableRel)physical, ClrEnumerablePrefer.Array);

            return (bindable.Bind(new PlanDataContext(rootSchema, parameters)), leaf);
        }

        sealed class PlanDataContext(org.apache.calcite.schema.SchemaPlus rootSchema, java.util.Map parameters) : DataContext
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
        /// A streaming plan stops reading its leaf when the caller cancels.
        /// </summary>
        [TestMethod]
        public async Task ShouldCancelAStreamingPlan()
        {
            var (rows, leaf) = Plan("SELECT K, V FROM SORTED WHERE K >= 0", 10_000);

            using var cancellation = new CancellationTokenSource();
            var read = 0;
            var cancelled = false;

            try
            {
                await foreach (var row in rows.WithCancellation(cancellation.Token))
                    if (++read == 5)
                        cancellation.Cancel();
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue();
            leaf.SawCancellableToken.Should().BeTrue("the leaf must receive the caller's token, not the default one");
            leaf.Produced.Should().BeLessThan(100, "the leaf must stop producing once the caller has cancelled, not run to 10,000");
        }

        /// <summary>
        /// A plan whose root buffers its whole input still stops reading the leaf when the caller cancels.
        /// </summary>
        /// <remarks>
        /// The case a differential test cannot see. A sort reads everything before it yields anything, so a
        /// sort that ignored the token would produce identical rows and simply take as long as the input is
        /// large — which here is 10,000 rows that each suspend.
        /// </remarks>
        [TestMethod]
        public async Task ShouldCancelWhileABlockingOperatorIsStillReading()
        {
            var (rows, leaf) = Plan("SELECT K, V FROM SORTED ORDER BY V, K", 10_000);

            using var cancellation = new CancellationTokenSource();

            // cancelled at a known point in the input rather than after a delay, so that the sort is
            // certainly still reading when it happens
            leaf.OnRow = n => { if (n == 50) cancellation.Cancel(); };

            var cancelled = false;

            try
            {
                await foreach (var row in rows.WithCancellation(cancellation.Token))
                {
                }
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue("a sort that ignored the token would read all 10,000 rows and then succeed");
            leaf.Produced.Should().BeLessThan(10_000);
        }

        /// <summary>
        /// The same, for an aggregate, which folds rather than buffers.
        /// </summary>
        [TestMethod]
        public async Task ShouldCancelWhileAnAggregateIsStillFolding()
        {
            var (rows, leaf) = Plan("SELECT COUNT(*) FROM SORTED", 10_000);

            using var cancellation = new CancellationTokenSource();

            leaf.OnRow = n => { if (n == 50) cancellation.Cancel(); };

            var cancelled = false;

            try
            {
                await foreach (var row in rows.WithCancellation(cancellation.Token))
                {
                }
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue();
            leaf.Produced.Should().BeLessThan(10_000);
        }

        /// <summary>
        /// Abandoning a plan part way through stops the leaf, without any cancellation at all.
        /// </summary>
        /// <remarks>
        /// This is disposal rather than cancellation: <c>break</c> out of an <c>await foreach</c> disposes
        /// the enumerator, and each operator has to dispose the one beneath it. A hand-written async iterator
        /// that swallowed that would leave the leaf running, which is the classic leak in this kind of code
        /// and the one no differential test would ever show.
        /// </remarks>
        [TestMethod]
        public async Task ShouldStopTheLeafWhenThePlanIsAbandoned()
        {
            var (rows, leaf) = Plan("SELECT K, V FROM SORTED WHERE K >= 0", 10_000);

            var read = 0;

            await foreach (var row in rows)
                if (++read == 5)
                    break;

            var atBreak = leaf.Produced;

            // give anything still running a chance to show itself
            await Task.Delay(100);

            leaf.Produced.Should().Be(atBreak, "the leaf must not still be producing after the plan was abandoned");
            leaf.Produced.Should().BeLessThan(100);
        }

    }

}
