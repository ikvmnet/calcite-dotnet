using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// What <c>ClrEnumerableDefaults.RepeatUnion</c> and its asynchronous twin do, against what
    /// <c>EnumerableDefaults.repeatUnion</c> does.
    /// </summary>
    /// <remarks>
    /// <c>ShouldAgreeOnARecursiveQueryWhoseStepAggregates</c> holds the same property against Calcite as the
    /// oracle, and it only reaches the synchronous convention -- the asynchronous differential suite is
    /// driven by SQL, and standard SQL will not put an aggregate in a recursive term. So the asymmetric
    /// round count is pinned here instead, on both operators, by scripting the iterative part directly:
    /// what a caller can see is how many times that sequence is enumerated, and the two must agree.
    /// </remarks>
    [TestClass]
    public class ClrRepeatUnionTests
    {

        /// <summary>
        /// An iterative part that yields a row on its first evaluation and nothing afterwards, counting how
        /// many times it was evaluated.
        /// </summary>
        sealed class Rounds
        {

            public int Evaluations { get; private set; }

            public IEnumerable<int> Sync()
            {
                var round = Evaluations++;
                if (round == 0)
                    yield return 100;
            }

            public async IAsyncEnumerable<int> Async()
            {
                var round = Evaluations++;
                if (round == 0)
                {
                    await Task.Yield();
                    yield return 100;
                }
            }

        }

        /// <summary>
        /// A round that produced rows puts the sentinel back, so the empty round after it does stop the
        /// sequence.
        /// </summary>
        /// <remarks>
        /// The contrast that places the extra round at the seed/iteration boundary and nowhere else. Round 0
        /// yields 100 and, being productive, ends with the sentinel restored; round 1 yields nothing and the
        /// sentinel is set, so it stops. Two evaluations, not three. The defect below is not "an empty round
        /// is always retried" -- it is that the boundary is the one place the sentinel is never restored.
        /// </remarks>
        [TestMethod]
        public void ShouldNotRunAnExtraRoundWhereTheFirstRoundProducedRows()
        {
            var rounds = new Rounds();
            var rows = ClrEnumerableDefaults.RepeatUnion(new[] { 1 }, rounds.Sync(), -1, true, null, null).ToList();

            rows.Should().Equal(1, 100);
            rounds.Evaluations.Should().Be(2);
        }

        /// <summary>
        /// The asynchronous operator counts the same rounds as the synchronous one.
        /// </summary>
        [TestMethod]
        public async Task ShouldNotRunAnExtraRoundWhereTheFirstRoundProducedRowsAsync()
        {
            var rounds = new Rounds();
            var rows = new List<int>();

            await foreach (var row in ClrAsyncEnumerableDefaults.RepeatUnion(AsyncOf(1), rounds.Async(), -1, true, null, null))
                rows.Add(row);

            rows.Should().Equal(1, 100);
            rounds.Evaluations.Should().Be(2);
        }

        /// <summary>
        /// A seed that emitted a row costs an extra evaluation of an iterative part that was never going to
        /// yield anything at all.
        /// </summary>
        /// <remarks>
        /// The cleanest statement of the defect, with the iterative part empty from the start so nothing else
        /// is in play. Round 0 is empty, but <c>current</c> holds the seed row, so it goes round again; round
        /// 1 is empty with the sentinel back, and that ends it. Two evaluations to discover that a sequence
        /// which never yields anything never yields anything.
        /// </remarks>
        [TestMethod]
        public void ShouldEvaluateAnEmptyIterativePartTwiceWhereTheSeedEmittedARow()
        {
            var evaluations = 0;

            IEnumerable<int> Empty()
            {
                evaluations++;
                yield break;
            }

            ClrEnumerableDefaults.RepeatUnion(new[] { 1 }, Empty(), -1, true, null, null).ToList().Should().Equal(1);
            evaluations.Should().Be(2);
        }

        /// <summary>
        /// A seed that emitted nothing leaves the sentinel set, so the first empty round does stop it.
        /// </summary>
        /// <remarks>
        /// The other side of the same test, and the reason the extra round is a property of the seed/iteration
        /// boundary rather than of the loop: with no seed row there is nothing to leave in <c>current</c>, and
        /// round 0 stops the sequence on its own. One evaluation rather than two.
        /// </remarks>
        [TestMethod]
        public void ShouldEvaluateAnEmptyIterativePartOnceWhereTheSeedWasEmpty()
        {
            var evaluations = 0;

            IEnumerable<int> Empty()
            {
                evaluations++;
                yield break;
            }

            ClrEnumerableDefaults.RepeatUnion(System.Array.Empty<int>(), Empty(), -1, true, null, null).ToList().Should().BeEmpty();
            evaluations.Should().Be(1);
        }

        /// <summary>
        /// A limit of zero never opens the iterative part at all.
        /// </summary>
        [TestMethod]
        public void ShouldNotOpenTheIterativePartWhereTheLimitIsZero()
        {
            var rounds = new Rounds();
            var rows = ClrEnumerableDefaults.RepeatUnion(new[] { 1 }, rounds.Sync(), 0, true, null, null).ToList();

            rows.Should().Equal(1);
            rounds.Evaluations.Should().Be(0);
        }

        /// <summary>
        /// The clean-up runs before the enumerators are disposed, which is the order of Calcite's
        /// <c>close()</c>.
        /// </summary>
        /// <remarks>
        /// The caller has to abandon the sequence for this to be observable at all. Run to the end and the
        /// seed's own <c>finally</c> has already fired -- a C# iterator runs it when its body falls off the
        /// end, during the very <c>MoveNext</c> that reports false, not when the enumerator is later disposed.
        /// So a drained seed logs itself before the clean-up no matter which order the two are written in, and
        /// a test that drains it is measuring nothing. Taking one row and stopping leaves the seed suspended
        /// at its <c>yield</c>, and then disposal is what runs its <c>finally</c>.
        /// </remarks>
        [TestMethod]
        public void ShouldRunTheCleanUpBeforeDisposingTheEnumerators()
        {
            var order = new List<string>();

            IEnumerable<int> Seed()
            {
                try
                {
                    yield return 1;
                    yield return 2;
                }
                finally
                {
                    order.Add("seed disposed");
                }
            }

            using (var enumerator = ClrEnumerableDefaults.RepeatUnion(Seed(), System.Array.Empty<int>(), -1, true, null, () => order.Add("clean up")).GetEnumerator())
                enumerator.MoveNext().Should().BeTrue();

            order.Should().Equal("clean up", "seed disposed");
        }

        static async IAsyncEnumerable<int> AsyncOf(params int[] values)
        {
            foreach (var value in values)
            {
                await Task.Yield();
                yield return value;
            }
        }

    }

}
