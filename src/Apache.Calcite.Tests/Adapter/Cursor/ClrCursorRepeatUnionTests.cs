using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// What <c>ClrCursorDefaults.RepeatUnion</c> and its awaiting twin do, against what
    /// <c>EnumerableDefaults.repeatUnion</c> does.
    /// </summary>
    /// <remarks>
    /// <c>ShouldAgreeOnARecursiveQueryWhoseStepAggregates</c> holds the same property against Calcite as the
    /// oracle, through SQL a caller cannot write -- standard SQL will not put an aggregate in a recursive
    /// term. So the asymmetric round count is pinned here too, on both opens and through both advances, by
    /// scripting the iterative part directly: what a caller can see is how many times that part is opened,
    /// and every way of reading the cursor must agree.
    /// </remarks>
    public class ClrCursorRepeatUnionTests
    {

        /// <summary>
        /// A cursor over rows in hand that records its disposal.
        /// </summary>
        sealed class RowsCursor(IReadOnlyList<int> rows, Action? onDispose = null) : ClrCursor<int>
        {

            int index = -1;

            public override int Current => rows[index];

            public override bool Read()
            {
                if (index + 1 >= rows.Count)
                    return false;

                index++;
                return true;
            }

            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                return new ValueTask<bool>(Read());
            }

            public override void Dispose()
            {
                onDispose?.Invoke();
            }

        }

        /// <summary>
        /// An iterative part that yields a row on its first opening and nothing afterwards, counting how
        /// many times it was opened.
        /// </summary>
        sealed class Rounds
        {

            public int Evaluations { get; private set; }

            public IClrCursor<int> Open()
            {
                var round = Evaluations++;
                return new RowsCursor(round == 0 ? [100] : []);
            }

            public async ValueTask<IClrCursor<int>> OpenAsync(CancellationToken cancellationToken)
            {
                await Task.Yield();
                return Open();
            }

        }

        /// <summary>
        /// An iterative part that never yields anything, counting how many times it was opened.
        /// </summary>
        sealed class Empty
        {

            public int Evaluations { get; private set; }

            public IClrCursor<int> Open()
            {
                Evaluations++;
                return new RowsCursor([]);
            }

            public ValueTask<IClrCursor<int>> OpenAsync(CancellationToken cancellationToken)
            {
                return new ValueTask<IClrCursor<int>>(Open());
            }

        }

        static List<int> ReadAll(IClrCursor<int> cursor)
        {
            var rows = new List<int>();
            while (cursor.Read())
                rows.Add(cursor.Current);

            return rows;
        }

        static async Task<List<int>> ReadAllAsync(IClrCursor<int> cursor)
        {
            var rows = new List<int>();
            while (await cursor.ReadAsync(CancellationToken.None))
                rows.Add(cursor.Current);

            return rows;
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
        [Fact]
        public void ShouldNotRunAnExtraRoundWhereTheFirstRoundProducedRows()
        {
            var rounds = new Rounds();
            var rows = ReadAll(ClrCursorDefaults.RepeatUnion(new RowsCursor([1]), rounds.Open, rounds.OpenAsync, -1, true, null, null));

            rows.Should().Equal(1, 100);
            rounds.Evaluations.Should().Be(2);
        }

        /// <summary>
        /// The awaiting open counts the same rounds as the synchronous one, and an awaiting advance opens
        /// a round with the awaiting open.
        /// </summary>
        [Fact]
        public async Task ShouldNotRunAnExtraRoundWhereTheFirstRoundProducedRowsAsync()
        {
            var rounds = new Rounds();
            var openedAsync = 0;
            var cursor = await ClrCursorDefaults.RepeatUnionAsync(
                new ValueTask<IClrCursor<int>>(new RowsCursor([1])),
                rounds.Open,
                token => { openedAsync++; return rounds.OpenAsync(token); },
                -1, true, null, null, CancellationToken.None);

            var rows = await ReadAllAsync(cursor);

            rows.Should().Equal(1, 100);
            rounds.Evaluations.Should().Be(2);
            openedAsync.Should().Be(2, "every round was started by an awaiting advance");
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
        [Fact]
        public void ShouldEvaluateAnEmptyIterativePartTwiceWhereTheSeedEmittedARow()
        {
            var empty = new Empty();

            ReadAll(ClrCursorDefaults.RepeatUnion(new RowsCursor([1]), empty.Open, empty.OpenAsync, -1, true, null, null)).Should().Equal(1);
            empty.Evaluations.Should().Be(2);
        }

        /// <summary>
        /// A seed that emitted nothing leaves the sentinel set, so the first empty round does stop it.
        /// </summary>
        /// <remarks>
        /// The other side of the same test, and the reason the extra round is a property of the seed/iteration
        /// boundary rather than of the loop: with no seed row there is nothing to leave in <c>current</c>, and
        /// round 0 stops the sequence on its own. One evaluation rather than two.
        /// </remarks>
        [Fact]
        public void ShouldEvaluateAnEmptyIterativePartOnceWhereTheSeedWasEmpty()
        {
            var empty = new Empty();

            ReadAll(ClrCursorDefaults.RepeatUnion(new RowsCursor([]), empty.Open, empty.OpenAsync, -1, true, null, null)).Should().BeEmpty();
            empty.Evaluations.Should().Be(1);
        }

        /// <summary>
        /// A limit of zero never opens the iterative part at all.
        /// </summary>
        [Fact]
        public void ShouldNotOpenTheIterativePartWhereTheLimitIsZero()
        {
            var rounds = new Rounds();
            var rows = ReadAll(ClrCursorDefaults.RepeatUnion(new RowsCursor([1]), rounds.Open, rounds.OpenAsync, 0, true, null, null));

            rows.Should().Equal(1);
            rounds.Evaluations.Should().Be(0);
        }

        /// <summary>
        /// The two advances read one sequence of rounds: a round started by one kind of advance is
        /// continued by the other.
        /// </summary>
        [Fact]
        public async Task ShouldReadTheSameRoundsThroughEitherAdvance()
        {
            var rounds = new Rounds();
            var cursor = ClrCursorDefaults.RepeatUnion(new RowsCursor([1, 2]), rounds.Open, rounds.OpenAsync, -1, true, null, null);

            cursor.Read().Should().BeTrue();
            cursor.Current.Should().Be(1);
            (await cursor.ReadAsync(CancellationToken.None)).Should().BeTrue();
            cursor.Current.Should().Be(2);
            cursor.Read().Should().BeTrue();
            cursor.Current.Should().Be(100);
            (await cursor.ReadAsync(CancellationToken.None)).Should().BeFalse();

            rounds.Evaluations.Should().Be(2);
        }

        /// <summary>
        /// The clean-up runs before the cursors are disposed, which is the order of Calcite's
        /// <c>close()</c>.
        /// </summary>
        /// <remarks>
        /// Taking one row and stopping leaves the seed open, so that its disposal is observable after the
        /// clean-up rather than lost in the drain.
        /// </remarks>
        [Fact]
        public void ShouldRunTheCleanUpBeforeDisposingTheCursors()
        {
            var order = new List<string>();
            var empty = new Empty();

            using (var cursor = ClrCursorDefaults.RepeatUnion(new RowsCursor([1, 2], () => order.Add("seed disposed")), empty.Open, empty.OpenAsync, -1, true, null, () => order.Add("clean up")))
                cursor.Read().Should().BeTrue();

            order.Should().Equal("clean up", "seed disposed");
        }

        /// <summary>
        /// <c>Current</c> before the first row and after the last is Calcite's <c>NoSuchElementException</c>:
        /// the sentinel is what the cursor holds there.
        /// </summary>
        [Fact]
        public void ShouldRefuseCurrentOffTheSentinel()
        {
            var empty = new Empty();
            var cursor = ClrCursorDefaults.RepeatUnion(new RowsCursor([1]), empty.Open, empty.OpenAsync, -1, true, null, null);

            var before = () => cursor.Current;
            before.Should().Throw<InvalidOperationException>();

            cursor.Read().Should().BeTrue();
            cursor.Read().Should().BeFalse();

            var after = () => cursor.Current;
            after.Should().Throw<InvalidOperationException>();
        }

    }

}
