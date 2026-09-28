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
    /// Tests that <c>ClrCursorDefaults.RepeatUnion</c> and <c>RepeatUnionAsync</c> behave as
    /// <c>EnumerableDefaults.repeatUnion</c> does, including how many rounds of the iterative part it opens.
    /// </summary>
    /// <remarks>
    /// Calcite's enumerator stops when <c>current</c> still holds its sentinel after a round, not when a round
    /// is empty, and it does not restore the sentinel between the seed and the first round. So a seed that
    /// produced a row costs one extra round. <c>ShouldAgreeOnARecursiveQueryWhoseStepAggregates</c> compares
    /// that against Calcite through SQL; these tests script the iterative part directly and count how many
    /// times it is opened, through both opens and both advances.
    /// </remarks>
    public class ClrCursorRepeatUnionTests
    {

        /// <summary>
        /// A cursor over a fixed list of rows that calls an optional action when disposed.
        /// </summary>
        /// <param name="rows">The rows the cursor returns, in order.</param>
        /// <param name="onDispose">Called when the cursor is disposed; null for nothing.</param>
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
        /// A round that produced rows restores the sentinel, so the empty round after it stops the sequence.
        /// </summary>
        /// <remarks>
        /// Round 0 yields 100 and ends with the sentinel restored; round 1 yields nothing and stops: two
        /// evaluations, not three. This shows that the extra round arises only at the boundary between the seed
        /// and the first round.
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
        /// <returns>A task that completes when the test has run.</returns>
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
        /// A seed that emitted a row causes an extra evaluation of an iterative part that never yields anything,
        /// as in Calcite.
        /// </summary>
        /// <remarks>
        /// Round 0 is empty, but <c>current</c> still holds the seed row, so the loop runs again; round 1 is empty
        /// with the sentinel restored, and that ends it.
        /// </remarks>
        [Fact]
        public void ShouldEvaluateAnEmptyIterativePartTwiceWhereTheSeedEmittedARow()
        {
            var empty = new Empty();

            ReadAll(ClrCursorDefaults.RepeatUnion(new RowsCursor([1]), empty.Open, empty.OpenAsync, -1, true, null, null)).Should().Equal(1);
            empty.Evaluations.Should().Be(2);
        }

        /// <summary>
        /// A seed that emitted nothing leaves the sentinel set, so the first empty round stops the sequence
        /// after one evaluation.
        /// </summary>
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
        /// <returns>A task that completes when the test has run.</returns>
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
        /// The clean-up runs before the cursors are disposed, the order of Calcite's <c>close()</c>.
        /// </summary>
        /// <remarks>
        /// Reading one row and stopping leaves the seed open, so its disposal happens at <c>Dispose</c> and its
        /// order relative to the clean-up is observable.
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
        /// <c>Current</c> throws before the first row and after the last, where the cursor holds the sentinel
        /// and Calcite's enumerator would fail on it.
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
