using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

using JoinType = org.apache.calcite.linq4j.JoinType;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests that <c>ClrCursorDefaults.NestedLoopJoin</c> behaves as <c>EnumerableDefaults.nestedLoopJoin</c>
    /// does.
    /// </summary>
    /// <remarks>
    /// None of this is visible to a query run through both conventions: a plan's semi join selector ignores
    /// its right parameter, the planner never produces some join types, and how often and when the operator
    /// opens its inputs, and how it treats two right rows that are the same object, do not change the rows a
    /// query returns. So these test the operator directly, with Calcite's implementation as the expected
    /// behaviour rather than SQL's. Most read the join through both <c>Read</c> and <c>ReadAsync</c>.
    /// </remarks>
    public class ClrCursorNestedLoopJoinTests
    {

        /// <summary>
        /// A right row. It is a reference type, so that two rows can be the same object.
        /// </summary>
        /// <param name="value">The row's text, which the join predicate matches against the outer row.</param>
        sealed class Row(string value)
        {

            public readonly string Value = value;

            public override string ToString() => Value;

        }

        /// <summary>
        /// A cursor over a fixed list of rows.
        /// </summary>
        /// <typeparam name="T">The row type.</typeparam>
        /// <param name="rows">The rows the cursor returns, in order.</param>
        sealed class RowsCursor<T>(IReadOnlyList<T> rows) : ClrCursor<T>
        {

            int index = -1;

            public override T Current => rows[index];

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

            }

        }

        /// <summary>
        /// An input that counts how many times it has been opened, by either opener.
        /// </summary>
        /// <typeparam name="T">The row type.</typeparam>
        /// <param name="source">The rows each opened cursor returns.</param>
        sealed class Counting<T>(IReadOnlyList<T> source)
        {

            public int Count;

            public IClrCursor<T> Open()
            {
                Count++;
                return new RowsCursor<T>(source);
            }

            public ValueTask<IClrCursor<T>> OpenAsync(CancellationToken cancellationToken)
            {
                Count++;
                return new ValueTask<IClrCursor<T>>(new RowsCursor<T>(source));
            }

        }

        static string Pair(string? left, Row? right) => (left ?? "-") + "/" + (right?.Value ?? "-");

        static bool StartsWith(string? l, Row? r) => r != null && l != null && r.Value.StartsWith(l);

        /// <summary>
        /// Opens the join synchronously and reads it through <c>Read</c>.
        /// </summary>
        /// <param name="outer">The left rows.</param>
        /// <param name="inner">The right input, which counts its opens.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>Each output row as <c>left/right</c>, with a missing side written as <c>-</c>.</returns>
        static List<string> Join(IReadOnlyList<string> outer, Counting<Row> inner, JoinType joinType)
        {
            var join = ClrCursorDefaults.NestedLoopJoin<string, Row, string>(new RowsCursor<string>(outer), inner.Open, inner.OpenAsync, Pair, StartsWith, joinType);

            var rows = new List<string>();
            while (join.Read())
                rows.Add(join.Current);

            return rows;
        }

        /// <summary>
        /// Opens the join with await and reads it through <c>ReadAsync</c>.
        /// </summary>
        /// <param name="outer">The left rows.</param>
        /// <param name="inner">The right input, which counts its opens.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>Each output row as <c>left/right</c>, with a missing side written as <c>-</c>.</returns>
        static async Task<List<string>> JoinAsync(IReadOnlyList<string> outer, Counting<Row> inner, JoinType joinType)
        {
            var join = await ClrCursorDefaults.NestedLoopJoinAsync<string, Row, string>(
                new ValueTask<IClrCursor<string>>(new RowsCursor<string>(outer)), inner.Open, inner.OpenAsync, Pair, StartsWith, joinType, CancellationToken.None);

            var rows = new List<string>();
            while (await join.ReadAsync(CancellationToken.None))
                rows.Add(join.Current);

            return rows;
        }

        /// <summary>
        /// Reads the join through each advance, requires the same rows from both, and returns them.
        /// </summary>
        /// <param name="outer">The left rows.</param>
        /// <param name="inner">The right rows; each reading gets its own counting input over them.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>Each output row as <c>left/right</c>, with a missing side written as <c>-</c>.</returns>
        static async Task<List<string>> BothWays(IReadOnlyList<string> outer, IReadOnlyList<Row> inner, JoinType joinType)
        {
            var read = Join(outer, new Counting<Row>(inner), joinType);
            var awaited = await JoinAsync(outer, new Counting<Row>(inner), joinType);

            awaited.Should().Equal(read, "the cursor reads the same rows whichever advance is used");

            return read;
        }

        static Row[] Rows(params string[] values) => values.Select(v => new Row(v)).ToArray();

        // ------------------------------------------------------------------ which right row a semi join passes

        /// <summary>
        /// A semi join passes the right row that matched, not a null one.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c> returns from state 1 with <c>innerValue</c> set to the row the predicate
        /// accepted, and <c>current()</c> is <c>resultSelector.apply(outerValue, innerValue)</c>. A plan's semi
        /// join selector ignores its right parameter, so only a direct test sees which row is passed.
        /// </remarks>
        [Fact]
        public async Task ShouldPassTheMatchedRightRowToASemiJoin() =>
            (await BothWays(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.SEMI))
                .Should().Equal("a/a1", "b/b1");

        /// <summary>
        /// An anti join passes no right row, because it never returns one that matched.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldPassNoRightRowToAnAntiJoin() =>
            (await BothWays(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.ANTI))
                .Should().Equal("c/-");

        [Fact]
        public async Task ShouldPairEveryMatchOfAnInnerJoin() =>
            (await BothWays(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.INNER))
                .Should().Equal("a/a1", "a/a2", "b/b1");

        [Fact]
        public async Task ShouldKeepTheUnmatchedLeftRowOfALeftJoin() =>
            (await BothWays(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.LEFT))
                .Should().Equal("a/a1", "a/a2", "b/b1", "c/-");

        [Fact]
        public async Task ShouldKeepTheUnmatchedRightRowOfARightJoin() =>
            (await BothWays(["a", "b"], Rows("a1", "c1", "b1"), JoinType.RIGHT))
                .Should().Equal("a/a1", "b/b1", "-/c1");

        [Fact]
        public async Task ShouldKeepBothUnmatchedSidesOfAFullJoin() =>
            (await BothWays(["a", "b", "d"], Rows("a1", "c1", "b1"), JoinType.FULL))
                .Should().Equal("a/a1", "b/b1", "d/-", "-/c1");

        // ------------------------------------------------------------------ a join type the switch does not name

        /// <summary>
        /// A join type none of the six cases names returns nothing at all.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c>'s switch on the join type falls to <c>default: break</c> for ASOF,
        /// LEFT_ASOF and LEFT_MARK, which moves the inner on rather than returning the pair, and the
        /// unmatched-left branch names only LEFT and ANTI. So neither a match nor a miss returns a row, and the
        /// join is empty rather than behaving as an inner join.
        /// </remarks>
        [Fact]
        public async Task ShouldReturnNothingForAJoinTypeTheSwitchDoesNotName()
        {
            (await BothWays(["a", "b"], Rows("a1", "b1"), JoinType.ASOF)).Should().BeEmpty();
            (await BothWays(["a", "b"], Rows("a1", "b1"), JoinType.LEFT_ASOF)).Should().BeEmpty();
            (await BothWays(["a", "b"], Rows("a1", "b1"), JoinType.LEFT_MARK)).Should().BeEmpty();
        }

        // ------------------------------------------------------------------ what each body does with the inner

        /// <summary>
        /// The streaming body opens the inner once for every outer row, by the opener of the advance that
        /// reached the row.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c> calls <c>inner.enumerator()</c> in state 0, which it reaches once
        /// per outer row, and never reads the inner into a list. <c>leftMarkJoinInternal</c> does the same.
        /// </remarks>
        [Fact]
        public async Task ShouldEnumerateTheInnerForEveryOuterRowOfAnInnerJoin()
        {
            var inner = new Counting<Row>(Rows("x1", "y1"));

            Join(["a", "b", "c"], inner, JoinType.INNER).Should().BeEmpty();
            inner.Count.Should().Be(3);

            inner = new Counting<Row>(Rows("x1", "y1"));

            (await JoinAsync(["a", "b", "c"], inner, JoinType.INNER)).Should().BeEmpty();
            inner.Count.Should().Be(3);
        }

        /// <summary>
        /// The list body reads the inner once and walks the list from then on.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> calls <c>inner.toList()</c>, because it needs the right rows a second
        /// time to emit the ones nothing matched.
        /// </remarks>
        [Fact]
        public async Task ShouldEnumerateTheInnerOnceForARightJoin()
        {
            var inner = new Counting<Row>(Rows("x1", "y1"));

            Join(["a", "b", "c"], inner, JoinType.RIGHT).Should().HaveCount(2);
            inner.Count.Should().Be(1);

            inner = new Counting<Row>(Rows("x1", "y1"));

            (await JoinAsync(["a", "b", "c"], inner, JoinType.RIGHT)).Should().HaveCount(2);
            inner.Count.Should().Be(1);
        }

        // ------------------------------------------------------------------ when each body runs

        /// <summary>
        /// A right join runs when it is opened, not when its result is read.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> builds the whole result and hands back
        /// <c>Linq4j.asEnumerable(result)</c>, so the work is done before the caller has the cursor: the
        /// inner has been opened and the outer read to its end.
        /// </remarks>
        [Fact]
        public async Task ShouldRunARightJoinWhenItIsCalled()
        {
            var inner = new Counting<Row>(Rows("a1"));
            var outer = new ReadCountingCursor(["a", "b"]);

            ClrCursorDefaults.NestedLoopJoin<string, Row, string>(outer, inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.RIGHT);

            inner.Count.Should().Be(1);
            outer.Reads.Should().Be(3, "the outer was read to its end at the open");

            inner = new Counting<Row>(Rows("a1"));
            outer = new ReadCountingCursor(["a", "b"]);

            await ClrCursorDefaults.NestedLoopJoinAsync<string, Row, string>(
                new ValueTask<IClrCursor<string>>(outer), inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.RIGHT, CancellationToken.None);

            inner.Count.Should().Be(1);
            outer.Reads.Should().Be(3, "the awaiting open awaited the whole join");
        }

        /// <summary>
        /// An inner join runs when its result is read, and not before.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The outer arrives already opened, so what is checked is when the inner is opened and when the outer
        /// is first read: neither happens at the open, and both happen at the first advance.
        /// </remarks>
        [Fact]
        public async Task ShouldNotRunAnInnerJoinUntilItIsEnumerated()
        {
            var inner = new Counting<Row>(Rows("a1"));
            var outer = new ReadCountingCursor(["a", "b"]);

            var join = ClrCursorDefaults.NestedLoopJoin<string, Row, string>(outer, inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.INNER);

            inner.Count.Should().Be(0);
            outer.Reads.Should().Be(0);

            join.Read().Should().BeFalse();
            inner.Count.Should().Be(2, "one open per outer row, once the rows were asked for");
            outer.Reads.Should().Be(3);

            inner = new Counting<Row>(Rows("a1"));
            outer = new ReadCountingCursor(["a", "b"]);

            join = await ClrCursorDefaults.NestedLoopJoinAsync<string, Row, string>(
                new ValueTask<IClrCursor<string>>(outer), inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.INNER, CancellationToken.None);

            inner.Count.Should().Be(0);
            outer.Reads.Should().Be(0);

            (await join.ReadAsync(CancellationToken.None)).Should().BeFalse();
            inner.Count.Should().Be(2);
            outer.Reads.Should().Be(3);
        }

        /// <summary>
        /// A cursor over a fixed list of rows that counts its advances.
        /// </summary>
        /// <param name="rows">The rows the cursor returns, in order.</param>
        sealed class ReadCountingCursor(IReadOnlyList<string> rows) : ClrCursor<string>
        {

            int index = -1;

            public int Reads;

            public override string Current => rows[index];

            public override bool Read()
            {
                Reads++;

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

            }

        }

        // ------------------------------------------------------------------ the identity set of unmatched right rows

        /// <summary>
        /// Two unmatched right rows that are the same object are emitted once.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> holds the unmatched right rows in <c>Sets.newIdentityHashSet()</c>,
        /// which keys on the reference, so one object added twice is one entry and is emitted once. SQL would
        /// emit two rows, since the right side has two rows and neither matched; this reproduces Calcite.
        /// </remarks>
        [Fact]
        public async Task ShouldEmitOneRowForTwoUnmatchedRightRowsThatAreTheSameObject()
        {
            var shared = new Row("x1");

            (await BothWays(["a"], [shared, shared], JoinType.RIGHT)).Should().Equal("-/x1");
            (await BothWays(["a"], [shared, shared], JoinType.FULL)).Should().Equal("a/-", "-/x1");
        }

        /// <summary>
        /// Two unmatched right rows of equal value but different identity are both emitted.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The set keys on the reference, not on the value, so only rows that are the same object collapse.
        /// </remarks>
        [Fact]
        public async Task ShouldEmitBothUnmatchedRightRowsThatAreEqualButNotTheSameObject() =>
            (await BothWays(["a"], [new Row("x1"), new Row("x1")], JoinType.RIGHT))
                .Should().Equal("-/x1", "-/x1");

        /// <summary>
        /// One match against a right row that is in the list twice takes both occurrences out.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        /// <remarks>
        /// The set holds one entry for the object, so the single <c>remove</c> a match performs leaves nothing
        /// for the second occurrence.
        /// </remarks>
        [Fact]
        public async Task ShouldTreatBothOccurrencesOfOneObjectAsMatched()
        {
            var shared = new Row("a1");

            (await BothWays(["a"], [shared, shared], JoinType.RIGHT)).Should().Equal("a/a1", "a/a1");
        }

    }

}
