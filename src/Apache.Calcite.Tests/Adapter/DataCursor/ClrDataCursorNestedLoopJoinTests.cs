using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

using JoinType = org.apache.calcite.linq4j.JoinType;

namespace Apache.Calcite.Extensions.Adapter.DataCursor.Tests
{

    /// <summary>
    /// What <c>ClrDataCursorDefaults.NestedLoopJoin</c> does, against what
    /// <c>EnumerableDefaults.nestedLoopJoin</c> does.
    /// </summary>
    /// <remarks>
    /// The oracle everywhere else is a query run through both conventions, and it cannot reach any of these.
    /// The result selector a plan hands the join is <c>ClrEnumUtils.JoinSelector</c>, and for SEMI that
    /// lambda declares its right parameter and never reads it — a semi join's row is the left row — so which
    /// right row the join passes is invisible to every query, and so is a join type the planner never
    /// produces. What the operator does with an input it is given — how many times it opens it, when it
    /// runs, and what it does with two right rows that are the same object — is not a property of any row a
    /// query returns either. So these are direct tests of the operator, and each one is written against
    /// Calcite's body rather than against what SQL would want. Each reads the join through <c>Read</c> and
    /// again through <c>ReadAsync</c>, because the cursor promises the same rows either way.
    /// </remarks>
    public class ClrDataCursorNestedLoopJoinTests
    {

        /// <summary>
        /// A right row, which is a reference, so that two of them can be the same object.
        /// </summary>
        sealed class Row(string value)
        {

            public readonly string Value = value;

            public override string ToString() => Value;

        }

        /// <summary>
        /// A cursor over rows in hand.
        /// </summary>
        sealed class RowsCursor<T>(IReadOnlyList<T> rows) : ClrDataCursor<T>
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
        sealed class Counting<T>(IReadOnlyList<T> source)
        {

            public int Count;

            public ClrDataCursor<T> Open()
            {
                Count++;
                return new RowsCursor<T>(source);
            }

            public ValueTask<ClrDataCursor<T>> OpenAsync(CancellationToken cancellationToken)
            {
                Count++;
                return new ValueTask<ClrDataCursor<T>>(new RowsCursor<T>(source));
            }

        }

        static string Pair(string? left, Row? right) => (left ?? "-") + "/" + (right?.Value ?? "-");

        static bool StartsWith(string? l, Row? r) => r != null && l != null && r.Value.StartsWith(l);

        /// <summary>
        /// Opens the join synchronously and reads it through <c>Read</c>.
        /// </summary>
        static List<string> Join(IReadOnlyList<string> outer, Counting<Row> inner, JoinType joinType)
        {
            var join = ClrDataCursorDefaults.NestedLoopJoin<string, Row, string>(new RowsCursor<string>(outer), inner.Open, inner.OpenAsync, Pair, StartsWith, joinType);

            var rows = new List<string>();
            while (join.Read())
                rows.Add(join.Current);

            return rows;
        }

        /// <summary>
        /// Opens the join with await and reads it through <c>ReadAsync</c>.
        /// </summary>
        static async Task<List<string>> JoinAsync(IReadOnlyList<string> outer, Counting<Row> inner, JoinType joinType)
        {
            var join = await ClrDataCursorDefaults.NestedLoopJoinAsync<string, Row, string>(
                new ValueTask<ClrDataCursor<string>>(new RowsCursor<string>(outer)), inner.Open, inner.OpenAsync, Pair, StartsWith, joinType, CancellationToken.None);

            var rows = new List<string>();
            while (await join.ReadAsync(CancellationToken.None))
                rows.Add(join.Current);

            return rows;
        }

        /// <summary>
        /// Reads the join both ways and requires the same rows of each.
        /// </summary>
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
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c> returns from state 1 with <c>innerValue</c> set to the row the
        /// predicate accepted, and <c>current()</c> is <c>resultSelector.apply(outerValue, innerValue)</c>.
        /// The row is therefore built from the match. Passing null instead returns the same rows for every
        /// plan, because a semi join's selector ignores its right parameter, and a different answer for
        /// <c>MergeJoin</c>, which calls this operator for a semi merge join with a residual.
        /// </remarks>
        [Fact]
        public async Task ShouldPassTheMatchedRightRowToASemiJoin() =>
            (await BothWays(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.SEMI))
                .Should().Equal("a/a1", "b/b1");

        /// <summary>
        /// An anti join passes no right row, because it never returns one that matched.
        /// </summary>
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
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c>'s switch on the join type falls to <c>default: break</c> for ASOF,
        /// LEFT_ASOF and LEFT_MARK, which moves the inner on rather than returning the pair, and the
        /// unmatched-left branch names only LEFT and ANTI. So a match returns nothing and a miss returns
        /// nothing, and the join is empty. It is not an inner join.
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
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> calls <c>inner.toList()</c>, and that asymmetry with the streaming
        /// body is Calcite's own: it needs the right rows a second time, to emit the ones nothing matched.
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
        /// A right join runs when it is called, which is the open, not when its result is read.
        /// </summary>
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

            ClrDataCursorDefaults.NestedLoopJoin<string, Row, string>(outer, inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.RIGHT);

            inner.Count.Should().Be(1);
            outer.Reads.Should().Be(3, "the outer was read to its end at the open");

            inner = new Counting<Row>(Rows("a1"));
            outer = new ReadCountingCursor(["a", "b"]);

            await ClrDataCursorDefaults.NestedLoopJoinAsync<string, Row, string>(
                new ValueTask<ClrDataCursor<string>>(outer), inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.RIGHT, CancellationToken.None);

            inner.Count.Should().Be(1);
            outer.Reads.Should().Be(3, "the awaiting open awaited the whole join");
        }

        /// <summary>
        /// An inner join runs when its result is read, and not before.
        /// </summary>
        /// <remarks>
        /// The outer arrives opened, so what the operator decides is when the inner is opened and when the
        /// outer is first read: neither happens at the open, and both happen at the first advance.
        /// </remarks>
        [Fact]
        public async Task ShouldNotRunAnInnerJoinUntilItIsEnumerated()
        {
            var inner = new Counting<Row>(Rows("a1"));
            var outer = new ReadCountingCursor(["a", "b"]);

            var join = ClrDataCursorDefaults.NestedLoopJoin<string, Row, string>(outer, inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.INNER);

            inner.Count.Should().Be(0);
            outer.Reads.Should().Be(0);

            join.Read().Should().BeFalse();
            inner.Count.Should().Be(2, "one open per outer row, once the rows were asked for");
            outer.Reads.Should().Be(3);

            inner = new Counting<Row>(Rows("a1"));
            outer = new ReadCountingCursor(["a", "b"]);

            join = await ClrDataCursorDefaults.NestedLoopJoinAsync<string, Row, string>(
                new ValueTask<ClrDataCursor<string>>(outer), inner.Open, inner.OpenAsync, Pair, (l, r) => false, JoinType.INNER, CancellationToken.None);

            inner.Count.Should().Be(0);
            outer.Reads.Should().Be(0);

            (await join.ReadAsync(CancellationToken.None)).Should().BeFalse();
            inner.Count.Should().Be(2);
            outer.Reads.Should().Be(3);
        }

        /// <summary>
        /// A cursor over rows in hand that counts its advances.
        /// </summary>
        sealed class ReadCountingCursor(IReadOnlyList<string> rows) : ClrDataCursor<string>
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
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> holds the unmatched right rows in <c>Sets.newIdentityHashSet()</c>,
        /// which keys on the reference, so one object added twice is one entry and comes out once. SQL wants
        /// it twice — the right side has two rows and neither matched — and Calcite does not give that. This
        /// is a port, so the answer here is Calcite's.
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
        /// <remarks>
        /// The set keys on the reference and not on the value, so equality of the rows is not what collapses
        /// them; only being the same object is.
        /// </remarks>
        [Fact]
        public async Task ShouldEmitBothUnmatchedRightRowsThatAreEqualButNotTheSameObject() =>
            (await BothWays(["a"], [new Row("x1"), new Row("x1")], JoinType.RIGHT))
                .Should().Equal("-/x1", "-/x1");

        /// <summary>
        /// One match against a right row that is in the list twice takes both occurrences out.
        /// </summary>
        /// <remarks>
        /// The set holds one entry for the object, so the single <c>remove</c> a match performs leaves
        /// nothing behind for the second occurrence.
        /// </remarks>
        [Fact]
        public async Task ShouldTreatBothOccurrencesOfOneObjectAsMatched()
        {
            var shared = new Row("a1");

            (await BothWays(["a"], [shared, shared], JoinType.RIGHT)).Should().Equal("a/a1", "a/a1");
        }

    }

}
