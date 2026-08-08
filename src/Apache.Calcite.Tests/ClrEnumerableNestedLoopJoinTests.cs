using System.Collections;
using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using JoinType = org.apache.calcite.linq4j.JoinType;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// What <c>ClrEnumerableDefaults.NestedLoopJoin</c> does, against what
    /// <c>EnumerableDefaults.nestedLoopJoin</c> does.
    /// </summary>
    /// <remarks>
    /// The oracle everywhere else is a query run through both conventions, and it cannot reach any of these.
    /// The result selector a plan hands the join is <c>ClrEnumUtils.JoinSelector</c>, and for SEMI that
    /// lambda declares its right parameter and never reads it — a semi join's row is the left row — so which
    /// right row the join passes is invisible to every query, and so is a join type the planner never
    /// produces. What the method does with a sequence it is given — how many times it enumerates it, when it
    /// runs, and what it does with two right rows that are the same object — is not a property of any row a
    /// query returns either. So these are direct tests of the method, and each one is written against
    /// Calcite's body rather than against what SQL would want.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableNestedLoopJoinTests
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
        /// A sequence that counts how many times it has been enumerated.
        /// </summary>
        sealed class Counting<T>(IEnumerable<T> source) : IEnumerable<T>
        {

            public int Count;

            public IEnumerator<T> GetEnumerator()
            {
                Count++;
                return source.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        }

        static string Pair(string? left, Row? right) => (left ?? "-") + "/" + (right?.Value ?? "-");

        static List<string> Join(IEnumerable<string> outer, IEnumerable<Row> inner, JoinType joinType) =>
            [.. ClrEnumerableDefaults.NestedLoopJoin<string, Row, string>(outer, inner, Pair, (l, r) => r != null && l != null && r.Value.StartsWith(l), joinType)];

        static IEnumerable<Row> Rows(params string[] values) => values.Select(v => new Row(v));

        // ------------------------------------------------------------------ which right row a semi join passes

        /// <summary>
        /// A semi join passes the right row that matched, not a null one.
        /// </summary>
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c> returns from state 1 with <c>innerValue</c> set to the row the
        /// predicate accepted, and <c>current()</c> is <c>resultSelector.apply(outerValue, innerValue)</c>.
        /// The row is therefore built from the match. Passing null instead returns the same rows for every
        /// plan, because a semi join's selector ignores its right parameter, and a different answer for
        /// <c>MergeJoin</c>, which calls this method for a semi merge join with a residual.
        /// </remarks>
        [TestMethod]
        public void ShouldPassTheMatchedRightRowToASemiJoin() =>
            Join(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.SEMI)
                .Should().Equal("a/a1", "b/b1");

        /// <summary>
        /// An anti join passes no right row, because it never returns one that matched.
        /// </summary>
        [TestMethod]
        public void ShouldPassNoRightRowToAnAntiJoin() =>
            Join(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.ANTI)
                .Should().Equal("c/-");

        [TestMethod]
        public void ShouldPairEveryMatchOfAnInnerJoin() =>
            Join(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.INNER)
                .Should().Equal("a/a1", "a/a2", "b/b1");

        [TestMethod]
        public void ShouldKeepTheUnmatchedLeftRowOfALeftJoin() =>
            Join(["a", "b", "c"], Rows("a1", "a2", "b1"), JoinType.LEFT)
                .Should().Equal("a/a1", "a/a2", "b/b1", "c/-");

        [TestMethod]
        public void ShouldKeepTheUnmatchedRightRowOfARightJoin() =>
            Join(["a", "b"], Rows("a1", "c1", "b1"), JoinType.RIGHT)
                .Should().Equal("a/a1", "b/b1", "-/c1");

        [TestMethod]
        public void ShouldKeepBothUnmatchedSidesOfAFullJoin() =>
            Join(["a", "b", "d"], Rows("a1", "c1", "b1"), JoinType.FULL)
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
        [TestMethod]
        public void ShouldReturnNothingForAJoinTypeTheSwitchDoesNotName()
        {
            Join(["a", "b"], Rows("a1", "b1"), JoinType.ASOF).Should().BeEmpty();
            Join(["a", "b"], Rows("a1", "b1"), JoinType.LEFT_ASOF).Should().BeEmpty();
            Join(["a", "b"], Rows("a1", "b1"), JoinType.LEFT_MARK).Should().BeEmpty();
        }

        // ------------------------------------------------------------------ what each body does with the inner

        /// <summary>
        /// The streaming body enumerates the inner once for every outer row.
        /// </summary>
        /// <remarks>
        /// <c>nestedLoopJoinOptimized</c> calls <c>inner.enumerator()</c> in state 0, which it reaches once
        /// per outer row, and never reads the inner into a list. <c>leftMarkJoinInternal</c> does the same.
        /// </remarks>
        [TestMethod]
        public void ShouldEnumerateTheInnerForEveryOuterRowOfAnInnerJoin()
        {
            var inner = new Counting<Row>(Rows("x1", "y1").ToList());

            Join(["a", "b", "c"], inner, JoinType.INNER).Should().BeEmpty();
            inner.Count.Should().Be(3);
        }

        /// <summary>
        /// The list body reads the inner once and walks the list from then on.
        /// </summary>
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> calls <c>inner.toList()</c>, and that asymmetry with the streaming
        /// body is Calcite's own: it needs the right rows a second time, to emit the ones nothing matched.
        /// </remarks>
        [TestMethod]
        public void ShouldEnumerateTheInnerOnceForARightJoin()
        {
            var inner = new Counting<Row>(Rows("x1", "y1").ToList());

            Join(["a", "b", "c"], inner, JoinType.RIGHT).Should().HaveCount(2);
            inner.Count.Should().Be(1);
        }

        // ------------------------------------------------------------------ when each body runs

        /// <summary>
        /// A right join runs when it is called, not when its result is enumerated.
        /// </summary>
        /// <remarks>
        /// <c>nestedLoopJoinAsList</c> builds the whole result and hands back
        /// <c>Linq4j.asEnumerable(result)</c>, so the work is done before the caller has the sequence.
        /// </remarks>
        [TestMethod]
        public void ShouldRunARightJoinWhenItIsCalled()
        {
            var outer = new Counting<string>(["a", "b"]);

            ClrEnumerableDefaults.NestedLoopJoin<string, Row, string>(outer, [.. Rows("a1")], Pair, (l, r) => false, JoinType.RIGHT);

            outer.Count.Should().Be(1);
        }

        /// <summary>
        /// An inner join runs when its result is enumerated, and not before.
        /// </summary>
        [TestMethod]
        public void ShouldNotRunAnInnerJoinUntilItIsEnumerated()
        {
            var outer = new Counting<string>(["a", "b"]);

            ClrEnumerableDefaults.NestedLoopJoin<string, Row, string>(outer, [.. Rows("a1")], Pair, (l, r) => false, JoinType.INNER);

            outer.Count.Should().Be(0);
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
        [TestMethod]
        public void ShouldEmitOneRowForTwoUnmatchedRightRowsThatAreTheSameObject()
        {
            var shared = new Row("x1");

            Join(["a"], [shared, shared], JoinType.RIGHT).Should().Equal("-/x1");
            Join(["a"], [shared, shared], JoinType.FULL).Should().Equal("a/-", "-/x1");
        }

        /// <summary>
        /// Two unmatched right rows of equal value but different identity are both emitted.
        /// </summary>
        /// <remarks>
        /// The set keys on the reference and not on the value, so equality of the rows is not what collapses
        /// them; only being the same object is.
        /// </remarks>
        [TestMethod]
        public void ShouldEmitBothUnmatchedRightRowsThatAreEqualButNotTheSameObject() =>
            Join(["a"], [new Row("x1"), new Row("x1")], JoinType.RIGHT)
                .Should().Equal("-/x1", "-/x1");

        /// <summary>
        /// One match against a right row that is in the list twice takes both occurrences out.
        /// </summary>
        /// <remarks>
        /// The set holds one entry for the object, so the single <c>remove</c> a match performs leaves
        /// nothing behind for the second occurrence.
        /// </remarks>
        [TestMethod]
        public void ShouldTreatBothOccurrencesOfOneObjectAsMatched()
        {
            var shared = new Row("a1");

            Join(["a"], [shared, shared], JoinType.RIGHT).Should().Equal("a/a1", "a/a1");
        }

    }

}
