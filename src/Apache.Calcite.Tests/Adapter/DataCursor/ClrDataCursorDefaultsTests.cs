using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.DataCursor.Tests
{

    /// <summary>
    /// Holds the operator table to the shape the convention reads it by, and the operators to linq4j's
    /// timing.
    /// </summary>
    public class ClrDataCursorDefaultsTests
    {

        /// <summary>
        /// A cursor over rows in hand that counts what is asked of it.
        /// </summary>
        sealed class CountingCursor(IReadOnlyList<object[]> rows) : ClrDataCursor<object[]>
        {

            int index = -1;

            public int Drawn { get; private set; }

            public bool Disposed { get; private set; }

            public override object[] Current => rows[index];

            public override bool Read()
            {
                Drawn++;

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
                Disposed = true;
            }

        }

        /// <summary>
        /// An operator that awaits its acquisition returns a <see cref="ValueTask{TResult}"/> of a cursor,
        /// carries the suffix and ends in a token; one that acquires synchronously returns the cursor, and
        /// carries neither.
        /// </summary>
        /// <remarks>
        /// The tree-reading tests tell the two sets apart by exactly this, so it is held here directly, on
        /// the type rather than on a plan.
        /// </remarks>
        [Fact]
        public void ShouldNameEveryAwaitingOpenWithTheSuffixAndNoOtherOpen()
        {
            var wrong = new List<string>();

            foreach (var method in typeof(ClrDataCursorDefaults).GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                var returns = method.ReturnType;
                if (returns.IsGenericType == false)
                    continue;

                var definition = returns.GetGenericTypeDefinition();
                var awaits = definition == typeof(ValueTask<>);
                var opens = definition == typeof(ClrDataCursor<>);

                if (awaits == false && opens == false)
                    continue;

                if (awaits != method.Name.EndsWith("Async", StringComparison.Ordinal))
                    wrong.Add($"{method.Name} {(awaits ? "awaits" : "does not await")}");

                var parameters = method.GetParameters();
                if (awaits && (parameters.Length == 0 || parameters[^1].ParameterType != typeof(CancellationToken)))
                    wrong.Add($"{method.Name} does not end in a token");
            }

            wrong.Should().BeEmpty("an open that awaits is named with the Async suffix and ends in a token, and one that does not is not");
        }

        /// <summary>
        /// A satisfied fetch has drawn one row more than it returned.
        /// </summary>
        /// <remarks>
        /// <c>take</c> is <c>takeWhile</c> over <c>n &lt; count</c>, and the enumerator has to draw a row
        /// before it can test it. That is Calcite's, and the cursor keeps it.
        /// </remarks>
        [Fact]
        public async Task ShouldDrawOneRowMoreThanItTakes()
        {
            var source = new CountingCursor([[1], [2], [3], [4]]);
            var take = ClrDataCursorDefaults.Take(source, java.math.BigDecimal.valueOf(2));

            take.Read().Should().BeTrue();
            (await take.ReadAsync(CancellationToken.None)).Should().BeTrue();
            take.Read().Should().BeFalse();
            source.Drawn.Should().Be(3);

            (await take.ReadAsync(CancellationToken.None)).Should().BeFalse("and once done, the source is not drawn from again");
            source.Drawn.Should().Be(3);
        }

        /// <summary>
        /// Disposing an operator's cursor disposes its source, read or not.
        /// </summary>
        [Fact]
        public async Task ShouldDisposeTheSourceWithTheCursor()
        {
            var source = new CountingCursor([[1]]);
            ClrDataCursorDefaults.Skip(source, java.math.BigDecimal.ONE).Dispose();
            source.Disposed.Should().BeTrue();

            var other = new CountingCursor([[1]]);
            await ClrDataCursorDefaults.Calc<object[], object>(other, null, r => r[0]).DisposeAsync();
            other.Disposed.Should().BeTrue();
        }

        /// <summary>
        /// A concat opens each source at its turn, by the opener of the advance that reached it, and
        /// closes it once exhausted.
        /// </summary>
        [Fact]
        public async Task ShouldOpenEachConcatSourceAtItsTurn()
        {
            var opened = new List<string>();
            CountingCursor? first = null;
            CountingCursor? second = null;

            var concat = ClrDataCursorDefaults.Concat<object[]>(
                () => { opened.Add("first"); return first = new CountingCursor([[1], [2]]); },
                token => { opened.Add("firstAsync"); return new ValueTask<ClrDataCursor<object[]>>(first = new CountingCursor([[1], [2]])); },
                () => { opened.Add("second"); return second = new CountingCursor([[3]]); },
                token => { opened.Add("secondAsync"); return new ValueTask<ClrDataCursor<object[]>>(second = new CountingCursor([[3]])); });

            opened.Should().BeEmpty("the open acquires nothing");

            concat.Read().Should().BeTrue();
            opened.Should().Equal(["first"]);
            concat.Read().Should().BeTrue();

            (await concat.ReadAsync(CancellationToken.None)).Should().BeTrue();
            opened.Should().Equal(["first", "secondAsync"], "the second was reached by an awaiting advance");
            first!.Disposed.Should().BeTrue("and the first was closed once exhausted");
            concat.Current[0].Should().Be(3);

            concat.Read().Should().BeFalse();
            second!.Disposed.Should().BeTrue();
        }

        /// <summary>
        /// A union drains its first source and closes it before it opens its second.
        /// </summary>
        /// <remarks>
        /// Over scalar rows, because the natural comparer the null stands for is Java's <c>equals</c>, and
        /// two arrays holding the same values are not equal by it; a plan passes the physical type's own
        /// comparer for an array row.
        /// </remarks>
        [Fact]
        public void ShouldDrainTheFirstSourceBeforeOpeningTheSecond()
        {
            var first = new ScalarCursor(["a", "b"]);
            var firstDisposedWhenSecondOpened = false;

            var union = ClrDataCursorDefaults.Union<object>(
                first,
                () => { firstDisposedWhenSecondOpened = first.Disposed; return new ScalarCursor(["b", "c"]); },
                null);

            firstDisposedWhenSecondOpened.Should().BeTrue("EnumerableDefaults.union runs source0.into(set) to completion before touching source1");

            var rows = new List<string>();
            while (union.Read())
                rows.Add((string)union.Current);

            rows.Should().Equal(["a", "b", "c"]);
        }

        /// <summary>
        /// A correlated join refuses RIGHT and FULL where it is built, not where it is read.
        /// </summary>
        /// <remarks>
        /// <c>correlateJoin</c> throws before it constructs the enumerable at all. Deferring the refusal into
        /// an advance would let a plan be opened and only fail once rows were read; falling through and
        /// inner-joining would let it not fail at all. The awaiting open refuses before it awaits its outer,
        /// so the refusal is the open's rather than the first advance's.
        /// </remarks>
        [Fact]
        public async Task ShouldRefuseARightOrFullCorrelateWhereItIsBuilt()
        {
            foreach (var joinType in new[] { org.apache.calcite.linq4j.JoinType.RIGHT, org.apache.calcite.linq4j.JoinType.FULL })
            {
                var act = () => ClrDataCursorDefaults.CorrelateJoin<object, object, string>(
                    new ScalarCursor([1]),
                    _ => new ScalarCursor([1]),
                    (_, _) => new ValueTask<ClrDataCursor<object>?>(new ScalarCursor([1])),
                    (a, b) => $"{a}:{b}",
                    joinType);

                act.Should().Throw<ArgumentException>().WithMessage("*" + joinType.name() + "*");

                var actAsync = () => ClrDataCursorDefaults.CorrelateJoinAsync<object, object, string>(
                    new ValueTask<ClrDataCursor<object>>(new ScalarCursor([1])),
                    _ => new ScalarCursor([1]),
                    (_, _) => new ValueTask<ClrDataCursor<object>?>(new ScalarCursor([1])),
                    (a, b) => $"{a}:{b}",
                    joinType,
                    CancellationToken.None).AsTask();

                await actAsync.Should().ThrowAsync<ArgumentException>().WithMessage("*" + joinType.name() + "*");
            }
        }

        /// <summary>
        /// A correlated function that answers null is read as a cursor of no rows.
        /// </summary>
        /// <remarks>
        /// <c>Linq4j.emptyEnumerable()</c>, which Calcite substitutes rather than dereferencing. A LEFT join
        /// therefore emits the outer row against null instead of throwing, through either advance.
        /// </remarks>
        [Fact]
        public async Task ShouldReadANullCorrelatedSequenceAsEmpty()
        {
            var join = ClrDataCursorDefaults.CorrelateJoin<object, object, string>(
                new ScalarCursor([1, 2]),
                _ => null,
                (_, _) => new ValueTask<ClrDataCursor<object>?>((ClrDataCursor<object>?)null),
                (a, b) => $"{a}:{b ?? "null"}",
                org.apache.calcite.linq4j.JoinType.LEFT);

            join.Read().Should().BeTrue();
            join.Current.Should().Be("1:null");
            (await join.ReadAsync(CancellationToken.None)).Should().BeTrue();
            join.Current.Should().Be("2:null");
            join.Read().Should().BeFalse();
        }

        /// <summary>
        /// A correlated join opens its inner once per outer row, by the opener of the advance that reached
        /// that row, and closes it before opening the next.
        /// </summary>
        /// <remarks>
        /// <c>correlateJoin</c> acquires its outer at <c>enumerator()</c>, which is the open, and each inner
        /// inside <c>moveNext</c>; the previous inner is closed there before the next is acquired.
        /// </remarks>
        [Fact]
        public async Task ShouldOpenTheInnerPerOuterRowByTheAdvanceThatReachesIt()
        {
            var opened = new List<string>();
            var inners = new List<ScalarCursor>();

            ScalarCursor Open(string kind, object row)
            {
                opened.Add($"{kind}:{row}");
                var inner = new ScalarCursor(["a", "b"]);
                inners.Add(inner);
                return inner;
            }

            var join = ClrDataCursorDefaults.CorrelateJoin<object, object, string>(
                new ScalarCursor([1, 2]),
                row => Open("inner", row),
                (row, token) => new ValueTask<ClrDataCursor<object>?>(Open("innerAsync", row)),
                (a, b) => $"{a}:{b}",
                org.apache.calcite.linq4j.JoinType.INNER);

            opened.Should().BeEmpty("the open acquires the outer and nothing else");

            join.Read().Should().BeTrue();
            opened.Should().Equal(["inner:1"]);
            join.Current.Should().Be("1:a");

            (await join.ReadAsync(CancellationToken.None)).Should().BeTrue("the second inner row, read through the other advance");
            join.Current.Should().Be("1:b");
            opened.Should().Equal(["inner:1"], "the inner already open is read, not reopened");

            (await join.ReadAsync(CancellationToken.None)).Should().BeTrue();
            opened.Should().Equal(["inner:1", "innerAsync:2"], "the next outer row was reached by an awaiting advance");
            inners[0].Disposed.Should().BeTrue("and the inner before it was closed first");
            join.Current.Should().Be("2:a");

            join.Read().Should().BeTrue();
            join.Read().Should().BeFalse();

            join.Dispose();
            inners[1].Disposed.Should().BeTrue();
        }

        /// <summary>
        /// An ASOF join drains both inputs at the open, the left before the right is opened.
        /// </summary>
        /// <remarks>
        /// <c>asofJoin</c> builds its indexes in the method body, one try-with-resources after the other,
        /// and only then returns the enumerable that walks them.
        /// </remarks>
        [Fact]
        public void ShouldDrainBothAsofInputsAtTheOpen()
        {
            // Java integers, because the keys go into a java.util.HashMap and the timestamps through a Java
            // comparator, exactly as a plan's would
            var left = new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)]);
            var leftDisposedWhenRightOpened = false;
            ScalarCursor? right = null;

            var join = ClrDataCursorDefaults.AsofJoin<object, object, object, string>(
                left,
                () => { leftDisposedWhenRightOpened = left.Disposed; return right = new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)]); },
                x => x,
                x => x,
                (a, b) => $"{a}:{b ?? "null"}",
                (a, b) => true,
                com.google.common.collect.Ordering.natural(),
                false);

            leftDisposedWhenRightOpened.Should().BeTrue("asofJoin closes the left before it acquires the right");
            right!.Disposed.Should().BeTrue("and the right is drained and closed before the cursor is handed back");

            var rows = new List<string>();
            while (join.Read())
                rows.Add(join.Current);

            rows.Should().Equal(["1:1", "2:2"]);
        }

        /// <summary>
        /// A cursor over scalar rows in hand.
        /// </summary>
        sealed class ScalarCursor(IReadOnlyList<object> rows) : ClrDataCursor<object>
        {

            int index = -1;

            public bool Disposed { get; private set; }

            public override object Current => rows[index];

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
                Disposed = true;
            }

        }

    }

}
