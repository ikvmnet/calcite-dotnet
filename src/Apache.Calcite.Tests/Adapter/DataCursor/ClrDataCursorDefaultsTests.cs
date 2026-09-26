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
        /// A window drains its input and computes every row at the open, whichever open it is, and the
        /// cursor handed back reads from the finished list.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableWindow</c>'s block runs to <c>Linq4j.asEnumerable(list)</c> where it is evaluated.
        /// The awaiting open is held to the same moment: the enumerable convention's twin had to leave the
        /// drain to the first advance, and this one does not.
        /// </remarks>
        [Fact]
        public async Task ShouldComputeTheWindowAtTheOpen()
        {
            // a running total over scalar rows, ordered by linq4j's own comparator of Comparables
            static int Amount(WindowFrame frame) => ((java.lang.Integer)frame.Rows[frame.Position]).intValue();

            static ClrDataCursor<int> Open(ScalarCursor source) => ClrDataCursorDefaults.Window<object, object, int, int>(
                source, null, org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false), org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_NO_OTHER,
                frame => 0, frame => frame.Index, true, false, false, false,
                () => 0, (frame, acc) => 0, (frame, acc) => acc + Amount(frame), (frame, acc) => acc, null, (frame, acc) => acc);

            static ValueTask<ClrDataCursor<int>> OpenAsync(ScalarCursor source) => ClrDataCursorDefaults.WindowAsync<object, object, int, int>(
                new ValueTask<ClrDataCursor<object>>(source), null, org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false), org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_NO_OTHER,
                frame => 0, frame => frame.Index, true, false, false, false,
                () => 0, (frame, acc) => 0, (frame, acc) => acc + Amount(frame), (frame, acc) => acc, null, (frame, acc) => acc, CancellationToken.None);

            var source = new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2), java.lang.Integer.valueOf(3)]);
            var window = Open(source);
            source.Drawn.Should().Be(4, "the open reads the input to its end");
            source.Disposed.Should().BeTrue("and closes it, as into and foreach do");

            var totals = new List<int>();
            while (window.Read())
                totals.Add(window.Current);
            totals.Should().Equal([1, 3, 6]);

            var other = new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2), java.lang.Integer.valueOf(3)]);
            var windowAsync = await OpenAsync(other);
            other.Drawn.Should().Be(4, "the awaiting open awaits the drain rather than leaving it to the first advance");
            other.Disposed.Should().BeTrue();

            totals.Clear();
            while (await windowAsync.ReadAsync(CancellationToken.None))
                totals.Add(windowAsync.Current);
            totals.Should().Equal([1, 3, 6]);
        }

        /// <summary>
        /// A cursor over scalar rows in hand.
        /// </summary>
        sealed class ScalarCursor(IReadOnlyList<object> rows) : ClrDataCursor<object>
        {

            int index = -1;

            public int Drawn { get; private set; }

            public bool Disposed { get; private set; }

            public override object Current => rows[index];

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

    }

}
