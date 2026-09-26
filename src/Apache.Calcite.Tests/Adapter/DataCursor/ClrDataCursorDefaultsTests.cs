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
        /// An intersect drains its second source and closes it before it opens its first.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.intersect</c> runs <c>source1.into(set1)</c> to completion and only then
        /// reads <c>source0.enumerator()</c> against the set, which is why the first source arrives as an
        /// open and the second as a cursor.
        /// </remarks>
        [Fact]
        public void ShouldDrainTheSecondSourceBeforeOpeningTheFirstOfAnIntersect()
        {
            var second = new ScalarCursor(["b", "c", "d"]);
            var secondDisposedWhenFirstOpened = false;

            var intersect = ClrDataCursorDefaults.Intersect<object>(
                () => { secondDisposedWhenFirstOpened = second.Disposed; return new ScalarCursor(["a", "b", "c"]); },
                second,
                null,
                false);

            secondDisposedWhenFirstOpened.Should().BeTrue("EnumerableDefaults.intersect runs source1.into(set1) to completion before touching source0");

            var rows = new List<string>();
            while (intersect.Read())
                rows.Add((string)intersect.Current);

            rows.Should().Equal(["b", "c"]);
        }

        /// <summary>
        /// INTERSECT ALL keeps a row once per pairing, counting rather than merely holding.
        /// </summary>
        [Fact]
        public async Task ShouldKeepARowOncePerPairingOfAnIntersectAll()
        {
            var intersect = await ClrDataCursorDefaults.IntersectAsync<object>(
                token => new ValueTask<ClrDataCursor<object>>(new ScalarCursor(["a", "b", "b", "c"])),
                new ValueTask<ClrDataCursor<object>>(new ScalarCursor(["b", "b", "b"])),
                null,
                true,
                CancellationToken.None);

            var rows = new List<string>();
            while (await intersect.ReadAsync(CancellationToken.None))
                rows.Add((string)intersect.Current);

            rows.Should().Equal(["b", "b"]);
        }

        /// <summary>
        /// An except drains its first source and closes it before it opens its second.
        /// </summary>
        [Fact]
        public void ShouldDrainTheFirstSourceBeforeOpeningTheSecondOfAnExcept()
        {
            var first = new ScalarCursor(["a", "b", "c"]);
            var firstDisposedWhenSecondOpened = false;

            var except = ClrDataCursorDefaults.Except<object>(
                first,
                () => { firstDisposedWhenSecondOpened = first.Disposed; return new ScalarCursor(["b"]); },
                null,
                false);

            firstDisposedWhenSecondOpened.Should().BeTrue("EnumerableDefaults.except runs source0.into(collection) to completion before touching source1");

            var rows = new List<string>();
            while (except.Read())
                rows.Add((string)except.Current);

            rows.Should().Equal(["a", "c"]);
        }

        /// <summary>
        /// A merge union opens every input and positions each on its first row inside its own open, and
        /// then advances only the input whose row it emitted.
        /// </summary>
        /// <remarks>
        /// <c>MergeUnionEnumerator</c>'s constructor acquires each input and calls <c>moveNext</c> on it,
        /// all at <c>enumerator()</c>; the cursor's open does both.
        /// </remarks>
        [Fact]
        public async Task ShouldPositionEveryMergeUnionInputAtTheOpen()
        {
            var first = new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(3), java.lang.Integer.valueOf(5)]);
            var second = new ScalarCursor([java.lang.Integer.valueOf(2), java.lang.Integer.valueOf(3), java.lang.Integer.valueOf(4)]);
            var opened = 0;

            var sources = new java.util.ArrayList();
            sources.add(new Func<ClrDataCursor<object>>(() => { opened++; return first; }));
            sources.add(new Func<ClrDataCursor<object>>(() => { opened++; return second; }));

            var merge = ClrDataCursorDefaults.MergeUnion<object, object>(
                sources,
                row => row,
                org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false),
                true,
                null);

            opened.Should().Be(2, "every input is acquired at the open");
            first.Drawn.Should().Be(1, "and positioned on its first row there");
            second.Drawn.Should().Be(1);

            var rows = new List<int>();
            while (rows.Count % 2 == 0 ? merge.Read() : await merge.ReadAsync(CancellationToken.None))
                rows.Add(((java.lang.Integer)merge.Current).intValue());

            rows.Should().Equal([1, 2, 3, 3, 4, 5]);
            first.Drawn.Should().Be(4, "each input is drawn once per row it held and once more to find its end");
            second.Drawn.Should().Be(4);

            merge.Dispose();
            first.Disposed.Should().BeTrue("closing the merge closes every input");
            second.Disposed.Should().BeTrue();
        }

        /// <summary>
        /// A merge union without ALL drops a row that repeats one already emitted under the same key.
        /// </summary>
        [Fact]
        public void ShouldDropARepeatedRowOfAMergeUnion()
        {
            var sources = new java.util.ArrayList();
            sources.add(new Func<ClrDataCursor<object>>(() => new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(3)])));
            sources.add(new Func<ClrDataCursor<object>>(() => new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2), java.lang.Integer.valueOf(3)])));

            var merge = ClrDataCursorDefaults.MergeUnion<object, object>(
                sources,
                row => row,
                org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false),
                false,
                null);

            var rows = new List<int>();
            while (merge.Read())
                rows.Add(((java.lang.Integer)merge.Current).intValue());

            rows.Should().Equal([1, 2, 3]);
        }

        /// <summary>
        /// A limit sort drains its source inside its open, and a fetch of no rows never opens it.
        /// </summary>
        /// <remarks>
        /// linq4j's bounded <c>orderBy</c> tests the fetch inside <c>enumerator()</c> before calling
        /// <c>source.enumerator()</c>, and answers <c>Linq4j.emptyEnumerator()</c> without it.
        /// </remarks>
        [Fact]
        public void ShouldNotOpenTheSourceOfALimitSortForAFetchOfNoRows()
        {
            var opened = false;

            var sorted = ClrDataCursorDefaults.OrderByWithFetchAndOffset<object, object>(
                () => { opened = true; return new ScalarCursor([java.lang.Integer.valueOf(1)]); },
                row => row,
                org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false),
                java.math.BigDecimal.ZERO,
                java.math.BigDecimal.ZERO);

            opened.Should().BeFalse("a fetch of no rows answers an empty enumerator before the source is acquired");
            sorted.Read().Should().BeFalse();
        }

        /// <summary>
        /// A limit sort has drained and closed its source by the time its open returns, whichever way it
        /// was opened, and hands back the rows the offset and fetch leave, in order.
        /// </summary>
        [Fact]
        public async Task ShouldDrainTheSourceOfALimitSortAtTheOpen()
        {
            static ScalarCursor Source() => new([java.lang.Integer.valueOf(5), java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(4), java.lang.Integer.valueOf(2), java.lang.Integer.valueOf(3)]);

            var source = Source();
            var sorted = ClrDataCursorDefaults.OrderByWithFetchAndOffset<object, object>(
                () => source,
                row => row,
                org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false),
                java.math.BigDecimal.ONE,
                java.math.BigDecimal.valueOf(2));

            source.Drawn.Should().Be(6, "the open read the whole input");
            source.Disposed.Should().BeTrue("and closed it");

            var rows = new List<int>();
            while (sorted.Read())
                rows.Add(((java.lang.Integer)sorted.Current).intValue());

            rows.Should().Equal([2, 3]);

            var awaitedSource = Source();
            var awaited = await ClrDataCursorDefaults.OrderByWithFetchAndOffsetAsync<object, object>(
                token => new ValueTask<ClrDataCursor<object>>(awaitedSource),
                row => row,
                org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false),
                java.math.BigDecimal.ONE,
                java.math.BigDecimal.valueOf(2),
                CancellationToken.None);

            awaitedSource.Drawn.Should().Be(6, "the awaiting open awaited the whole drain");
            awaitedSource.Disposed.Should().BeTrue();

            rows.Clear();
            while (await awaited.ReadAsync(CancellationToken.None))
                rows.Add(((java.lang.Integer)awaited.Current).intValue());

            rows.Should().Equal([2, 3]);
        }

        /// <summary>
        /// An offset past every row a limit sort holds answers nothing.
        /// </summary>
        [Fact]
        public void ShouldAnswerNothingFromALimitSortWhoseOffsetPassesTheEnd()
        {
            var sorted = ClrDataCursorDefaults.OrderByWithFetchAndOffset<object, object>(
                () => new ScalarCursor([java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2)]),
                row => row,
                org.apache.calcite.linq4j.function.Functions.nullsComparator(false, false),
                java.math.BigDecimal.TEN,
                java.math.BigDecimal.valueOf(2));

            sorted.Read().Should().BeFalse();
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
