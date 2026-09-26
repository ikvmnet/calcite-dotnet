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
        /// A collect drains its input where it is called, which is the open, and closes it there.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableCollect</c> calls <c>toList</c> in its generated <c>bind</c> and wraps the value in
        /// <c>singletonEnumerable</c>; evaluating the tree is that bind.
        /// </remarks>
        [Fact]
        public async Task ShouldDrainACollectAtTheOpen()
        {
            var source = new CountingCursor([[1], [2], [3]]);
            var list = ClrDataCursorDefaults.ToJavaList(source);

            source.Drawn.Should().Be(4, "the drain read every row and the end");
            source.Disposed.Should().BeTrue("and closed the input once it was read");
            list.size().Should().Be(3);

            var singleton = ClrDataCursorDefaults.Singleton(list);
            singleton.Read().Should().BeTrue();
            singleton.Current.Should().BeSameAs(list);
            (await singleton.ReadAsync(CancellationToken.None)).Should().BeFalse();
        }

        /// <summary>
        /// The awaiting collect drains at the open too: the open awaits the drain and hands back the one
        /// row, rather than folding on the first advance as a sequence that cannot await at
        /// <c>GetAsyncEnumerator</c> had to.
        /// </summary>
        [Fact]
        public async Task ShouldDrainACollectAtTheOpenAsync()
        {
            var source = new CountingCursor([[1], [2], [3]]);
            var singleton = await ClrDataCursorDefaults.SingletonJavaListAsync(new ValueTask<ClrDataCursor<object[]>>(source), CancellationToken.None);

            source.Drawn.Should().Be(4, "the open drained the input before any advance");
            source.Disposed.Should().BeTrue();

            (await singleton.ReadAsync(CancellationToken.None)).Should().BeTrue();
            singleton.Current.size().Should().Be(3);
            singleton.Read().Should().BeFalse();
        }

        /// <summary>
        /// A map collect keeps the order the keys arrived in.
        /// </summary>
        [Fact]
        public void ShouldKeepTheArrivalOrderOfACollectedMap()
        {
            var source = new CountingCursor([["b", 2], ["a", 1], ["c", 3]]);
            var map = ClrDataCursorDefaults.ToJavaMap(source, r => r[0], r => r[1]);

            source.Disposed.Should().BeTrue();
            map.Should().BeOfType<java.util.LinkedHashMap>("EnumerableDefaults.toMap builds one so that the order is kept");

            var keys = new List<object>();
            for (var i = map.keySet().iterator(); i.hasNext();)
                keys.Add(i.next());

            keys.Should().Equal(["b", "a", "c"]);
        }

        /// <summary>
        /// An uncollect builds and opens each row's sequence at its turn, inside the advance, and closes
        /// it once exhausted.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.selectMany</c> acquires its source in a field initializer and each row's
        /// sequence inside <c>moveNext</c>; the source arrives opened here and nothing else is touched at
        /// the open.
        /// </remarks>
        [Fact]
        public async Task ShouldOpenEachRowsSequenceAtItsTurn()
        {
            var applied = new List<object>();
            var source = new CountingCursor([[1], [2]]);

            var selectMany = ClrDataCursorDefaults.SelectMany<object[], object>(
                source,
                new Apache.Calcite.Extensions.Linq4j.Function.DelegateFunction1<object[], object>(row =>
                {
                    applied.Add(row[0]);
                    return org.apache.calcite.linq4j.Linq4j.asEnumerable(java.util.Arrays.asList(new object[] { row[0], row[0] }));
                }));

            applied.Should().BeEmpty("the open builds no row's sequence");

            selectMany.Read().Should().BeTrue();
            applied.Should().Equal([1]);
            selectMany.Current.Should().Be(1);

            (await selectMany.ReadAsync(CancellationToken.None)).Should().BeTrue("the second element of the first row's sequence, read by the other advance");
            applied.Should().Equal([1]);

            selectMany.Read().Should().BeTrue();
            applied.Should().Equal([1, 2], "the second row's sequence was built when the first was exhausted");
            selectMany.Current.Should().Be(2);

            (await selectMany.ReadAsync(CancellationToken.None)).Should().BeTrue();
            selectMany.Read().Should().BeFalse();

            selectMany.Dispose();
            source.Disposed.Should().BeTrue();
        }

        /// <summary>
        /// The awaiting combine opens each query only after the one before it has been read and closed,
        /// which is the order Calcite's generated <c>bind</c> reads them in and the order the synchronous
        /// tree evaluates them in.
        /// </summary>
        [Fact]
        public async Task ShouldOpenEachCombinedQueryAfterTheOneBeforeItIsRead()
        {
            var first = new RowsCursor<java.util.Map>([Map("A", 1), Map("A", 2)]);
            var second = new RowsCursor<java.util.Map>([Map("B", 3)]);
            var firstDisposedWhenSecondOpened = false;

            var combined = await ClrDataCursorDefaults.CombineQueryResultsAsync<object[]>(
                [
                    token => new ValueTask<ClrDataCursor<java.util.Map>>(first),
                    token => { firstDisposedWhenSecondOpened = first.Disposed; return new ValueTask<ClrDataCursor<java.util.Map>>(second); },
                ],
                org.apache.calcite.runtime.SqlFunctions.combineQueryResults,
                CancellationToken.None);

            firstDisposedWhenSecondOpened.Should().BeTrue("the first query was read to completion before the second was opened");
            second.Disposed.Should().BeTrue();

            var rows = new List<object[]>();
            while (await combined.ReadAsync(CancellationToken.None))
                rows.Add(combined.Current);

            rows.Should().HaveCount(2, "the row count is the longer query's");
            ((java.util.Map)rows[0][0]).get("A").Should().Be(java.lang.Integer.valueOf(1));
            ((java.util.Map)rows[0][1]).get("B").Should().Be(java.lang.Integer.valueOf(3));
            rows[1][1].Should().BeNull("the shorter query has nothing for the second row");
        }

        /// <summary>
        /// Builds a one-entry map, as <c>SqlFunctions.map</c> builds a query's row.
        /// </summary>
        static java.util.Map Map(string key, int value) =>
            org.apache.calcite.runtime.SqlFunctions.map([key, java.lang.Integer.valueOf(value)]);

        /// <summary>
        /// A cursor over rows of any type in hand.
        /// </summary>
        sealed class RowsCursor<T>(IReadOnlyList<T> rows) : ClrDataCursor<T>
        {

            int index = -1;

            public bool Disposed { get; private set; }

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
                Disposed = true;
            }

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
