using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using org.apache.calcite.linq4j.function;

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
        /// A spool leaves a round's rows in the collection on the advance that finds the input exhausted,
        /// whichever kind of advance that is, and each row is converted on its way in.
        /// </summary>
        /// <remarks>
        /// <c>lazyCollectionSpool</c> buffers in <c>moveNext</c> and flushes in the same <c>moveNext</c> that
        /// returns false, so the collection is untouched while rows are being read and holds the round after.
        /// What reads it back is Java, so a row goes in as Java's: an <see cref="int"/> row is a
        /// <c>java.lang.Integer</c> in the collection.
        /// </remarks>
        [Fact]
        public async Task ShouldLeaveTheRoundInTheCollectionOnceTheInputIsExhausted()
        {
            var collection = new java.util.ArrayList();
            collection.add(java.lang.Integer.valueOf(99));

            var spool = ClrDataCursorDefaults.LazyCollectionSpool<int>(collection, new ScalarIntCursor([1, 2, 3]));

            spool.Read().Should().BeTrue();
            (await spool.ReadAsync(CancellationToken.None)).Should().BeTrue();
            spool.Read().Should().BeTrue();
            collection.size().Should().Be(1, "the collection is untouched until the input is exhausted");

            (await spool.ReadAsync(CancellationToken.None)).Should().BeFalse();
            collection.size().Should().Be(3, "the advance that found the input exhausted replaced the collection with the round");
            collection.get(0).Should().Be(java.lang.Integer.valueOf(1));
            collection.get(2).Should().Be(java.lang.Integer.valueOf(3));
        }

        /// <summary>
        /// A cursor over CLR integers in hand.
        /// </summary>
        sealed class ScalarIntCursor(IReadOnlyList<int> rows) : ClrDataCursor<int>
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

            }

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
        /// A grouped aggregate folds its whole input at the open, before a row is read from it, and closes
        /// the input there.
        /// </summary>
        /// <remarks>
        /// <c>groupBy_</c> drains the input into the map and only then returns a <c>LookupResultEnumerable</c>
        /// over a map that is already finished; the call is the open.
        /// </remarks>
        [Fact]
        public async Task ShouldFoldAGroupedAggregateAtTheOpen()
        {
            var source = new CountingCursor([[java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(2)]]);

            var groups = ClrDataCursorDefaults.GroupBy<object[], object, object>(source, r => r[0], new Count(), new Add(), new Result(), null);

            source.Drawn.Should().Be(4, "the fold runs at the open, reading the input to its end");
            source.Disposed.Should().BeTrue("and closes it there");

            groups.Read().Should().BeTrue();
            groups.Current.Should().Be(java.lang.Integer.valueOf(2));
            (await groups.ReadAsync(CancellationToken.None)).Should().BeTrue("the other advance continues from the same position");
            groups.Current.Should().Be(java.lang.Integer.valueOf(1));
            groups.Read().Should().BeFalse();
            source.Drawn.Should().Be(4, "reading the groups reads nothing more");
        }

        /// <summary>
        /// <see cref="ShouldFoldAGroupedAggregateAtTheOpen"/> through the awaiting open, which awaits the
        /// fold rather than leaving it to the first advance.
        /// </summary>
        [Fact]
        public async Task ShouldFoldAGroupedAggregateAtTheAwaitingOpen()
        {
            var source = new CountingCursor([[java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(2)]]);

            var groups = await ClrDataCursorDefaults.GroupByAsync<object[], object, object>(new ValueTask<ClrDataCursor<object[]>>(source), r => r[0], new Count(), new Add(), new Result(), null, CancellationToken.None);

            source.Drawn.Should().Be(4, "the fold is awaited inside the open");
            source.Disposed.Should().BeTrue();

            (await groups.ReadAsync(CancellationToken.None)).Should().BeTrue();
            groups.Current.Should().Be(java.lang.Integer.valueOf(2));
            groups.Read().Should().BeTrue();
            groups.Current.Should().Be(java.lang.Integer.valueOf(1));
            (await groups.ReadAsync(CancellationToken.None)).Should().BeFalse();
        }

        /// <summary>
        /// A sorted aggregate reads nothing at the open and emits a group when its key changes, holding
        /// nothing but the accumulator of the group being read.
        /// </summary>
        /// <remarks>
        /// <c>SortedAggregateEnumerator</c>'s constructor acquires the enumerator and its <c>moveNext</c>
        /// reads one row past the group, which is the row that starts the next.
        /// </remarks>
        [Fact]
        public async Task ShouldEmitASortedGroupWhenItsKeyChanges()
        {
            var source = new CountingCursor([[java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(2)], [java.lang.Integer.valueOf(3)]]);

            var groups = ClrDataCursorDefaults.SortedGroupBy<object[], object, object>(source, r => r[0], new Count(), new Add(), new Result(), java.util.Collections.reverseOrder());

            source.Drawn.Should().Be(0, "the open acquires and reads nothing");

            groups.Read().Should().BeTrue();
            groups.Current.Should().Be(java.lang.Integer.valueOf(2));
            source.Drawn.Should().Be(3, "the group ends at the row whose key differs, which has been read");

            (await groups.ReadAsync(CancellationToken.None)).Should().BeTrue("the other advance takes the row the last one stopped on");
            groups.Current.Should().Be(java.lang.Integer.valueOf(1));
            source.Drawn.Should().Be(4);

            groups.Read().Should().BeTrue();
            groups.Current.Should().Be(java.lang.Integer.valueOf(1));
            source.Drawn.Should().Be(5, "the last group ends where the input does");

            (await groups.ReadAsync(CancellationToken.None)).Should().BeFalse();
            source.Drawn.Should().Be(5, "and the input is not read again once it has ended");

            groups.Dispose();
            source.Disposed.Should().BeTrue();
        }

        /// <summary>
        /// An aggregate with no group folds where it is called, which is the open, and its awaiting twin
        /// awaits the fold inside the open and hands back the one row.
        /// </summary>
        [Fact]
        public async Task ShouldFoldASingletonAggregateAtTheOpen()
        {
            var source = new CountingCursor([[java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(2)]]);

            var one = ClrDataCursorDefaults.Singleton(ClrDataCursorDefaults.Aggregate<object[], object>(source, new Count().apply(), new Add(), new SingleResult()));

            source.Drawn.Should().Be(4, "Aggregate folds where it is called");
            source.Disposed.Should().BeTrue();

            one.Read().Should().BeTrue();
            one.Current.Should().Be(java.lang.Integer.valueOf(3));
            (await one.ReadAsync(CancellationToken.None)).Should().BeFalse();

            var other = new CountingCursor([[java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(1)], [java.lang.Integer.valueOf(2)]]);

            var awaited = await ClrDataCursorDefaults.SingletonAggregateAsync<object[], object>(new ValueTask<ClrDataCursor<object[]>>(other), new Count().apply(), new Add(), new SingleResult(), CancellationToken.None);

            other.Drawn.Should().Be(4, "the awaiting open awaits the fold, so nothing is left to the first advance");
            other.Disposed.Should().BeTrue();

            (await awaited.ReadAsync(CancellationToken.None)).Should().BeTrue();
            awaited.Current.Should().Be(java.lang.Integer.valueOf(3));
            awaited.Read().Should().BeFalse();
        }

        /// <summary>
        /// A distinct drains its input at the open and closes it there.
        /// </summary>
        [Fact]
        public async Task ShouldDrainADistinctAtTheOpen()
        {
            var source = new ScalarCursor(["a", "b", "a"]);

            var distinct = ClrDataCursorDefaults.Distinct<object>(source, null);

            source.Disposed.Should().BeTrue("EnumerableDefaults.distinct drains and closes where it is called");

            var rows = new List<string>();
            while (distinct.Read())
                rows.Add((string)distinct.Current);

            rows.Should().Equal(["a", "b"]);

            var other = new ScalarCursor(["a", "b", "a"]);

            var awaited = await ClrDataCursorDefaults.DistinctAsync<object>(new ValueTask<ClrDataCursor<object>>(other), null, CancellationToken.None);

            other.Disposed.Should().BeTrue("the awaiting open awaits the drain");

            rows.Clear();
            while (await awaited.ReadAsync(CancellationToken.None))
                rows.Add((string)awaited.Current);

            rows.Should().Equal(["a", "b"]);
        }

        /// <summary>
        /// An accumulator of one counter.
        /// </summary>
        sealed class Count : Function0
        {

            public object apply() => new int[1];

        }

        /// <summary>
        /// Adds a row to the counter.
        /// </summary>
        sealed class Add : Function2
        {

            public object apply(object accumulator, object row)
            {
                ((int[])accumulator)[0]++;
                return accumulator;
            }

        }

        /// <summary>
        /// Answers the counter of a group.
        /// </summary>
        sealed class Result : Function2
        {

            public object apply(object key, object accumulator) => java.lang.Integer.valueOf(((int[])accumulator)[0]);

        }

        /// <summary>
        /// Answers the counter of the one group there is.
        /// </summary>
        sealed class SingleResult : Function1
        {

            public object apply(object accumulator) => java.lang.Integer.valueOf(((int[])accumulator)[0]);

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
