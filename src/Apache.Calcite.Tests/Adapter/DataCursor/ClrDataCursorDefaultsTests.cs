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
