using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The operators a plan of the <see cref="ClrDataCursorConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>EnumerableDefaults</c>, with one difference of shape that runs through
    /// everything here. A linq4j operator returns a lazy <c>Enumerable</c> and acquires its source inside
    /// <c>enumerator()</c>; an operator here <em>is</em> that <c>enumerator()</c>. It takes an opened cursor
    /// and returns an opened cursor, so a plan's tree of calls is evaluated as the cascade of acquisitions
    /// linq4j runs when the root's enumerator is obtained, and there is no lazy layer for a C# iterator to
    /// quietly move the acquisition out of. Where linq4j defers deliberately — <c>concat</c> acquires each
    /// source at its turn inside <c>moveNext</c> — an operator takes the source as a delegate and the
    /// deferral reads at the site.
    ///
    /// <para><b>Each operator is an open, and each comes in two.</b> The unsuffixed one takes opened
    /// cursors and acquires whatever it acquires — a sort drains — synchronously; the <c>Async</c>-suffixed
    /// one takes awaiting opens, awaits them, and acquires with await. Both return the same cursor class.
    /// That cursor carries <see cref="ClrDataCursor.Read"/> and <see cref="ClrDataCursor.ReadAsync"/> over
    /// one position, each stepping its input with the advance of the same kind, so which way a plan was
    /// opened decides nothing about how it is read.</para>
    ///
    /// <para>The cursor classes are here rather than in <c>Runtime</c> because they are the operators'
    /// bodies: what <c>ClrEnumerableDefaults</c> writes as an iterator method, this writes as a class with
    /// two advance methods over one set of fields.</para>
    /// </remarks>
    static partial class ClrDataCursorDefaults
    {

        /// <summary>
        /// Returns the first field of each row.
        /// </summary>
        /// <typeparam name="TRow"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// A one column result is the value, not a one element row. Calcite ends a plan the same way, with
        /// <c>Enumerables.slice0</c>, which is <c>select(elements -&gt; elements[0])</c>.
        /// </remarks>
        public static ClrDataCursor<TRow> Slice0<TRow>(ClrDataCursor<object[]> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new Slice0Cursor<TRow>(source);
        }

        /// <summary>
        /// <see cref="Slice0{TRow}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TRow>> Slice0Async<TRow>(ValueTask<ClrDataCursor<object[]>> source, CancellationToken cancellationToken)
        {
            return new Slice0Cursor<TRow>(await source.ConfigureAwait(false));
        }

        /// <summary>
        /// The cursor of <see cref="Slice0{TRow}"/>.
        /// </summary>
        sealed class Slice0Cursor<TRow>(ClrDataCursor<object[]> source) : ClrDataCursor<TRow>
        {

            TRow current = default!;

            /// <inheritdoc />
            public override TRow Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (source.Read() == false)
                    return false;

                // the row came from a table and its fields are still Java's, so the field taken out of it is
                // converted rather than cast
                current = JavaValues.As<TRow>(source.Current[0]);
                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (await source.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    return false;

                current = JavaValues.As<TRow>(source.Current[0]);
                return true;
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// Filters and projects in one pass.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="predicate">Condition each row must satisfy, or null to keep every row.</param>
        /// <param name="selector"></param>
        /// <returns></returns>
        /// <remarks>
        /// What Calcite's <c>EnumerableCalc</c> generates an anonymous <c>Enumerator</c> for: <c>moveNext</c>
        /// advances the input until the condition holds, and <c>current</c> projects. That enumerator
        /// acquires its input in a field initializer, which runs at <c>enumerator()</c>; here the input
        /// arrives opened, which is the same moment.
        /// </remarks>
        public static ClrDataCursor<TResult> Calc<TSource, TResult>(ClrDataCursor<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new CalcCursor<TSource, TResult>(source, predicate, selector);
        }

        /// <summary>
        /// <see cref="Calc{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> CalcAsync<TSource, TResult>(ValueTask<ClrDataCursor<TSource>> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new CalcCursor<TSource, TResult>(await source.ConfigureAwait(false), predicate, selector);
        }

        /// <summary>
        /// The cursor of <see cref="Calc{TSource, TResult}"/>.
        /// </summary>
        sealed class CalcCursor<TSource, TResult>(ClrDataCursor<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector) : ClrDataCursor<TResult>
        {

            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (source.Read())
                {
                    if (predicate == null || predicate(source.Current))
                    {
                        current = selector(source.Current);
                        return true;
                    }
                }

                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (await source.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (predicate == null || predicate(source.Current))
                    {
                        current = selector(source.Current);
                        return true;
                    }
                }

                return false;
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// Projects each row into a new form.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.select</c>, which acquires its source in a field initializer at
        /// <c>enumerator()</c>; the source arrives opened here, which is the same moment.
        /// </remarks>
        public static ClrDataCursor<TResult> Select<TSource, TResult>(ClrDataCursor<TSource> source, Func<TSource, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectCursor<TSource, TResult>(source, selector);
        }

        /// <summary>
        /// <see cref="Select{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> SelectAsync<TSource, TResult>(ValueTask<ClrDataCursor<TSource>> source, Func<TSource, TResult> selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectCursor<TSource, TResult>(await source.ConfigureAwait(false), selector);
        }

        /// <summary>
        /// The cursor of <see cref="Select{TSource, TResult}"/>.
        /// </summary>
        sealed class SelectCursor<TSource, TResult>(ClrDataCursor<TSource> source, Func<TSource, TResult> selector) : ClrDataCursor<TResult>
        {

            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (source.Read() == false)
                    return false;

                current = selector(source.Current);
                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (await source.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    return false;

                current = selector(source.Current);
                return true;
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// Orders rows by a key.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="comparator">Comparison of two keys, or null to compare them naturally.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.orderBy</c> drains its whole input into a <c>TreeMap</c> of lists inside
        /// <c>enumerator()</c>, so the drain is here, at the open, and the cursor handed back is over the
        /// sorted buffer. The sort itself is System.Linq's, which is stable exactly as the map of lists is.
        ///
        /// <para>The comparator is Java's, because that is what <c>PhysType.generateCollationKey</c> yields
        /// and by two different routes: a method call returning one when there is a single collation, and
        /// an anonymous class when there are several.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> OrderBy<TSource, TKey>(ClrDataCursor<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);

            var rows = new List<TSource>();
            try
            {
                while (source.Read())
                    rows.Add(source.Current);
            }
            finally
            {
                source.Dispose();
            }

            return new ListCursor<TSource>(Sorted(rows, keySelector, comparator));
        }

        /// <summary>
        /// <see cref="OrderBy{TSource, TKey}"/>, over an open that awaits. The drain awaits each row, which
        /// is what a cursor lets an open do and an <see cref="IAsyncEnumerable{T}"/> could not: its
        /// <c>GetAsyncEnumerator</c> cannot await, so the enumerable convention's sort had to leave its
        /// drain to the first advance and say so.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> OrderByAsync<TSource, TKey>(ValueTask<ClrDataCursor<TSource>> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(keySelector);

            var cursor = await source.ConfigureAwait(false);

            var rows = new List<TSource>();
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                    rows.Add(cursor.Current);
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return new ListCursor<TSource>(Sorted(rows, keySelector, comparator));
        }

        /// <summary>
        /// Sorts the drained rows, stably, by the key.
        /// </summary>
        static List<TSource> Sorted<TSource, TKey>(List<TSource> rows, Func<TSource, TKey> keySelector, java.util.Comparator? comparator)
        {
            return comparator == null
                ? rows.OrderBy(keySelector).ToList()
                : rows.OrderBy(keySelector, Comparer<TKey>.Create((x, y) => comparator.compare(x, y))).ToList();
        }

        /// <summary>
        /// A cursor over rows already in hand.
        /// </summary>
        /// <remarks>
        /// What a drain, a VALUES and a set operation hand back. Both advances step one index, and there is
        /// nothing here to await or to dispose.
        /// </remarks>
        sealed class ListCursor<TSource>(IReadOnlyList<TSource> rows) : ClrDataCursor<TSource>
        {

            int index = -1;

            /// <inheritdoc />
            public override TSource Current => rows[index];

            /// <inheritdoc />
            public override bool Read()
            {
                if (index + 1 >= rows.Count)
                {
                    index = rows.Count;
                    return false;
                }

                index++;
                return true;
            }

            /// <inheritdoc />
            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                return new ValueTask<bool>(Read());
            }

            /// <inheritdoc />
            public override void Dispose()
            {

            }

        }

        /// <summary>
        /// Bypasses a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.skip(source, BigDecimal)</c>, which is <c>skipWhileBigDecimal</c> over
        /// <c>n &lt; count</c>; the counter holds whatever a FETCH or OFFSET expression evaluated to —
        /// CALCITE-7624, where an <c>int</c> could not. <c>SkipWhileEnumerator</c> takes
        /// <c>source.enumerator()</c> eagerly, and the source arrives opened here.
        /// </remarks>
        public static ClrDataCursor<TSource> Skip<TSource>(ClrDataCursor<TSource> source, java.math.BigDecimal count)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(count);

            return new SkipCursor<TSource>(source, count);
        }

        /// <summary>
        /// <see cref="Skip{TSource}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> SkipAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, java.math.BigDecimal count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(count);

            return new SkipCursor<TSource>(await source.ConfigureAwait(false), count);
        }

        /// <summary>
        /// The cursor of <see cref="Skip{TSource}"/>.
        /// </summary>
        sealed class SkipCursor<TSource>(ClrDataCursor<TSource> source, java.math.BigDecimal count) : ClrDataCursor<TSource>
        {

            java.math.BigDecimal n = java.math.BigDecimal.ZERO;

            /// <inheritdoc />
            public override TSource Current => source.Current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (source.Read())
                {
                    if (n.compareTo(count) < 0)
                    {
                        n = n.add(java.math.BigDecimal.ONE);
                        continue;
                    }

                    return true;
                }

                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (await source.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (n.compareTo(count) < 0)
                    {
                        n = n.add(java.math.BigDecimal.ONE);
                        continue;
                    }

                    return true;
                }

                return false;
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// Takes a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.take(source, BigDecimal)</c>, which is <c>takeWhileBigDecimal</c>, and
        /// its enumerator's <c>moveNext</c> is <c>enumerator.moveNext() &amp;&amp; predicate(current, ++n)</c>:
        /// it has to draw a row before it can test it. So a satisfied fetch has drawn one row more than it
        /// returned, and a count of zero still opens the input and still draws that row. Both are Calcite's
        /// and both are kept.
        /// </remarks>
        public static ClrDataCursor<TSource> Take<TSource>(ClrDataCursor<TSource> source, java.math.BigDecimal count)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(count);

            return new TakeCursor<TSource>(source, count);
        }

        /// <summary>
        /// <see cref="Take{TSource}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> TakeAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, java.math.BigDecimal count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(count);

            return new TakeCursor<TSource>(await source.ConfigureAwait(false), count);
        }

        /// <summary>
        /// The cursor of <see cref="Take{TSource}"/>.
        /// </summary>
        sealed class TakeCursor<TSource>(ClrDataCursor<TSource> source, java.math.BigDecimal count) : ClrDataCursor<TSource>
        {

            java.math.BigDecimal n = java.math.BigDecimal.ZERO;
            bool done;

            /// <inheritdoc />
            public override TSource Current => source.Current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (done)
                    return false;

                if (source.Read() && n.compareTo(count) < 0)
                {
                    n = n.add(java.math.BigDecimal.ONE);
                    return true;
                }

                done = true;
                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (done)
                    return false;

                if (await source.ReadAsync(cancellationToken).ConfigureAwait(false) && n.compareTo(count) < 0)
                {
                    n = n.add(java.math.BigDecimal.ONE);
                    return true;
                }

                done = true;
                return false;
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// Returns the rows of two sources, one after the other, keeping duplicates.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="first">Opens the first source synchronously.</param>
        /// <param name="firstAsync">Opens the first source with await.</param>
        /// <param name="second">Opens the second source synchronously.</param>
        /// <param name="secondAsync">Opens the second source with await.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.concat</c>, which is <c>Linq4j.concat</c> over the two, and the one operator
        /// here whose deferral is linq4j's own: <c>CompositeEnumerable</c>'s <c>enumerator()</c> acquires
        /// nothing, and each source is acquired at its turn inside <c>moveNext</c>. So this takes the
        /// sources as opens rather than as cursors, and runs each when the cursor reaches it.
        ///
        /// <para><b>It takes both opens of each source, and that is the whole point of the design.</b> An
        /// acquisition that happens inside an advance happens inside whichever advance the consumer called,
        /// so the cursor needs the open of that kind: <c>Read</c> reaching the second source calls
        /// <paramref name="second"/> and <c>ReadAsync</c> reaching it calls <paramref name="secondAsync"/>,
        /// with the token that advance was given. Neither the synchronous open of this operator nor the
        /// awaiting one has anything of its own to acquire, which is why <see cref="ConcatAsync{TSource}"/>
        /// completes at once.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> Concat<TSource>(
            Func<ClrDataCursor<TSource>> first,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> firstAsync,
            Func<ClrDataCursor<TSource>> second,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> secondAsync)
        {
            ArgumentNullException.ThrowIfNull(first);
            ArgumentNullException.ThrowIfNull(firstAsync);
            ArgumentNullException.ThrowIfNull(second);
            ArgumentNullException.ThrowIfNull(secondAsync);

            return new ConcatCursor<TSource>([first, second], [firstAsync, secondAsync]);
        }

        /// <summary>
        /// <see cref="Concat{TSource}"/>, over opens that await. Nothing is acquired at this open, so it
        /// completes at once; the sources are acquired inside the advances.
        /// </summary>
        public static ValueTask<ClrDataCursor<TSource>> ConcatAsync<TSource>(
            Func<ClrDataCursor<TSource>> first,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> firstAsync,
            Func<ClrDataCursor<TSource>> second,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> secondAsync,
            CancellationToken cancellationToken)
        {
            return new ValueTask<ClrDataCursor<TSource>>(Concat(first, firstAsync, second, secondAsync));
        }

        /// <summary>
        /// The cursor of <see cref="Concat{TSource}"/>: linq4j's <c>CompositeEnumerable</c>'s enumerator,
        /// with the source opened by the open matching the advance.
        /// </summary>
        sealed class ConcatCursor<TSource>(Func<ClrDataCursor<TSource>>[] opens, Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>>[] opensAsync) : ClrDataCursor<TSource>
        {

            ClrDataCursor<TSource>? current;
            int next;

            /// <inheritdoc />
            public override TSource Current => current is not null ? current.Current : throw new InvalidOperationException("The cursor is not positioned on a row.");

            /// <inheritdoc />
            public override bool Read()
            {
                for (; ; )
                {
                    if (current is not null && current.Read())
                        return true;

                    current?.Dispose();

                    if (next == opens.Length)
                    {
                        current = null;
                        return false;
                    }

                    current = opens[next++]();
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                for (; ; )
                {
                    if (current is not null && await current.ReadAsync(cancellationToken).ConfigureAwait(false))
                        return true;

                    if (current is not null)
                        await current.DisposeAsync().ConfigureAwait(false);

                    if (next == opensAsync.Length)
                    {
                        current = null;
                        return false;
                    }

                    current = await opensAsync[next++](cancellationToken).ConfigureAwait(false);
                }
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                var closing = current;
                current = null;
                closing?.Dispose();
            }

            /// <inheritdoc />
            public override ValueTask DisposeAsync()
            {
                var closing = current;
                current = null;

                return closing?.DisposeAsync() ?? default;
            }

        }

        /// <summary>
        /// Returns the distinct rows of both sources.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other">Opens the second source, which is acquired only once the first has been
        /// drained and closed.</param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        /// <remarks>
        /// Drains both inputs <b>at the open</b>, which is linq4j's own timing: <c>EnumerableDefaults.union</c>
        /// runs <c>source0.into(set)</c> and then <c>source1.into(set)</c> in the method body and returns
        /// <c>Linq4j.asEnumerable(set)</c>. The second is acquired after the first has been drained and
        /// closed, which is why it arrives as an open rather than as a cursor.
        ///
        /// <para>A <c>java.util.HashSet</c>, and not because the CLR has nothing to hold rows in: what a set
        /// operator yields a row in is the order of the collection it held them in, and Calcite's is this
        /// one.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> Union<TSource>(ClrDataCursor<TSource> source, Func<ClrDataCursor<TSource>> other, EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(other);

            var set = new java.util.HashSet();

            try
            {
                while (source.Read())
                    set.add(JavaWrapped.Of(comparer, JavaValues.From(source.Current)));
            }
            finally
            {
                source.Dispose();
            }

            var second = other();
            try
            {
                while (second.Read())
                    set.add(JavaWrapped.Of(comparer, JavaValues.From(second.Current)));
            }
            finally
            {
                second.Dispose();
            }

            return new ListCursor<TSource>(Unwrap<TSource>(set));
        }

        /// <summary>
        /// <see cref="Union{TSource}"/>, over opens that await.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> UnionAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> other, EqualityComparer? comparer, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(other);

            var set = new java.util.HashSet();

            var first = await source.ConfigureAwait(false);
            try
            {
                while (await first.ReadAsync(cancellationToken).ConfigureAwait(false))
                    set.add(JavaWrapped.Of(comparer, JavaValues.From(first.Current)));
            }
            finally
            {
                await first.DisposeAsync().ConfigureAwait(false);
            }

            var second = await other(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await second.ReadAsync(cancellationToken).ConfigureAwait(false))
                    set.add(JavaWrapped.Of(comparer, JavaValues.From(second.Current)));
            }
            finally
            {
                await second.DisposeAsync().ConfigureAwait(false);
            }

            return new ListCursor<TSource>(Unwrap<TSource>(set));
        }

        /// <summary>
        /// Returns the values of a Java collection, in its order, each unwrapped and converted back.
        /// </summary>
        static List<TSource> Unwrap<TSource>(java.lang.Iterable collection)
        {
            var rows = new List<TSource>();
            for (var i = collection.iterator(); i.hasNext();)
                rows.Add(JavaValues.As<TSource>(JavaWrapped.Unwrap(i.next())));

            return rows;
        }

        /// <summary>
        /// Returns a cursor over the rows of an array.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a VALUES clause becomes, which Calcite spells <c>Linq4j.asEnumerable</c>.
        /// </remarks>
        public static ClrDataCursor<TSource> AsCursor<TSource>(TSource[] source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new ListCursor<TSource>(source);
        }

        /// <summary>
        /// <see cref="AsCursor{TSource}(TSource[])"/>, as an open that awaits. There is nothing to await, so
        /// it completes at once.
        /// </summary>
        public static ValueTask<ClrDataCursor<TSource>> AsCursorAsync<TSource>(TSource[] source, CancellationToken cancellationToken)
        {
            return new ValueTask<ClrDataCursor<TSource>>(AsCursor(source));
        }

        /// <summary>
        /// Returns a cursor over a .NET sequence, acquiring its enumerator here.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a scan of an <see cref="Schema.IClrScannableTable"/> or an
        /// <see cref="Schema.IClrQueryableTable"/> becomes: the table hands back a sequence, and
        /// <see cref="IEnumerable{T}.GetEnumerator"/> is where that sequence runs, so it is called at the
        /// open. The cursor's <see cref="ClrDataCursor.ReadAsync"/> completes synchronously, because its
        /// source is pulled.
        /// </remarks>
        public static ClrDataCursor<TSource> AsCursor<TSource>(IEnumerable<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new EnumeratorCursor<TSource>(source.GetEnumerator());
        }

        /// <summary>
        /// Returns a cursor over an asynchronous .NET sequence, acquiring its enumerator here.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken">The token the sequence is enumerated under, which is the only
        /// token an <see cref="IAsyncEnumerable{T}"/> can take; an advance's own token is checked before
        /// each advance and can reach no further.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <see cref="AsCursor{TSource}(IEnumerable{TSource})"/> for the awaiting half of
        /// the table SPI. <c>GetAsyncEnumerator</c> cannot await, so this open completes at once; what is
        /// awaited is each row. The cursor's <see cref="ClrDataCursor.Read"/> blocks for a row with the
        /// context suppressed, which is the cost of a source that can only be awaited and is paid where the
        /// consumer chose to read synchronously.
        /// </remarks>
        public static ValueTask<ClrDataCursor<TSource>> AsCursorAsync<TSource>(IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new ValueTask<ClrDataCursor<TSource>>(new AsyncEnumeratorCursor<TSource>(source.GetAsyncEnumerator(cancellationToken)));
        }

        /// <summary>
        /// Reads a cursor plan as a sequence, opening it at <see cref="IEnumerable{T}.GetEnumerator"/>.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="open">The plan's synchronous open, run once per enumerator.</param>
        /// <returns></returns>
        /// <remarks>
        /// What a converter from the cursor convention into the sequence one builds. The open is taken as a
        /// delegate rather than as a cursor because a sequence acquires at <c>GetEnumerator</c>, and
        /// evaluating an open <em>is</em> the acquisition: an opened cursor handed in would have run the
        /// sub-plan while the enclosing plan was still being built, and once for every enumeration.
        /// </remarks>
        public static IEnumerable<TSource> AsEnumerable<TSource>(Func<ClrDataCursor<TSource>> open)
        {
            ArgumentNullException.ThrowIfNull(open);

            return new ClrEnumerable<TSource>(() => new CursorEnumerator<TSource>(open()));
        }

        /// <summary>
        /// Reads a cursor plan as an asynchronous sequence, opening it on the first advance.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="open">The plan's awaiting open, run once per enumerator.</param>
        /// <param name="cancellationToken">Unused: the token that matters is the one given to
        /// <c>GetAsyncEnumerator</c>, which is the open's and every advance's.</param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="AsEnumerable{TSource}"/> for the awaiting sequence, with the one difference the CLR
        /// imposes: <c>GetAsyncEnumerator</c> cannot await, so an open that awaits has to run inside the
        /// first <c>MoveNextAsync</c>. That is the sanctioned exception the sequence convention states at
        /// every awaited drain, and it is stated here for the same reason.
        /// </remarks>
        public static IAsyncEnumerable<TSource> AsAsyncEnumerable<TSource>(Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> open, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(open);

            return new ClrAsyncEnumerable<TSource>(token => new CursorAsyncEnumerator<TSource>(open, token));
        }

        /// <summary>
        /// A .NET enumerator over an opened cursor.
        /// </summary>
        sealed class CursorEnumerator<TSource>(ClrDataCursor<TSource> cursor) : IEnumerator<TSource>
        {

            /// <inheritdoc />
            public TSource Current => cursor.Current;

            /// <inheritdoc />
            object? System.Collections.IEnumerator.Current => Current;

            /// <inheritdoc />
            public bool MoveNext() => cursor.Read();

            /// <inheritdoc />
            public void Reset() => throw new NotSupportedException();

            /// <inheritdoc />
            public void Dispose() => cursor.Dispose();

        }

        /// <summary>
        /// An asynchronous .NET enumerator over a cursor it opens on its first advance.
        /// </summary>
        sealed class CursorAsyncEnumerator<TSource>(Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> open, CancellationToken cancellationToken) : IAsyncEnumerator<TSource>
        {

            ClrDataCursor<TSource>? cursor;

            /// <inheritdoc />
            public TSource Current => cursor is not null ? cursor.Current : throw new InvalidOperationException("The enumerator is not positioned on a row.");

            /// <inheritdoc />
            public async ValueTask<bool> MoveNextAsync()
            {
                cursor ??= await open(cancellationToken).ConfigureAwait(false);

                return await cursor.ReadAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc />
            public ValueTask DisposeAsync()
            {
                var closing = cursor;
                cursor = null;

                return closing?.DisposeAsync() ?? default;
            }

        }

        /// <summary>
        /// A cursor over a .NET enumerator.
        /// </summary>
        sealed class EnumeratorCursor<TSource>(IEnumerator<TSource> source) : ClrDataCursor<TSource>
        {

            /// <inheritdoc />
            public override TSource Current => source.Current;

            /// <inheritdoc />
            public override bool Read() => source.MoveNext();

            /// <inheritdoc />
            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                return new ValueTask<bool>(source.MoveNext());
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

        }

        /// <summary>
        /// A cursor over an asynchronous .NET enumerator.
        /// </summary>
        sealed class AsyncEnumeratorCursor<TSource>(IAsyncEnumerator<TSource> source) : ClrDataCursor<TSource>
        {

            /// <inheritdoc />
            public override TSource Current => source.Current;

            /// <inheritdoc />
            /// <remarks>
            /// Blocks for the row, with the context suppressed before the advance is called.
            /// </remarks>
            public override bool Read() => ClrDataCursors.BlockRead(this);

            /// <inheritdoc />
            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                // the sequence is enumerating under the token it was opened with, and MoveNextAsync takes
                // none, so this advance's token can stop the read before it starts and no later
                cancellationToken.ThrowIfCancellationRequested();

                return source.MoveNextAsync();
            }

            /// <inheritdoc />
            public override void Dispose() => ClrDataCursors.BlockDispose(source);

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

    }

}
