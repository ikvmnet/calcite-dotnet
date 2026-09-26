using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;

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
    static class ClrDataCursorDefaults
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

        // ---- Aggregate ----


        /// <summary>
        /// Returns the distinct rows of a cursor.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.distinct</c>, which drains its input into a <c>HashSet</c> where it is
        /// called, closes it, and returns <c>Linq4j.asEnumerable(set)</c>; here the call is the open, so the
        /// drain is at the open and the cursor handed back is over the finished set. A
        /// <c>java.util.HashSet</c>, because the order the rows come out in is the set's and Calcite's is
        /// this one.
        /// </remarks>
        public static ClrDataCursor<TSource> Distinct<TSource>(ClrDataCursor<TSource> source, EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(source);

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

            return new ListCursor<TSource>(Unwrap<TSource>(set));
        }

        /// <summary>
        /// <see cref="Distinct{TSource}"/>, over an open that awaits. The drain awaits each row inside the
        /// open, which is what an open can do and a sequence's <c>GetAsyncEnumerator</c> could not.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> DistinctAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, EqualityComparer? comparer, CancellationToken cancellationToken)
        {
            var set = new java.util.HashSet();

            var cursor = await source.ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                    set.add(JavaWrapped.Of(comparer, JavaValues.From(cursor.Current)));
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return new ListCursor<TSource>(Unwrap<TSource>(set));
        }

        /// <summary>
        /// Groups rows by a key and folds each group into one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="accumulatorInitializer"></param>
        /// <param name="accumulatorAdder"></param>
        /// <param name="resultSelector"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.groupBy</c>. The three functions are Calcite's, because
        /// they come from its <c>AggregateLambdaFactory</c> rather than from anything built here. Groups are
        /// returned in the order their keys were first seen, which is what linq4j's own map ordering gives.
        /// <para>The fold runs at the open, because <c>groupBy_</c> drains the input into the map where it is
        /// called and then returns a <c>LookupResultEnumerable</c> over a map that is already finished; the
        /// cursor handed back reads that map, applying the result selector a row at a time as
        /// <c>LookupResultEnumerable</c>'s iterator does.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> GroupBy<TSource, TKey, TResult>(
            ClrDataCursor<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);

            // a java.util.HashMap, because the order the groups come out in is the map's and Calcite's is
            // this one. Holding the insertion order instead gave a different answer to the same GROUP BY.
            var accumulators = new java.util.HashMap();

            try
            {
                while (source.Read())
                {
                    var row = source.Current;
                    var key = JavaWrapped.Of(comparer, JavaValues.From(keySelector(row)));
                    var accumulator = accumulators.get(key) ?? accumulatorInitializer.apply();

                    accumulators.put(key, accumulatorAdder.apply(accumulator, row));
                }
            }
            finally
            {
                source.Dispose();
            }

            return new LookupResultCursor<TResult>(accumulators, resultSelector);
        }

        /// <summary>
        /// <see cref="GroupBy{TSource, TKey, TResult}"/>, over an open that awaits. The fold awaits each row
        /// inside the open, so every group is finished before the cursor is handed back, exactly as
        /// <c>groupBy_</c> finishes its map before it returns.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> GroupByAsync<TSource, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);

            // a java.util.HashMap, for the reason GroupBy gives
            var accumulators = new java.util.HashMap();

            var cursor = await source.ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = cursor.Current;
                    var key = JavaWrapped.Of(comparer, JavaValues.From(keySelector(row)));
                    var accumulator = accumulators.get(key) ?? accumulatorInitializer.apply();

                    accumulators.put(key, accumulatorAdder.apply(accumulator, row));
                }
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return new LookupResultCursor<TResult>(accumulators, resultSelector);
        }

        /// <summary>
        /// Groups the rows by each of several keys at once, folding each group into an accumulator.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelectors">One selector per grouping set.</param>
        /// <param name="accumulatorInitializer"></param>
        /// <param name="accumulatorAdder"></param>
        /// <param name="resultSelector"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.groupByMultiple</c>, which exists to support
        /// <c>GROUPING SETS</c> and has no counterpart on <c>Enumerable</c>. Every row is offered to every
        /// selector, so one pass folds it into one group per grouping set; the keys of two sets never collide
        /// because each carries an indicator per field saying which set it came from.
        /// <para>The fold runs at the open, for the reason <see cref="GroupBy{TSource, TKey, TResult}"/>
        /// gives: <c>groupByMultiple_</c> drains where it is called and returns a
        /// <c>LookupResultEnumerable</c> over a finished map.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> GroupByMultiple<TSource, TKey, TResult>(
            ClrDataCursor<TSource> source,
            Func<TSource, TKey>[] keySelectors,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelectors);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);

            // a java.util.HashMap, for the reason GroupBy gives: the order the groups come out in is the
            // map's, and Calcite's map is this one
            var accumulators = new java.util.HashMap();

            try
            {
                while (source.Read())
                {
                    var row = source.Current;

                    foreach (var keySelector in keySelectors)
                    {
                        var key = JavaWrapped.Of(comparer, JavaValues.From(keySelector(row)));
                        var accumulator = accumulators.get(key) ?? accumulatorInitializer.apply();

                        accumulators.put(key, accumulatorAdder.apply(accumulator, row));
                    }
                }
            }
            finally
            {
                source.Dispose();
            }

            return new LookupResultCursor<TResult>(accumulators, resultSelector);
        }

        /// <summary>
        /// <see cref="GroupByMultiple{TSource, TKey, TResult}"/>, over an open that awaits. The fold awaits
        /// each row inside the open, so every group is finished before the cursor is handed back.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> GroupByMultipleAsync<TSource, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> source,
            Func<TSource, TKey>[] keySelectors,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(keySelectors);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);

            // a java.util.HashMap, for the reason GroupBy gives
            var accumulators = new java.util.HashMap();

            var cursor = await source.ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = cursor.Current;

                    foreach (var keySelector in keySelectors)
                    {
                        var key = JavaWrapped.Of(comparer, JavaValues.From(keySelector(row)));
                        var accumulator = accumulators.get(key) ?? accumulatorInitializer.apply();

                        accumulators.put(key, accumulatorAdder.apply(accumulator, row));
                    }
                }
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return new LookupResultCursor<TResult>(accumulators, resultSelector);
        }

        /// <summary>
        /// A cursor over a finished map of accumulators, applying the result selector a row at a time.
        /// </summary>
        /// <remarks>
        /// linq4j's <c>LookupResultEnumerable</c>'s iterator: the map is walked in its own order and the
        /// selector is applied in <c>next()</c>, not before. The input was drained and closed at the open,
        /// so there is nothing here to dispose, and nothing to await.
        /// </remarks>
        sealed class LookupResultCursor<TResult>(java.util.Map map, Function2 resultSelector) : ClrDataCursor<TResult>
        {

            readonly java.util.Iterator iterator = map.entrySet().iterator();
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (iterator.hasNext() == false)
                    return false;

                var entry = (java.util.Map.Entry)iterator.next();
                current = JavaValues.As<TResult>(resultSelector.apply(JavaWrapped.Unwrap(entry.getKey()), entry.getValue()));
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
        /// Aggregates an input that already arrives grouped, by walking it once.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="accumulatorInitializer"></param>
        /// <param name="accumulatorAdder"></param>
        /// <param name="resultSelector"></param>
        /// <param name="comparator">Decides where one group ends and the next begins.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.sortedGroupBy</c> and its
        /// <c>SortedAggregateEnumerator</c>. Nothing is held but the accumulator of the group being read,
        /// which is the whole point of it against <see cref="GroupBy{TSource, TKey, TResult}"/>, and the
        /// groups come out in the order the input was sorted in rather than a map's.
        /// <para><c>SortedAggregateEnumerator</c>'s constructor acquires <c>enumerable.enumerator()</c>, and
        /// the source arrives opened here, which is the same moment; the walk is in the advances.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> SortedGroupBy<TSource, TKey, TResult>(
            ClrDataCursor<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            java.util.Comparator comparator)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(comparator);

            return new SortedAggregateCursor<TSource, TKey, TResult>(source, keySelector, accumulatorInitializer, accumulatorAdder, resultSelector, comparator);
        }

        /// <summary>
        /// <see cref="SortedGroupBy{TSource, TKey, TResult}"/>, over an open that awaits. Nothing is read at
        /// the open; the walk is in the advances.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> SortedGroupByAsync<TSource, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            java.util.Comparator comparator,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(comparator);

            return new SortedAggregateCursor<TSource, TKey, TResult>(await source.ConfigureAwait(false), keySelector, accumulatorInitializer, accumulatorAdder, resultSelector, comparator);
        }

        /// <summary>
        /// The cursor of <see cref="SortedGroupBy{TSource, TKey, TResult}"/>: linq4j's
        /// <c>SortedAggregateEnumerator</c>, with its <c>moveNext</c> written twice over one set of fields.
        /// </summary>
        /// <remarks>
        /// An advance folds the row the source is positioned on — the first row, or the row that ended the
        /// previous group — and then reads on until the key changes or the input ends. Where the key changes
        /// the group's result is taken, the accumulator is made afresh, and the row that changed it is left
        /// at the source's position for the next advance; where the input ends the accumulator is dropped,
        /// which is how the next advance knows there is nothing more. linq4j tells whether a round produced
        /// its result at the key change by testing the result for null; that cannot be written on a
        /// <typeparamref name="TResult"/>, so it is a flag here.
        /// </remarks>
        sealed class SortedAggregateCursor<TSource, TKey, TResult>(
            ClrDataCursor<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            java.util.Comparator comparator) : ClrDataCursor<TResult>
        {

            bool isInitialized;
            bool isLastMoveNextFalse;
            object? curAccumulator;
            TResult curResult = default!;

            /// <inheritdoc />
            public override TResult Current => isLastMoveNextFalse ? throw new InvalidOperationException("The cursor is not positioned on a row.") : curResult;

            /// <inheritdoc />
            public override bool Read()
            {
                if (isInitialized == false)
                {
                    isInitialized = true;

                    // input is empty
                    if (source.Read() == false)
                    {
                        isLastMoveNextFalse = true;
                        return false;
                    }
                }
                else if (curAccumulator == null)
                {
                    // input has been exhausted
                    isLastMoveNextFalse = true;
                    return false;
                }

                // linq4j assumes the adder never answers null, and says so
                curAccumulator ??= accumulatorInitializer.apply();

                var haveResult = false;
                var row = source.Current;
                var prevKey = JavaValues.From(keySelector(row));
                curAccumulator = accumulatorAdder.apply(curAccumulator, row);

                while (source.Read())
                {
                    row = source.Current;
                    var curKey = JavaValues.From(keySelector(row));

                    if (comparator.compare(prevKey, curKey) != 0)
                    {
                        // the key changed: the group's result is taken and the accumulator is made afresh
                        // for the row that changed it, which the next advance reads from the source
                        curResult = JavaValues.As<TResult>(resultSelector.apply(prevKey, curAccumulator));
                        haveResult = true;
                        curAccumulator = accumulatorInitializer.apply();
                        break;
                    }

                    curAccumulator = accumulatorAdder.apply(curAccumulator, row);
                    prevKey = curKey;
                }

                if (haveResult == false)
                {
                    // the last key: nothing is kept for it
                    curResult = JavaValues.As<TResult>(resultSelector.apply(prevKey, curAccumulator));
                    curAccumulator = null;
                }

                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (isInitialized == false)
                {
                    isInitialized = true;

                    // input is empty
                    if (await source.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    {
                        isLastMoveNextFalse = true;
                        return false;
                    }
                }
                else if (curAccumulator == null)
                {
                    // input has been exhausted
                    isLastMoveNextFalse = true;
                    return false;
                }

                // linq4j assumes the adder never answers null, and says so
                curAccumulator ??= accumulatorInitializer.apply();

                var haveResult = false;
                var row = source.Current;
                var prevKey = JavaValues.From(keySelector(row));
                curAccumulator = accumulatorAdder.apply(curAccumulator, row);

                while (await source.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    row = source.Current;
                    var curKey = JavaValues.From(keySelector(row));

                    if (comparator.compare(prevKey, curKey) != 0)
                    {
                        // the key changed: the group's result is taken and the accumulator is made afresh
                        // for the row that changed it, which the next advance reads from the source
                        curResult = JavaValues.As<TResult>(resultSelector.apply(prevKey, curAccumulator));
                        haveResult = true;
                        curAccumulator = accumulatorInitializer.apply();
                        break;
                    }

                    curAccumulator = accumulatorAdder.apply(curAccumulator, row);
                    prevKey = curKey;
                }

                if (haveResult == false)
                {
                    // the last key: nothing is kept for it
                    curResult = JavaValues.As<TResult>(resultSelector.apply(prevKey, curAccumulator));
                    curAccumulator = null;
                }

                return true;
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// Folds every row into one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="seed"></param>
        /// <param name="accumulatorAdder"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.aggregate</c>, which is what a query with aggregate calls
        /// and no GROUP BY becomes. It answers the row rather than a cursor, as linq4j's answers the value:
        /// the fold runs where it is called, which here is the open, and <see cref="Singleton{TSource}"/>
        /// makes the row a cursor.
        /// </remarks>
        public static TResult Aggregate<TSource, TResult>(ClrDataCursor<TSource> source, object seed, Function2 accumulatorAdder, Function1 resultSelector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);

            var accumulator = seed;

            try
            {
                while (source.Read())
                    accumulator = accumulatorAdder.apply(accumulator, source.Current);
            }
            finally
            {
                source.Dispose();
            }

            return JavaValues.As<TResult>(resultSelector.apply(accumulator));
        }

        /// <summary>
        /// Returns a cursor of one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="element"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Linq4j.singletonEnumerable</c>.
        /// </remarks>
        public static ClrDataCursor<TSource> Singleton<TSource>(TSource element)
        {
            return new ListCursor<TSource>([element]);
        }

        /// <summary>
        /// Folds every row into one, over an open that awaits, and returns the cursor of that one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="seed"></param>
        /// <param name="accumulatorAdder"></param>
        /// <param name="resultSelector"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(Aggregate(source, …))</c> as one operator. The synchronous body composes the two as
        /// nested calls in the tree, because <see cref="Aggregate{TSource, TResult}"/> answers the row and
        /// the tree hands it on; here the fold has to be awaited and an expression tree cannot await, so
        /// the composition is an operator rather than a tree.
        ///
        /// <para>It folds at the open, once, exactly as the pair it stands in for does and as Calcite's
        /// generated block folds once at bind: an open that awaits can await the fold, so nothing is left to
        /// the first advance and nothing has to be remembered for a second one.</para>
        /// </remarks>
        public static async ValueTask<ClrDataCursor<TResult>> SingletonAggregateAsync<TSource, TResult>(
            ValueTask<ClrDataCursor<TSource>> source,
            object seed,
            Function2 accumulatorAdder,
            Function1 resultSelector,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(accumulatorAdder);
            ArgumentNullException.ThrowIfNull(resultSelector);

            var accumulator = seed;

            var cursor = await source.ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                    accumulator = accumulatorAdder.apply(accumulator, cursor.Current);
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return Singleton(JavaValues.As<TResult>(resultSelector.apply(accumulator)));
        }

        // ---- Collect ----
        // The operators a collect, an uncollect and a combine are built from.


        /// <summary>
        /// Reads every row into a Java list, closing the cursor once it is read.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.toList</c>, which is <c>source.into(new ArrayList())</c>: a drain in the
        /// method body, so it drains here, where the tree is evaluated. A <c>java.util.List</c>, because
        /// this is a value in a row and the reader of that row is Calcite's.
        /// </remarks>
        public static java.util.List ToJavaList<TSource>(ClrDataCursor<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            var list = new java.util.ArrayList();

            try
            {
                while (source.Read())
                    list.add(source.Current);
            }
            finally
            {
                source.Dispose();
            }

            return list;
        }

        /// <summary>
        /// Reads every row into a Java map, keeping the order the keys were seen in, and closes the cursor
        /// once it is read.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="valueSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.toMap</c>, which drains inside a <c>try</c> over the enumerator into a
        /// <c>LinkedHashMap</c>, so that the order the rows arrived in is the order the map keeps.
        /// </remarks>
        public static java.util.Map ToJavaMap<TSource>(ClrDataCursor<TSource> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(valueSelector);

            var map = new java.util.LinkedHashMap();

            try
            {
                while (source.Read())
                    map.put(keySelector(source.Current), valueSelector(source.Current));
            }
            finally
            {
                source.Dispose();
            }

            return map;
        }

        /// <summary>
        /// Returns each row of each sequence a function yields.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector">Yields a linq4j sequence for one row, which is what Calcite builds here.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.selectMany</c>, whose enumerator acquires the source in a field initializer
        /// at <c>enumerator()</c> — the source arrives opened here, which is the same moment — and builds and
        /// acquires each row's sequence at its turn, inside <c>moveNext</c>.
        /// </remarks>
        public static ClrDataCursor<TResult> SelectMany<TSource, TResult>(ClrDataCursor<TSource> source, Function1 selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectManyCursor<TSource, TResult>(source, selector);
        }

        /// <summary>
        /// The cursor of <see cref="SelectMany{TSource, TResult}"/>: linq4j's <c>selectMany</c> enumerator,
        /// with the row's sequence read through <see cref="JavaCursors.FromJava{TSource}"/>.
        /// </summary>
        /// <remarks>
        /// The inner sequence is linq4j's, produced for one row by a generator of Calcite's, and it is
        /// pulled whichever advance reaches it: it is a value already in hand rather than a source, so
        /// nothing about reading it can suspend, and the cursor it is read through completes its
        /// <see cref="ClrDataCursor.ReadAsync"/> synchronously for that reason. A null is where linq4j
        /// holds <c>Linq4j.emptyEnumerator()</c> before the first row and between one row's sequence and
        /// the next.
        /// </remarks>
        sealed class SelectManyCursor<TSource, TResult>(ClrDataCursor<TSource> source, Function1 selector) : ClrDataCursor<TResult>
        {

            ClrDataCursor<TResult>? result;

            /// <inheritdoc />
            public override TResult Current => result is not null ? result.Current : throw new InvalidOperationException("The cursor is not positioned on a row.");

            /// <inheritdoc />
            public override bool Read()
            {
                for (; ; )
                {
                    if (result is not null)
                    {
                        if (result.Read())
                            return true;

                        result.Dispose();
                        result = null;
                    }

                    if (source.Read() == false)
                        return false;

                    result = JavaCursors.FromJava<TResult>((org.apache.calcite.linq4j.Enumerable)selector.apply(source.Current));
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                for (; ; )
                {
                    if (result is not null)
                    {
                        if (await result.ReadAsync(cancellationToken).ConfigureAwait(false))
                            return true;

                        await result.DisposeAsync().ConfigureAwait(false);
                        result = null;
                    }

                    if (await source.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                        return false;

                    result = JavaCursors.FromJava<TResult>((org.apache.calcite.linq4j.Enumerable)selector.apply(source.Current));
                }
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                source.Dispose();

                var closing = result;
                result = null;
                closing?.Dispose();
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                await source.DisposeAsync().ConfigureAwait(false);

                var closing = result;
                result = null;
                if (closing is not null)
                    await closing.DisposeAsync().ConfigureAwait(false);
            }

        }

        /// <summary>
        /// Reads a Java list as a cursor.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Linq4j.asEnumerable(List)</c>, read through the crossing every linq4j sequence is read
        /// through, so each value is converted rather than cast for the reason
        /// <see cref="JavaSequences.FromJava{TSource}"/> gives.
        /// </remarks>
        public static ClrDataCursor<TSource> FromJavaList<TSource>(java.util.List source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return JavaCursors.FromJava<TSource>(org.apache.calcite.linq4j.Linq4j.asEnumerable(source));
        }

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="SelectMany{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> SelectManyAsync<TSource, TResult>(ValueTask<ClrDataCursor<TSource>> source, Function1 selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectManyCursor<TSource, TResult>(await source.ConfigureAwait(false), selector);
        }

        /// <summary>
        /// Reads a whole cursor into a Java list and hands back the one row holding it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(ToJavaList(source))</c> as one operator, because the drain has to be awaited and an
        /// expression tree cannot await: the composition the synchronous body writes as two nested calls
        /// cannot be written as a tree here, so it is written as an operator.
        ///
        /// <para>The drain is at the open, as the synchronous pair's is, which is what an awaiting open can
        /// do and an <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> could not: its
        /// <c>GetAsyncEnumerator</c> cannot await, so the sequence convention's operator had to fold on the
        /// first advance and keep the row for every later enumeration. Nothing here is deferred and nothing
        /// is kept — the open awaits the drain and hands back a cursor over the one row.</para>
        /// </remarks>
        public static async ValueTask<ClrDataCursor<java.util.List>> SingletonJavaListAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, CancellationToken cancellationToken)
        {
            return Singleton(await ToJavaListAsync(source, cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Reads a whole cursor into a Java map and hands back the one row holding it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="valueSelector"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(ToJavaMap(source, …))</c> as one operator, for the reason
        /// <see cref="SingletonJavaListAsync{TSource}"/> gives, and draining at the open as that does. A
        /// <c>LinkedHashMap</c>, so that the order the rows arrived in is the order the map keeps.
        /// </remarks>
        public static async ValueTask<ClrDataCursor<java.util.Map>> SingletonJavaMapAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(valueSelector);

            var map = new java.util.LinkedHashMap();

            var cursor = await source.ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                    map.put(keySelector(cursor.Current), valueSelector(cursor.Current));
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return Singleton<java.util.Map>(map);
        }

        /// <summary>
        /// Reads a whole cursor into a Java list, awaiting each row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ToJavaList{TSource}"/>, over an open that awaits. Not named in
        /// <see cref="ClrDataCursorBuiltInMethod"/>: no plan calls it, because an expression tree cannot
        /// await what it returns. It is what the operators that have to read everything before they can
        /// hand back a row drain with.
        /// </remarks>
        public static async ValueTask<java.util.List> ToJavaListAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, CancellationToken cancellationToken)
        {
            var list = new java.util.ArrayList();

            var cursor = await source.ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                    list.add(cursor.Current);
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return list;
        }

        /// <summary>
        /// Opens and reads each input into a Java list, one after another, combines the lists, and hands
        /// back a cursor over the rows that come back.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="sources">Opens each input, called at its turn.</param>
        /// <param name="combine">What to do with the lists once they are all read, which is
        /// <c>SqlFunctions.combineQueryResults</c>.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// The synchronous node reads each input into a list inside the tree and passes the lists to the
        /// combine; here each read has to be awaited and an expression tree cannot await, so the reading
        /// moves into this operator, for the reason <see cref="SingletonJavaListAsync{TSource}"/> gives.
        ///
        /// <para><b>The inputs arrive as opens rather than opened, because an awaiting open is eager where
        /// the tree it stands in is not.</b> Calcite's generated <c>bind</c> reads <c>list0</c> to completion
        /// before <c>child1</c> is touched, and the synchronous body does the same by construction: each
        /// <c>ToJavaList</c> in the array initializer runs to completion before the next element's open is
        /// evaluated. An array of awaiting opens would have started every input before the first was
        /// drained, so each is opened here, after the one before it has been read and closed.</para>
        ///
        /// <para>The combining itself is not this operator's business and arrives as a delegate, so that
        /// which function Calcite combines with stays the node's decision, as it is in the synchronous
        /// body.</para>
        /// </remarks>
        public static async ValueTask<ClrDataCursor<TResult>> CombineQueryResultsAsync<TResult>(
            Func<CancellationToken, ValueTask<ClrDataCursor<java.util.Map>>>[] sources,
            Func<java.util.List[], java.util.List> combine,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(sources);
            ArgumentNullException.ThrowIfNull(combine);

            var lists = new java.util.List[sources.Length];
            for (int i = 0; i < sources.Length; i++)
                lists[i] = await ToJavaListAsync(sources[i](cancellationToken), cancellationToken).ConfigureAwait(false);

            return FromJavaList<TResult>(combine(lists));
        }

        /// <summary>
        /// <see cref="FromJavaList{TSource}"/>, as an open that awaits. The list is a value already in
        /// hand, so there is nothing to await and it completes at once.
        /// </summary>
        public static ValueTask<ClrDataCursor<TSource>> FromJavaListAsync<TSource>(java.util.List source, CancellationToken cancellationToken)
        {
            return new ValueTask<ClrDataCursor<TSource>>(FromJavaList<TSource>(source));
        }

        // ---- Correlate ----


        /// <summary>
        /// Joins each row of a cursor to the rows a function of it opens.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the cursor for one outer row synchronously, which is what makes the join
        /// correlated.</param>
        /// <param name="innerAsync">Opens the cursor for one outer row with await.</param>
        /// <param name="resultSelector"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.correlateJoin</c>, whose enumerator acquires the outer in
        /// a field initializer at <c>enumerator()</c> — the outer arrives opened here, which is the same
        /// moment — and runs the inner for each outer row inside <c>moveNext</c>. That acquisition happens
        /// inside whichever advance the consumer called, so the inner arrives as both opens and the cursor
        /// calls the one of the advance's kind, with the token that advance was given.
        ///
        /// <para>Two things it does before any row moves. RIGHT and FULL are refused -- a correlated join has
        /// no right side to drive -- and the refusal happens where the cursor is built rather than where it
        /// is read. And a correlated function that answers null is read as an empty cursor rather than
        /// dereferenced; Calcite writes <c>Linq4j.emptyEnumerable()</c> for it, and
        /// <see cref="CorrelateLeftMarkJoin"/> next door already guarded it.</para>
        ///
        /// <para>The null right row a SEMI join emits is Calcite's too, and for a subtler reason than it
        /// looks: its enumerator returns without assigning <c>innerValue</c>, so <c>current()</c> reads
        /// whatever was there. For a join that is SEMI throughout, that is null every time.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> CorrelateJoin<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(joinType);

            var name = joinType.name();

            if (name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL))
                throw new ArgumentException($"JoinType {name} is not valid for correlation");

            return new CorrelateJoinCursor<TSource, TInner, TResult>(outer, inner, innerAsync, resultSelector, joinType);
        }

        /// <summary>
        /// <see cref="CorrelateJoin{TSource, TInner, TResult}"/>, over an outer open that awaits. The
        /// refusal of RIGHT and FULL comes before the outer is awaited, so that it happens where
        /// <c>correlateJoin</c> makes it: before anything is acquired for the join itself.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> CorrelateJoinAsync<TSource, TInner, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(joinType);

            var name = joinType.name();

            if (name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL))
                throw new ArgumentException($"JoinType {name} is not valid for correlation");

            return new CorrelateJoinCursor<TSource, TInner, TResult>(await outer.ConfigureAwait(false), inner, innerAsync, resultSelector, joinType);
        }

        /// <summary>
        /// The cursor of <see cref="CorrelateJoin{TSource, TInner, TResult}"/>: <c>correlateJoin</c>'s
        /// enumerator, state for state.
        /// </summary>
        /// <remarks>
        /// State 0 is moving the outer and state 1 is moving the inner, as linq4j numbers them. The previous
        /// inner is closed before the next is opened, which is linq4j's order too, the difference being that
        /// opening here is the acquisition <c>enumerator()</c> was there.
        /// </remarks>
        sealed class CorrelateJoinCursor<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType) : ClrDataCursor<TResult>
        {

            readonly bool semi = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            readonly bool anti = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            readonly bool nullsOnRight = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.LEFT);

            ClrDataCursor<TInner>? innerCursor;
            TSource? outerValue;
            TInner? innerValue;
            int state; // 0 -- moving outer, 1 moving inner
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (true)
                {
                    switch (state)
                    {
                        case 0:
                            // move outer
                            if (outer.Read() == false)
                                return false;

                            outerValue = outer.Current;

                            // initial move inner: the previous inner is closed before the next is opened, and
                            // a function that answers null is read as empty
                            innerCursor?.Dispose();
                            innerCursor = inner(outerValue);

                            if (innerCursor != null && innerCursor.Read())
                            {
                                if (anti)
                                {
                                    // for anti-join need to try next outer row; current does not match
                                    continue;
                                }

                                if (semi)
                                {
                                    // current row matches, and innerValue is left as it was
                                    current = resultSelector(outerValue, innerValue);
                                    return true;
                                }

                                // INNER and LEFT just return result
                                innerValue = innerCursor.Current;
                                state = 1; // iterate over inner results
                                current = resultSelector(outerValue, innerValue);
                                return true;
                            }

                            // no match detected
                            innerValue = default;

                            if (nullsOnRight || anti)
                            {
                                current = resultSelector(outerValue, innerValue);
                                return true;
                            }

                            // for INNER and SEMI need to find another outer row
                            continue;

                        case 1:
                            // subsequent move inner
                            if (innerCursor!.Read())
                            {
                                innerValue = innerCursor.Current;
                                current = resultSelector(outerValue, innerValue);
                                return true;
                            }

                            state = 0;
                            // continue loop, move outer
                            break;
                    }
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    switch (state)
                    {
                        case 0:
                            if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                                return false;

                            outerValue = outer.Current;

                            if (innerCursor != null)
                                await innerCursor.DisposeAsync().ConfigureAwait(false);
                            innerCursor = await innerAsync(outerValue, cancellationToken).ConfigureAwait(false);

                            if (innerCursor != null && await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                            {
                                if (anti)
                                    continue;

                                if (semi)
                                {
                                    current = resultSelector(outerValue, innerValue);
                                    return true;
                                }

                                innerValue = innerCursor.Current;
                                state = 1;
                                current = resultSelector(outerValue, innerValue);
                                return true;
                            }

                            innerValue = default;

                            if (nullsOnRight || anti)
                            {
                                current = resultSelector(outerValue, innerValue);
                                return true;
                            }

                            continue;

                        case 1:
                            if (await innerCursor!.ReadAsync(cancellationToken).ConfigureAwait(false))
                            {
                                innerValue = innerCursor.Current;
                                current = resultSelector(outerValue, innerValue);
                                return true;
                            }

                            state = 0;
                            break;
                    }
                }
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                outer.Dispose();

                var closing = innerCursor;
                innerCursor = null;
                innerValue = default;
                closing?.Dispose();

                outerValue = default;
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                await outer.DisposeAsync().ConfigureAwait(false);

                var closing = innerCursor;
                innerCursor = null;
                innerValue = default;
                if (closing != null)
                    await closing.DisposeAsync().ConfigureAwait(false);

                outerValue = default;
            }

        }

        /// <summary>
        /// Returns every left row with a marker saying whether its own right side had a match.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the right rows for one left row synchronously.</param>
        /// <param name="innerAsync">Opens the right rows for one left row with await.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.correlateLeftMarkJoin</c>, which is
        /// <c>leftMarkJoinInternal</c> over a correlated inner: the outer is acquired in a field
        /// initializer at <c>enumerator()</c> and arrives opened here, and each right side is opened, read
        /// and closed at its left row's turn inside <c>moveNext</c> — by the open of the advance that
        /// reached it, which is why both opens arrive.
        ///
        /// <para>The marker is three-valued and the order it is resolved in matters: false until something
        /// is found, null if any comparison was unknown, and true on the first match, which stops the scan.
        /// So an unknown seen before a match is discarded, and one seen when there is no match is kept —
        /// which is what makes <c>IN</c> over a nullable column answer UNKNOWN rather than FALSE.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> CorrelateLeftMarkJoin<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(resultSelector);

            return new CorrelateLeftMarkJoinCursor<TSource, TInner, TResult>(outer, inner, innerAsync, predicate, resultSelector);
        }

        /// <summary>
        /// <see cref="CorrelateLeftMarkJoin{TSource, TInner, TResult}"/>, over an outer open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> CorrelateLeftMarkJoinAsync<TSource, TInner, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(resultSelector);

            return new CorrelateLeftMarkJoinCursor<TSource, TInner, TResult>(await outer.ConfigureAwait(false), inner, innerAsync, predicate, resultSelector);
        }

        /// <summary>
        /// The cursor of <see cref="CorrelateLeftMarkJoin{TSource, TInner, TResult}"/>:
        /// <c>leftMarkJoinInternal</c>'s enumerator.
        /// </summary>
        sealed class CorrelateLeftMarkJoinCursor<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector) : ClrDataCursor<TResult>
        {

            java.lang.Boolean? marker = java.lang.Boolean.FALSE;
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (outer.Read() == false)
                    return false;

                marker = java.lang.Boolean.FALSE;
                var outerRow = outer.Current;

                // opened, read and closed at this row's turn, as the try-with-resources in linq4j is
                var inners = inner(outerRow);
                if (inners != null)
                {
                    try
                    {
                        while (inners.Read())
                        {
                            var matched = predicate(outerRow, inners.Current);

                            if (matched == null)
                                marker = null;
                            else if (matched.booleanValue())
                            {
                                marker = java.lang.Boolean.TRUE;
                                break;
                            }
                        }
                    }
                    finally
                    {
                        inners.Dispose();
                    }
                }

                current = resultSelector(outerRow, marker);
                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    return false;

                marker = java.lang.Boolean.FALSE;
                var outerRow = outer.Current;

                var inners = await innerAsync(outerRow, cancellationToken).ConfigureAwait(false);
                if (inners != null)
                {
                    try
                    {
                        while (await inners.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            var matched = predicate(outerRow, inners.Current);

                            if (matched == null)
                                marker = null;
                            else if (matched.booleanValue())
                            {
                                marker = java.lang.Boolean.TRUE;
                                break;
                            }
                        }
                    }
                    finally
                    {
                        await inners.DisposeAsync().ConfigureAwait(false);
                    }
                }

                current = resultSelector(outerRow, marker);
                return true;
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Joins by running the right input once per batch of left rows, rather than once per row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="joinType"></param>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the right rows for a batch of left rows synchronously.</param>
        /// <param name="innerAsync">Opens the right rows for a batch of left rows with await.</param>
        /// <param name="resultSelector"></param>
        /// <param name="predicate"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.correlateBatchJoin</c>. The right input is a filter over
        /// a disjunction of the batch's conditions, so one pass of it serves every row of the batch. The
        /// outer is acquired in a field initializer at <c>enumerator()</c> and arrives opened here; each
        /// batch's right side is opened at that batch's turn inside <c>moveNext</c>, by the open of the
        /// advance that reached it.
        ///
        /// <para>It is read the way Calcite reads it: the batch's <em>first</em> left row pulls from it and
        /// caches each row as it goes, and every left row after that reads the cache. So the right input is
        /// never read further than the first left row needed — which is what a LIMIT above the join asks
        /// for, and is the only thing this shape buys over materialising it up front. The one place the
        /// first row would stop early is a semi or anti join finding its match, and there it finishes reading
        /// first, because the rest of the batch reads what it cached. Calcite does exactly that, for exactly
        /// that reason.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> CorrelateBatchJoin<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            ClrDataCursor<TSource> outer,
            Func<java.util.List, ClrDataCursor<TInner>?> inner,
            Func<java.util.List, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            int batchSize)
        {
            ArgumentNullException.ThrowIfNull(joinType);
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(predicate);

            return new CorrelateBatchJoinCursor<TSource, TInner, TResult>(joinType, outer, inner, innerAsync, resultSelector, predicate, batchSize);
        }

        /// <summary>
        /// <see cref="CorrelateBatchJoin{TSource, TInner, TResult}"/>, over an outer open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> CorrelateBatchJoinAsync<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<java.util.List, ClrDataCursor<TInner>?> inner,
            Func<java.util.List, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            int batchSize,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(joinType);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(predicate);

            return new CorrelateBatchJoinCursor<TSource, TInner, TResult>(joinType, await outer.ConfigureAwait(false), inner, innerAsync, resultSelector, predicate, batchSize);
        }

        /// <summary>
        /// The cursor of <see cref="CorrelateBatchJoin{TSource, TInner, TResult}"/>:
        /// <c>correlateBatchJoin</c>'s enumerator, field for field.
        /// </summary>
        /// <remarks>
        /// <c>i</c> is the position in the batch and <c>j</c> the position in the cached right rows, as
        /// linq4j names them. The first left row of a batch reads the right cursor and every other reads
        /// the cache, and the right's first row is drawn as soon as the batch is opened, which is what lets
        /// a batch with no right rows be skipped whole for a SEMI or an INNER join.
        ///
        /// <para>A short batch is padded by repeating its first row, as Calcite pads it: the condition is a
        /// disjunction, so a row that repeats adds nothing to it.</para>
        /// </remarks>
        sealed class CorrelateBatchJoinCursor<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            ClrDataCursor<TSource> outer,
            Func<java.util.List, ClrDataCursor<TInner>?> inner,
            Func<java.util.List, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            int batchSize) : ClrDataCursor<TResult>
        {

            readonly bool isSemi = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            readonly bool isAnti = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            readonly bool isLeft = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.LEFT);
            readonly bool isInner = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.INNER);

            readonly List<TSource> outerValues = new(batchSize);
            readonly List<TInner> innerValues = [];
            TSource? outerValue;
            TInner? innerValue;
            ClrDataCursor<TInner>? innerCursor;
            bool innerEnumHasNext;
            bool atLeastOneResult;
            int i = -1; // outer position
            int j = -1; // inner position
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (true)
                {
                    // fetch a new batch
                    if (i == outerValues.Count || i == -1)
                    {
                        i = 0;
                        j = 0;
                        outerValues.Clear();
                        innerValues.Clear();

                        while (outerValues.Count < batchSize && outer.Read())
                            outerValues.Add(outer.Current);

                        if (outerValues.Count == 0)
                            return false;

                        CloseInner();
                        innerCursor = inner(Padded());
                        innerEnumHasNext = innerCursor != null && innerCursor.Read();

                        // if no inner values skip the whole batch in case of SEMI and INNER join
                        if (innerEnumHasNext == false && (isSemi || isInner))
                        {
                            i = outerValues.Count;
                            continue;
                        }
                    }

                    if (InnerHasNext())
                    {
                        outerValue = outerValues[i]; // get current outer value
                        NextInnerValue();

                        // compare current block row to current inner value
                        if (predicate(outerValue!, innerValue!))
                        {
                            atLeastOneResult = true;

                            // skip the rest of inner values in case of ANTI and SEMI when a match is found
                            if (isAnti || isSemi)
                            {
                                // two ways of skipping inner values, cursor way and cache way
                                if (i == 0)
                                {
                                    while (innerEnumHasNext)
                                    {
                                        innerValues.Add(innerCursor!.Current);
                                        innerEnumHasNext = innerCursor.Read();
                                    }
                                }
                                else
                                {
                                    j = innerValues.Count;
                                }

                                if (isAnti)
                                    continue;
                            }

                            current = resultSelector(outerValue, innerValue);
                            return true;
                        }
                    }
                    else
                    {
                        // end of inner
                        if (atLeastOneResult == false && (isLeft || isAnti))
                        {
                            outerValue = outerValues[i]; // get current outer value
                            innerValue = default;
                            NextOuterValue();
                            current = resultSelector(outerValue, innerValue);
                            return true;
                        }

                        NextOuterValue();
                    }
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    if (i == outerValues.Count || i == -1)
                    {
                        i = 0;
                        j = 0;
                        outerValues.Clear();
                        innerValues.Clear();

                        while (outerValues.Count < batchSize && await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                            outerValues.Add(outer.Current);

                        if (outerValues.Count == 0)
                            return false;

                        await CloseInnerAsync().ConfigureAwait(false);
                        innerCursor = await innerAsync(Padded(), cancellationToken).ConfigureAwait(false);
                        innerEnumHasNext = innerCursor != null && await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false);

                        if (innerEnumHasNext == false && (isSemi || isInner))
                        {
                            i = outerValues.Count;
                            continue;
                        }
                    }

                    if (InnerHasNext())
                    {
                        outerValue = outerValues[i];
                        await NextInnerValueAsync(cancellationToken).ConfigureAwait(false);

                        if (predicate(outerValue!, innerValue!))
                        {
                            atLeastOneResult = true;

                            if (isAnti || isSemi)
                            {
                                if (i == 0)
                                {
                                    while (innerEnumHasNext)
                                    {
                                        innerValues.Add(innerCursor!.Current);
                                        innerEnumHasNext = await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false);
                                    }
                                }
                                else
                                {
                                    j = innerValues.Count;
                                }

                                if (isAnti)
                                    continue;
                            }

                            current = resultSelector(outerValue, innerValue);
                            return true;
                        }
                    }
                    else
                    {
                        if (atLeastOneResult == false && (isLeft || isAnti))
                        {
                            outerValue = outerValues[i];
                            innerValue = default;
                            NextOuterValue();
                            current = resultSelector(outerValue, innerValue);
                            return true;
                        }

                        NextOuterValue();
                    }
                }
            }

            /// <summary>
            /// The batch as the right side sees it: the batch size long, a short one filled
            /// out with its first row.
            /// </summary>
            java.util.ArrayList Padded()
            {
                var padded = new java.util.ArrayList(batchSize);
                for (int index = 0; index < batchSize; index++)
                    padded.add(JavaValues.From(outerValues[index < outerValues.Count ? index : 0]));

                return padded;
            }

            void NextOuterValue()
            {
                i++; // next outerValue
                j = 0; // rewind innerValues
                atLeastOneResult = false;
            }

            void NextInnerValue()
            {
                if (i == 0)
                {
                    innerValue = innerCursor!.Current;
                    innerValues.Add(innerValue);
                    innerEnumHasNext = innerCursor.Read(); // next cursor inner value
                }
                else
                {
                    innerValue = innerValues[j++]; // next cached inner value
                }
            }

            async ValueTask NextInnerValueAsync(CancellationToken cancellationToken)
            {
                if (i == 0)
                {
                    innerValue = innerCursor!.Current;
                    innerValues.Add(innerValue);
                    innerEnumHasNext = await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    innerValue = innerValues[j++];
                }
            }

            bool InnerHasNext()
            {
                return i == 0 ? innerEnumHasNext : j < innerValues.Count;
            }

            void CloseInner()
            {
                var closing = innerCursor;
                innerCursor = null;
                closing?.Dispose();
            }

            async ValueTask CloseInnerAsync()
            {
                var closing = innerCursor;
                innerCursor = null;
                if (closing != null)
                    await closing.DisposeAsync().ConfigureAwait(false);
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                outer.Dispose();
                CloseInner();
                outerValue = default;
                innerValue = default;
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                await outer.DisposeAsync().ConfigureAwait(false);
                await CloseInnerAsync().ConfigureAwait(false);
                outerValue = default;
                innerValue = default;
            }

        }

        /// <summary>
        /// Joins each row of the first cursor to the one row of the second that has the same key and the
        /// nearest timestamp satisfying the match condition.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the second cursor, which is acquired only once the first has been
        /// drained and closed.</param>
        /// <param name="outerKeySelector"></param>
        /// <param name="innerKeySelector"></param>
        /// <param name="resultSelector"></param>
        /// <param name="matchComparator"></param>
        /// <param name="timestampComparator"></param>
        /// <param name="emitNullsOnRight">Whether an outer row with no match is emitted against null.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.asofJoin</c>, and the same algorithm: index the left by
        /// key, hold the best right row per left row, scan the right updating it, then emit.
        ///
        /// <para>The index is a <c>java.util.HashMap</c> rather than a <see cref="Dictionary{TKey, TValue}"/>
        /// because the emitted order is that map's iteration order, and nothing else can agree with the map
        /// linq4j walks. Same lesson as the partition order of a window.</para>
        ///
        /// <para>Both scans run <b>at the open</b>, which is linq4j's own timing: <c>asofJoin</c> builds all
        /// three indexes in the method body and only then returns the enumerable that walks them. The
        /// outer is drained and closed before the inner is acquired — one try-with-resources after the
        /// other — which is why the inner arrives as an open rather than as a cursor.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> AsofJoin<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> matchComparator,
            java.util.Comparator timestampComparator,
            bool emitNullsOnRight)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(outerKeySelector);
            ArgumentNullException.ThrowIfNull(innerKeySelector);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(matchComparator);
            ArgumentNullException.ThrowIfNull(timestampComparator);

            var leftIndex = new java.util.HashMap();
            var rightIndex = new java.util.HashMap();
            var outerWithNullKeys = new List<TSource>();

            try
            {
                while (outer.Read())
                {
                    var row = outer.Current;
                    var key = outerKeySelector(row);
                    if (key == null)
                    {
                        // the key holds a null field, so it matches nothing
                        if (emitNullsOnRight)
                            outerWithNullKeys.Add(row);

                        continue;
                    }

                    var boxed = JavaValues.From(key);
                    if (leftIndex.get(boxed) is not List<TSource> left)
                    {
                        leftIndex.put(boxed, left = []);
                        rightIndex.put(boxed, new List<TInner>());
                    }

                    left.Add(row);
                    ((List<TInner>)rightIndex.get(boxed)).Add(default!);
                }
            }
            finally
            {
                outer.Dispose();
            }

            // scan right collection
            var second = inner();
            try
            {
                while (second.Read())
                {
                    var row = second.Current;
                    var key = innerKeySelector(row);
                    if (key == null)
                        continue;

                    var boxed = JavaValues.From(key);
                    if (leftIndex.get(boxed) is not List<TSource> left)
                        continue;

                    var best = (List<TInner>)rightIndex.get(boxed);

                    for (int i = 0; i < left.Count; i++)
                    {
                        if (matchComparator(left[i], row) == false)
                            continue;

                        if (best[i] == null || timestampComparator.compare(best[i], row) < 0)
                            best[i] = row;
                    }
                }
            }
            finally
            {
                second.Dispose();
            }

            return new AsofJoinCursor<TSource, TInner, TResult>(leftIndex, rightIndex, outerWithNullKeys, resultSelector, emitNullsOnRight);
        }

        /// <summary>
        /// <see cref="AsofJoin{TSource, TInner, TKey, TResult}"/>, over opens that await. Both scans await
        /// each row inside the open, which is what a cursor lets an open do and an
        /// <see cref="IAsyncEnumerable{T}"/> could not: its <c>GetAsyncEnumerator</c> cannot await, so the
        /// enumerable convention's ASOF join had to leave its scans to the first advance and say so.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> AsofJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> matchComparator,
            java.util.Comparator timestampComparator,
            bool emitNullsOnRight,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(outerKeySelector);
            ArgumentNullException.ThrowIfNull(innerKeySelector);
            ArgumentNullException.ThrowIfNull(resultSelector);
            ArgumentNullException.ThrowIfNull(matchComparator);
            ArgumentNullException.ThrowIfNull(timestampComparator);

            var leftIndex = new java.util.HashMap();
            var rightIndex = new java.util.HashMap();
            var outerWithNullKeys = new List<TSource>();

            var first = await outer.ConfigureAwait(false);
            try
            {
                while (await first.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = first.Current;
                    var key = outerKeySelector(row);
                    if (key == null)
                    {
                        // the key holds a null field, so it matches nothing
                        if (emitNullsOnRight)
                            outerWithNullKeys.Add(row);

                        continue;
                    }

                    var boxed = JavaValues.From(key);
                    if (leftIndex.get(boxed) is not List<TSource> left)
                    {
                        leftIndex.put(boxed, left = []);
                        rightIndex.put(boxed, new List<TInner>());
                    }

                    left.Add(row);
                    ((List<TInner>)rightIndex.get(boxed)).Add(default!);
                }
            }
            finally
            {
                await first.DisposeAsync().ConfigureAwait(false);
            }

            var second = await inner(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await second.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = second.Current;
                    var key = innerKeySelector(row);
                    if (key == null)
                        continue;

                    var boxed = JavaValues.From(key);
                    if (leftIndex.get(boxed) is not List<TSource> left)
                        continue;

                    var best = (List<TInner>)rightIndex.get(boxed);

                    for (int i = 0; i < left.Count; i++)
                    {
                        if (matchComparator(left[i], row) == false)
                            continue;

                        if (best[i] == null || timestampComparator.compare(best[i], row) < 0)
                            best[i] = row;
                    }
                }
            }
            finally
            {
                await second.DisposeAsync().ConfigureAwait(false);
            }

            return new AsofJoinCursor<TSource, TInner, TResult>(leftIndex, rightIndex, outerWithNullKeys, resultSelector, emitNullsOnRight);
        }

        /// <summary>
        /// The cursor of <see cref="AsofJoin{TSource, TInner, TKey, TResult}"/>: the small state machine
        /// <c>asofJoin</c> returns over its finished indexes.
        /// </summary>
        /// <remarks>
        /// It walks the left index's entries, and within each the left rows beside their best right rows,
        /// and once the entries are done it emits the outer rows whose key was null. Everything it walks is
        /// in hand, so nothing here awaits and nothing is disposed: both sources were closed by the open.
        /// </remarks>
        sealed class AsofJoinCursor<TSource, TInner, TResult>(
            java.util.HashMap leftIndex,
            java.util.HashMap rightIndex,
            List<TSource> outerWithNullKeys,
            Func<TSource?, TInner?, TResult> resultSelector,
            bool emitNullsOnRight) : ClrDataCursor<TResult>
        {

            readonly java.util.Iterator entries = leftIndex.entrySet().iterator();

            bool emittingNullKeys; // true when we emit the records with null keys
            List<TSource>? left; // the rows with the same key
            List<TInner>? right;
            int index = -1;
            int nullIndex = -1;
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (true)
                {
                    if (emittingNullKeys)
                    {
                        if (nullIndex + 1 >= outerWithNullKeys.Count)
                        {
                            nullIndex = outerWithNullKeys.Count;
                            return false;
                        }

                        nullIndex++;
                        current = resultSelector(outerWithNullKeys[nullIndex], default);
                        return true;
                    }

                    var hasNext = false;
                    if (left != null)
                    {
                        // advance left, right
                        index++;
                        hasNext = index < left.Count;
                    }

                    if (hasNext)
                    {
                        var r = right![index];
                        if (emitNullsOnRight == false && r == null)
                            continue;

                        current = resultSelector(left![index], r);
                        return true;
                    }

                    // advance the entries
                    if (entries.hasNext())
                    {
                        var entry = (java.util.Map.Entry)entries.next();
                        left = (List<TSource>)entry.getValue();
                        right = (List<TInner>)rightIndex.get(entry.getKey());
                        index = -1;
                    }
                    else
                    {
                        // done with the data, start emitting records with null keys
                        emittingNullKeys = true;
                    }
                }
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
                left = null;
                right = null;
            }

        }

        // ---- Join ----
        // The joins: the hash join, the semi join, the mark joins, the merge join and the nested loop join.
        // Each acquires at the moment its linq4j original does. A hash join drains its build side inside
        // <c>enumerator()</c>, so the open drains it and the cursor probes it with the other input, which
        // arrives opened. A semi join and a nested loop join acquire their inner inside <c>moveNext</c> — the
        // first memoized to the first outer row, the second once per outer row — so those take the inner as
        // openers of both kinds and call the one matching the advance. A merge join positions both inputs
        // inside <c>enumerator()</c>, and the open does. <c>nestedLoopJoinAsList</c> builds the whole result
        // where it is called, and here the call is the open.


        /// <summary>
        /// Returns every left row with a marker saying whether the right side had a match, using a hash table.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TNsKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="outerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="innerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="outerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are none.</param>
        /// <param name="innerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are none.</param>
        /// <param name="atMostOneNotNullSafeKey">Whether at most one join key uses EQUALS.</param>
        /// <param name="resultSelector"></param>
        /// <param name="comparer"></param>
        /// <param name="nullSafeComparer"></param>
        /// <param name="nonEquiPredicate">Three-valued, or null where the condition is all equalities.</param>
        /// <param name="equiPredicate">Three-valued.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.leftMarkHashJoin</c>, keeping the two algorithms behind
        /// it. A hash table answers whether anything matched, but a mark join needs the third value as well,
        /// and a lookup that finds nothing cannot tell FALSE from UNKNOWN on its own.
        ///
        /// <para>Where at most one key uses EQUALS, whether that key was ever null on the build side settles
        /// it: a probe that finds no bucket is UNKNOWN if it was and FALSE if it was not. Where several do,
        /// the equi-predicate has to be run against the rows whose key is null, because only it can say
        /// whether a comparison came out unknown. That is the whole of the difference between the two.</para>
        ///
        /// <para>The lookup is a <c>java.util.HashMap</c>, as every other operator here holds its rows in
        /// Calcite collection. None of this map order escapes — a mark join emits in the outer input order —
        /// but the hashing has to be Java hashing, because the keys are Calcite values.</para>
        ///
        /// <para><c>leftMarkHashJoin</c> builds its hash table inside <c>enumerator()</c> —
        /// <c>HashTableWithNullSafeKeySet.build</c> drains the build side there — so the build side is drained
        /// and closed here, at the open, and the probe side arrives opened.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> LeftMarkHashJoin<TSource, TInner, TKey, TNsKey, TResult>(
            ClrDataCursor<TSource> outer,
            ClrDataCursor<TInner> inner,
            Func<TSource, TKey> outerKeyNullAwareSelector,
            Func<TInner, TKey> innerKeyNullAwareSelector,
            Func<TSource, TNsKey>? outerNullSafeKeySelector,
            Func<TInner, TNsKey>? innerNullSafeKeySelector,
            bool atMostOneNotNullSafeKey,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            EqualityComparer? comparer,
            EqualityComparer? nullSafeComparer,
            Func<TSource, TInner, java.lang.Boolean?>? nonEquiPredicate,
            Func<TSource, TInner, java.lang.Boolean?> equiPredicate)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);

            // the build side: one bucket per key, plus the set of null-safe keys seen. A null key goes into
            // the map under null rather than being dropped, because the rows behind it are what say UNKNOWN
            var lookup = new java.util.HashMap();
            var nullSafeKeys = new java.util.HashSet();

            try
            {
                while (inner.Read())
                {
                    var row = inner.Current;
                    var key = innerKeyNullAwareSelector(row);
                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));

                    Bucket<TInner>(lookup, wrapped).Add(row);

                    if (innerNullSafeKeySelector != null)
                        nullSafeKeys.add(JavaWrapped.Of(nullSafeComparer, JavaValues.From(innerNullSafeKeySelector(row))));
                }
            }
            finally
            {
                inner.Dispose();
            }

            return new LeftMarkHashJoinCursor<TSource, TInner, TKey, TNsKey, TResult>(
                outer, outerKeyNullAwareSelector, outerNullSafeKeySelector, atMostOneNotNullSafeKey,
                resultSelector, comparer, nullSafeComparer, nonEquiPredicate, equiPredicate,
                lookup, nullSafeKeys);
        }

        /// <summary>
        /// <see cref="LeftMarkHashJoin"/>, over opens that await. The build side is drained with await
        /// inside the open, which an <see cref="IAsyncEnumerable{T}"/> could not do and a cursor's open
        /// can.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> LeftMarkHashJoinAsync<TSource, TInner, TKey, TNsKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            ValueTask<ClrDataCursor<TInner>> inner,
            Func<TSource, TKey> outerKeyNullAwareSelector,
            Func<TInner, TKey> innerKeyNullAwareSelector,
            Func<TSource, TNsKey>? outerNullSafeKeySelector,
            Func<TInner, TNsKey>? innerNullSafeKeySelector,
            bool atMostOneNotNullSafeKey,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            EqualityComparer? comparer,
            EqualityComparer? nullSafeComparer,
            Func<TSource, TInner, java.lang.Boolean?>? nonEquiPredicate,
            Func<TSource, TInner, java.lang.Boolean?> equiPredicate,
            CancellationToken cancellationToken)
        {
            var outerCursor = await outer.ConfigureAwait(false);
            var innerCursor = await inner.ConfigureAwait(false);

            // the build side: one bucket per key, plus the set of null-safe keys seen. A null key goes into
            // the map under null rather than being dropped, because the rows behind it are what say UNKNOWN
            var lookup = new java.util.HashMap();
            var nullSafeKeys = new java.util.HashSet();

            try
            {
                while (await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = innerCursor.Current;
                    var key = innerKeyNullAwareSelector(row);
                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));

                    Bucket<TInner>(lookup, wrapped).Add(row);

                    if (innerNullSafeKeySelector != null)
                        nullSafeKeys.add(JavaWrapped.Of(nullSafeComparer, JavaValues.From(innerNullSafeKeySelector(row))));
                }
            }
            finally
            {
                await innerCursor.DisposeAsync().ConfigureAwait(false);
            }

            return new LeftMarkHashJoinCursor<TSource, TInner, TKey, TNsKey, TResult>(
                outerCursor, outerKeyNullAwareSelector, outerNullSafeKeySelector, atMostOneNotNullSafeKey,
                resultSelector, comparer, nullSafeComparer, nonEquiPredicate, equiPredicate,
                lookup, nullSafeKeys);
        }

        /// <summary>
        /// The probe loop of <see cref="LeftMarkHashJoin"/>, over the lookup the open built.
        /// </summary>
        sealed class LeftMarkHashJoinCursor<TSource, TInner, TKey, TNsKey, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, TKey> outerKeyNullAwareSelector,
            Func<TSource, TNsKey>? outerNullSafeKeySelector,
            bool atMostOneNotNullSafeKey,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            EqualityComparer? comparer,
            EqualityComparer? nullSafeComparer,
            Func<TSource, TInner, java.lang.Boolean?>? nonEquiPredicate,
            Func<TSource, TInner, java.lang.Boolean?> equiPredicate,
            java.util.HashMap lookup,
            java.util.HashSet nullSafeKeys) : ClrDataCursor<TResult>
        {

            readonly bool buildSideIsEmpty = lookup.isEmpty();

            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (outer.Read() == false)
                    return false;

                current = resultSelector(outer.Current, Mark(outer.Current));
                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    return false;

                current = resultSelector(outer.Current, Mark(outer.Current));
                return true;
            }

            /// <summary>
            /// Returns the marker of one outer row.
            /// </summary>
            java.lang.Boolean? Mark(TSource row)
            {
                java.lang.Boolean? marker = java.lang.Boolean.FALSE;

                if (outerNullSafeKeySelector != null
                    && nullSafeKeys.contains(JavaWrapped.Of(nullSafeComparer, JavaValues.From(outerNullSafeKeySelector(row)))) == false)
                {
                    // a null-safe key matching nothing settles it: two rows that disagree there are not equal,
                    // whatever the rest of the condition says
                    return marker;
                }

                var key = outerKeyNullAwareSelector(row);

                if (key == null)
                {
                    if (atMostOneNotNullSafeKey)
                    {
                        // the one EQUALS key is null on this row, so every comparison it makes is unknown
                        marker = null;
                    }
                    else
                    {
                        // several EQUALS keys and one of them null: only the predicate can say whether a
                        // comparison came out unknown rather than false
                        foreach (var bucket in Buckets<TInner>(lookup))
                        {
                            foreach (var other in bucket)
                            {
                                if (equiPredicate(row, other) == null)
                                {
                                    marker = null;
                                    break;
                                }
                            }

                            if (marker == null)
                                break;
                        }
                    }
                }
                else if (lookup.get(JavaWrapped.Of(comparer, JavaValues.From(key))) is List<TInner> matches)
                {
                    if (nonEquiPredicate == null)
                    {
                        marker = java.lang.Boolean.TRUE;
                    }
                    else
                    {
                        foreach (var other in matches)
                        {
                            var matched = nonEquiPredicate(row, other);

                            if (matched == null)
                                marker = null;
                            else if (matched.booleanValue())
                            {
                                marker = java.lang.Boolean.TRUE;
                                break;
                            }
                        }
                    }
                }
                else if (lookup.get(null) is List<TInner> nulls)
                {
                    // nothing hashed to this key, but the build side holds rows whose key is null
                    if (atMostOneNotNullSafeKey)
                    {
                        marker = null;
                    }
                    else
                    {
                        foreach (var other in nulls)
                        {
                            if (equiPredicate(row, other) == null)
                            {
                                marker = null;
                                break;
                            }
                        }
                    }
                }

                // an empty build side is FALSE and never UNKNOWN: there was nothing to be unknown about
                if (marker == null && buildSideIsEmpty)
                    marker = java.lang.Boolean.FALSE;

                return marker;
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Returns each bucket of a lookup built by <see cref="LeftMarkHashJoin"/>.
        /// </summary>
        /// <typeparam name="TInner"></typeparam>
        /// <param name="lookup"></param>
        /// <returns></returns>
        static IEnumerable<List<TInner>> Buckets<TInner>(java.util.HashMap lookup)
        {
            for (var i = lookup.values().iterator(); i.hasNext();)
                if (i.next() is List<TInner> bucket)
                    yield return bucket;
        }

        /// <summary>
        /// Returns the bucket of a lookup under a key, adding an empty one where there was none.
        /// </summary>
        /// <typeparam name="TInner"></typeparam>
        /// <param name="lookup"></param>
        /// <param name="key">The key, wrapped for its comparer, or null for the rows whose key is null.</param>
        /// <returns></returns>
        static List<TInner> Bucket<TInner>(java.util.HashMap lookup, object? key)
        {
            if (lookup.get(key) is not List<TInner> bucket)
                lookup.put(key, bucket = []);

            return bucket;
        }

        /// <summary>
        /// Joins two inputs on a key.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="outerKeySelector"></param>
        /// <param name="innerKeySelector"></param>
        /// <param name="resultSelector"></param>
        /// <param name="comparer"></param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null right.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null when there is none.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.hashJoin</c>, taking the same arguments. A key that is null
        /// matches nothing, which is what the null aware accessor of a physical type arranges by returning null
        /// for the whole key.
        ///
        /// <para>Matching nothing is not the same as being dropped: a null-keyed build row is kept under a
        /// null key, which nothing probes, and a right or a full join ends by returning the rows that matched
        /// nothing — those among them.</para>
        ///
        /// <para>Calcite has two of these and so does this: <c>hashEquiJoin_</c> where the condition is an
        /// equality alone, and <c>hashJoinWithPredicate_</c> where it is not. They differ in more than the
        /// extra test — what "matched nothing" means is a key in one and a row in the other — so they are
        /// two methods rather than one with a null check inside the loop.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> HashJoin<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            ClrDataCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool>? predicate)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);

            return predicate == null
                ? HashEquiJoin(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight)
                : HashJoinWithPredicate(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
        }

        /// <summary>
        /// <see cref="HashJoin"/>, over opens that await. The build side is drained with await inside the
        /// open.
        /// </summary>
        public static ValueTask<ClrDataCursor<TResult>> HashJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            ValueTask<ClrDataCursor<TInner>> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool>? predicate,
            CancellationToken cancellationToken)
        {
            return predicate == null
                ? HashEquiJoinAsync(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, cancellationToken)
                : HashJoinWithPredicateAsync(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate, cancellationToken);
        }

        /// <summary>
        /// Joins two inputs on a key alone.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.hashEquiJoin_</c>. What is left over at the end is a
        /// <em>key</em> no outer row carried, and every build row under it comes out together.
        ///
        /// <para><c>hashEquiJoin_</c>'s <c>enumerator()</c> drains the build side into the lookup and
        /// acquires the probe side's enumerator, both before the first <c>moveNext</c>: the drain is here,
        /// at the open, and the probe side arrives opened.</para>
        /// </remarks>
        static ClrDataCursor<TResult> HashEquiJoin<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            ClrDataCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight)
        {
            // the lookup is a java.util.HashMap, as linq4j's toLookup builds one: a right or a full join
            // ends with the rows of the right input that matched nothing, and the order those come out in
            // is this map's. See JavaHashingTests for why that order is the same in every process.
            var lookup = new java.util.HashMap();

            try
            {
                while (inner.Read())
                {
                    var row = inner.Current;
                    var key = innerKeySelector(row);

                    // a null key is kept under a null key, as toLookup keeps one, and nothing probes it
                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                    Bucket<TInner>(lookup, wrapped).Add(row);
                }
            }
            finally
            {
                inner.Dispose();
            }

            // every key the build side has, less the ones an outer row carries. Calcite keeps it this way
            // round, and it matters where the lookup holds a key nothing probes: that key's rows are what
            // a right or a full join owes against a null left.
            var unmatched = generateNullsOnLeft ? new java.util.HashSet(lookup.keySet()) : null;

            return new HashEquiJoinCursor<TSource, TInner, TKey, TResult>(outer, outerKeySelector, resultSelector, comparer, generateNullsOnRight, lookup, unmatched);
        }

        /// <summary>
        /// <see cref="HashEquiJoin"/>, over opens that await.
        /// </summary>
        static async ValueTask<ClrDataCursor<TResult>> HashEquiJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            ValueTask<ClrDataCursor<TInner>> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            CancellationToken cancellationToken)
        {
            var outerCursor = await outer.ConfigureAwait(false);
            var innerCursor = await inner.ConfigureAwait(false);

            // the lookup is a java.util.HashMap, as linq4j's toLookup builds one: a right or a full join
            // ends with the rows of the right input that matched nothing, and the order those come out in
            // is this map's. See JavaHashingTests for why that order is the same in every process.
            var lookup = new java.util.HashMap();

            try
            {
                while (await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = innerCursor.Current;
                    var key = innerKeySelector(row);

                    // a null key is kept under a null key, as toLookup keeps one, and nothing probes it
                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                    Bucket<TInner>(lookup, wrapped).Add(row);
                }
            }
            finally
            {
                await innerCursor.DisposeAsync().ConfigureAwait(false);
            }

            // every key the build side has, less the ones an outer row carries. Calcite keeps it this way
            // round, and it matters where the lookup holds a key nothing probes: that key's rows are what
            // a right or a full join owes against a null left.
            var unmatched = generateNullsOnLeft ? new java.util.HashSet(lookup.keySet()) : null;

            return new HashEquiJoinCursor<TSource, TInner, TKey, TResult>(outerCursor, outerKeySelector, resultSelector, comparer, generateNullsOnRight, lookup, unmatched);
        }

        /// <summary>
        /// The probe loop of <see cref="HashEquiJoin"/>, over the lookup the open built.
        /// </summary>
        /// <remarks>
        /// Three states: drawing an outer row, pairing the one drawn with its bucket, and — once the outer
        /// is exhausted and the join generates nulls on the left — walking the unmatched keys. Both advances
        /// step the same states; only the draw differs.
        /// </remarks>
        sealed class HashEquiJoinCursor<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, TKey> outerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnRight,
            java.util.HashMap lookup,
            java.util.HashSet? unmatched) : ClrDataCursor<TResult>
        {

            const int DrawOuter = 0;
            const int Pairing = 1;
            const int Leftovers = 2;
            const int Done = 3;

            // the outer row being probed, the bucket its key found, how far along it the pairing is, and
            // whether it has paired with anything
            TSource row = default!;
            List<TInner>? bucket;
            int index;
            bool any;

            // the unmatched keys, walked once the outer is exhausted
            java.util.Iterator? leftovers;

            TResult current = default!;
            int state;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                for (; ; )
                {
                    switch (state)
                    {
                        case DrawOuter:
                            if (outer.Read())
                                Probe(outer.Current);
                            else
                                Exhausted();
                            continue;
                        case Pairing:
                            if (Pair())
                                return true;
                            continue;
                        case Leftovers:
                            if (Leftover())
                                return true;
                            continue;
                        default:
                            return false;
                    }
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                for (; ; )
                {
                    switch (state)
                    {
                        case DrawOuter:
                            if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                                Probe(outer.Current);
                            else
                                Exhausted();
                            continue;
                        case Pairing:
                            if (Pair())
                                return true;
                            continue;
                        case Leftovers:
                            if (Leftover())
                                return true;
                            continue;
                        default:
                            return false;
                    }
                }
            }

            /// <summary>
            /// Probes the lookup with an outer row, taking its key out of the unmatched set.
            /// </summary>
            void Probe(TSource row)
            {
                this.row = row;
                var key = outerKeySelector(row);
                any = false;
                bucket = null;
                index = 0;

                if (key != null)
                {
                    var wrapped = JavaWrapped.Of(comparer, JavaValues.From(key));
                    unmatched?.remove(wrapped);

                    bucket = lookup.get(wrapped) as List<TInner>;
                }

                state = Pairing;
            }

            /// <summary>
            /// Emits the next pairing of the outer row, or the row against a null right once its bucket is
            /// spent and it paired with nothing.
            /// </summary>
            bool Pair()
            {
                if (bucket != null && index < bucket.Count)
                {
                    any = true;
                    current = resultSelector(row, bucket[index++]);
                    return true;
                }

                state = DrawOuter;

                if (any == false && generateNullsOnRight)
                {
                    current = resultSelector(row, default);
                    return true;
                }

                return false;
            }

            /// <summary>
            /// Moves on from the exhausted outer: to the unmatched keys where the join owes them, and to the
            /// end otherwise.
            /// </summary>
            void Exhausted()
            {
                if (unmatched == null)
                {
                    state = Done;
                    return;
                }

                // the set is walked and each key looked back up, which is what linq4j does and is not the
                // same as walking the map and filtering by the set. A HashSet copied from a key set does not
                // have the map's iteration order: HashSet(Collection) sizes its table as
                // tableSizeFor(max((int) (n / 0.75f) + 1, 16)), while a map grown by insertion holds the
                // smallest power of two at or above 16 that still leaves n <= 0.75 * cap. The two disagree
                // exactly where n = 0.75 * 2^k — 12, 24, 48 — and these rows have no ORDER BY over them.
                leftovers = unmatched.iterator();
                bucket = null;
                index = 0;
                state = Leftovers;
            }

            /// <summary>
            /// Emits the next build row under an unmatched key, against a null left.
            /// </summary>
            bool Leftover()
            {
                for (; ; )
                {
                    if (bucket != null && index < bucket.Count)
                    {
                        current = resultSelector(default, bucket[index++]);
                        return true;
                    }

                    if (leftovers!.hasNext() == false)
                    {
                        state = Done;
                        return false;
                    }

                    bucket = (List<TInner>)lookup.get(leftovers.next());
                    index = 0;
                }
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Joins two inputs on a key and something else besides.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.hashJoinWithPredicate_</c>. What is left over at the end
        /// is a <em>row</em> the predicate rejected or the key never reached, and the leftovers come out in
        /// the build input's own order rather than the lookup's.
        ///
        /// <para>Per row and not per key, which is the whole difference. A build row whose key matched but
        /// whose predicate did not has matched nothing, and a right join owes it a row against a null left.
        /// Tracking the key instead lost it, because some other row under that key had passed.</para>
        ///
        /// <para><c>hashJoinWithPredicate_</c>'s <c>enumerator()</c> reads the build side, builds the lookup
        /// and the leftover list, and acquires the probe side's enumerator, all before the first
        /// <c>moveNext</c>: the build side is read here, at the open, and the probe side arrives opened.</para>
        /// </remarks>
        static ClrDataCursor<TResult> HashJoinWithPredicate<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            ClrDataCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate)
        {
            // read once, because a right or a full join walks it twice: a cursor is read once, so the rows
            // are kept as they go where the second walk will want them
            var innerToLookUp = generateNullsOnLeft ? new List<TInner>() : null;

            var lookup = new java.util.HashMap();

            try
            {
                while (inner.Read())
                {
                    var row = inner.Current;
                    innerToLookUp?.Add(row);

                    var key = innerKeySelector(row);

                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                    Bucket<TInner>(lookup, wrapped).Add(row);
                }
            }
            finally
            {
                inner.Dispose();
            }

            var unmatched = innerToLookUp == null ? null : new List<TInner>(innerToLookUp);

            return new HashJoinWithPredicateCursor<TSource, TInner, TKey, TResult>(outer, outerKeySelector, resultSelector, comparer, generateNullsOnRight, predicate, lookup, unmatched);
        }

        /// <summary>
        /// <see cref="HashJoinWithPredicate"/>, over opens that await.
        /// </summary>
        static async ValueTask<ClrDataCursor<TResult>> HashJoinWithPredicateAsync<TSource, TInner, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            ValueTask<ClrDataCursor<TInner>> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate,
            CancellationToken cancellationToken)
        {
            var outerCursor = await outer.ConfigureAwait(false);
            var innerCursor = await inner.ConfigureAwait(false);

            // read once, because a right or a full join walks it twice: a cursor is read once, so the rows
            // are kept as they go where the second walk will want them
            var innerToLookUp = generateNullsOnLeft ? new List<TInner>() : null;

            var lookup = new java.util.HashMap();

            try
            {
                while (await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = innerCursor.Current;
                    innerToLookUp?.Add(row);

                    var key = innerKeySelector(row);

                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                    Bucket<TInner>(lookup, wrapped).Add(row);
                }
            }
            finally
            {
                await innerCursor.DisposeAsync().ConfigureAwait(false);
            }

            var unmatched = innerToLookUp == null ? null : new List<TInner>(innerToLookUp);

            return new HashJoinWithPredicateCursor<TSource, TInner, TKey, TResult>(outerCursor, outerKeySelector, resultSelector, comparer, generateNullsOnRight, predicate, lookup, unmatched);
        }

        /// <summary>
        /// The probe loop of <see cref="HashJoinWithPredicate"/>, over the lookup and leftover list the open
        /// built.
        /// </summary>
        /// <remarks>
        /// The states of <see cref="HashEquiJoinCursor{TSource, TInner, TKey, TResult}"/>, with the bucket
        /// filtered by the predicate before it is paired and the leftovers a list of rows rather than a set
        /// of keys.
        /// </remarks>
        sealed class HashJoinWithPredicateCursor<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, TKey> outerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate,
            java.util.HashMap lookup,
            List<TInner>? unmatched) : ClrDataCursor<TResult>
        {

            const int DrawOuter = 0;
            const int Pairing = 1;
            const int Leftovers = 2;
            const int Done = 3;

            // the outer row being probed, the build rows of its key the predicate accepted, how far along
            // them the pairing is, and whether it has paired with anything
            TSource row = default!;
            List<TInner>? accepted;
            int index;
            bool any;

            // how far along the leftovers the walk is, once the outer is exhausted
            int leftover;

            TResult current = default!;
            int state;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                for (; ; )
                {
                    switch (state)
                    {
                        case DrawOuter:
                            if (outer.Read())
                                Probe(outer.Current);
                            else
                                state = unmatched == null ? Done : Leftovers;
                            continue;
                        case Pairing:
                            if (Pair())
                                return true;
                            continue;
                        case Leftovers:
                            if (Leftover())
                                return true;
                            continue;
                        default:
                            return false;
                    }
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                for (; ; )
                {
                    switch (state)
                    {
                        case DrawOuter:
                            if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                                Probe(outer.Current);
                            else
                                state = unmatched == null ? Done : Leftovers;
                            continue;
                        case Pairing:
                            if (Pair())
                                return true;
                            continue;
                        case Leftovers:
                            if (Leftover())
                                return true;
                            continue;
                        default:
                            return false;
                    }
                }
            }

            /// <summary>
            /// Probes the lookup with an outer row and runs the predicate over its bucket, taking every
            /// build row it accepts out of the leftovers.
            /// </summary>
            void Probe(TSource row)
            {
                this.row = row;
                var key = outerKeySelector(row);
                any = false;
                accepted = null;
                index = 0;

                if (key != null && lookup.get(JavaWrapped.Of(comparer, JavaValues.From(key))) is List<TInner> bucket)
                {
                    accepted = [];
                    foreach (var other in bucket)
                        if (predicate(row, other))
                            accepted.Add(other);

                    unmatched?.RemoveAll(accepted.Contains);
                }

                state = Pairing;
            }

            /// <summary>
            /// Emits the next pairing of the outer row, or the row against a null right once the accepted
            /// rows are spent and it paired with nothing.
            /// </summary>
            bool Pair()
            {
                if (accepted != null && index < accepted.Count)
                {
                    any = true;
                    current = resultSelector(row, accepted[index++]);
                    return true;
                }

                state = DrawOuter;

                if (any == false && generateNullsOnRight)
                {
                    current = resultSelector(row, default);
                    return true;
                }

                return false;
            }

            /// <summary>
            /// Emits the next leftover build row, against a null left.
            /// </summary>
            bool Leftover()
            {
                if (leftover < unmatched!.Count)
                {
                    current = resultSelector(default, unmatched[leftover++]);
                    return true;
                }

                state = Done;
                return false;
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Returns the rows of the first input that have, or have not, a match in the second.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the second input synchronously.</param>
        /// <param name="innerAsync">Opens the second input with await.</param>
        /// <param name="outerKeySelector"></param>
        /// <param name="innerKeySelector"></param>
        /// <param name="comparer"></param>
        /// <param name="anti">Whether the rows without a match are the ones returned.</param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.semiJoin</c>, which is a dispatch and not an implementation: with no
        /// predicate it is <c>semiEquiJoin_</c>, which holds the distinct inner <em>keys</em>, and with one it
        /// is <c>semiJoinWithPredicate_</c>, which holds a lookup of inner <em>rows</em> because the predicate
        /// has to see them. Those are the two methods below, and they are not the same algorithm.
        ///
        /// <para>Both acquire the outer at <c>enumerator()</c>, which is the open, and the inner not until the
        /// first outer row is tested — CALCITE-2909, which memoizes the lookup to that moment. That moment is
        /// inside an advance, and the advance may be either, so the inner arrives as openers of both kinds
        /// and the cursor calls the one matching the advance that reached it.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> SemiJoin<TSource, TInner, TKey>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool>? predicate)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);

            return predicate == null
                ? new SemiEquiJoinCursor<TSource, TInner, TKey>(outer, inner, innerAsync, outerKeySelector, innerKeySelector, comparer, anti)
                : new SemiJoinWithPredicateCursor<TSource, TInner, TKey>(outer, inner, innerAsync, outerKeySelector, innerKeySelector, comparer, anti, predicate);
        }

        /// <summary>
        /// <see cref="SemiJoin"/>, over an open that awaits. Nothing but the outer is acquired at this open;
        /// the inner is acquired inside the advance that reads the first outer row.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> SemiJoinAsync<TSource, TInner, TKey>(
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool>? predicate,
            CancellationToken cancellationToken)
        {
            return SemiJoin(await outer.ConfigureAwait(false), inner, innerAsync, outerKeySelector, innerKeySelector, comparer, anti, predicate);
        }

        /// <summary>
        /// Returns the rows of the first input whose key is, or is not, one of the second's.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.semiEquiJoin_</c>. It holds <c>inner.select(innerKeySelector).distinct()</c>
        /// -- the distinct keys, not the rows -- and asks it <c>contains</c> per outer row.
        ///
        /// <para>Two sets, and they are not redundant. <c>distinct(comparer)</c> decides which keys are
        /// duplicates of one another by the comparer, but the <c>contains</c> that follows is
        /// <c>EnumerableDefaults.contains</c> over the unwrapped result, which is <c>Objects.equals</c> and
        /// not the comparer. So a comparer coarser than <c>equals</c> collapses two keys that are not equal,
        /// and the survivor answers for both. Keying one set by the comparer reproduces that; keying the
        /// membership test by it as well does not.</para>
        ///
        /// <para>CALCITE-2909: the keys are not built until the first outer row is in hand, so an empty outer
        /// never opens the inner. Calcite writes <c>Suppliers.memoize</c>; a null field says the same thing
        /// where one loop is the only consumer.</para>
        /// </remarks>
        sealed class SemiEquiJoinCursor<TSource, TInner, TKey>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti) : ClrDataCursor<TSource>
        {

            java.util.HashSet? keys;

            /// <inheritdoc />
            public override TSource Current => outer.Current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (outer.Read())
                {
                    if (keys == null)
                    {
                        var distinct = new java.util.HashSet();
                        keys = new java.util.HashSet();

                        var rows = inner();
                        try
                        {
                            while (rows.Read())
                                Add(rows.Current, distinct);
                        }
                        finally
                        {
                            rows.Dispose();
                        }
                    }

                    if (Found(outer.Current) != anti)
                        return true;
                }

                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (keys == null)
                    {
                        var distinct = new java.util.HashSet();
                        keys = new java.util.HashSet();

                        var rows = await innerAsync(cancellationToken).ConfigureAwait(false);
                        try
                        {
                            while (await rows.ReadAsync(cancellationToken).ConfigureAwait(false))
                                Add(rows.Current, distinct);
                        }
                        finally
                        {
                            await rows.DisposeAsync().ConfigureAwait(false);
                        }
                    }

                    if (Found(outer.Current) != anti)
                        return true;
                }

                return false;
            }

            /// <summary>
            /// Adds an inner row's key, where the comparer has not seen it already.
            /// </summary>
            void Add(TInner innerRow, java.util.HashSet distinct)
            {
                var innerKey = JavaValues.From(innerKeySelector(innerRow));

                if (distinct.add(JavaWrapped.Of(comparer, innerKey)))
                    keys!.add(innerKey);
            }

            /// <summary>
            /// Returns whether an outer row's key is one of the inner's.
            /// </summary>
            bool Found(TSource row)
            {
                var key = outerKeySelector(row);

                return key != null && keys!.contains(JavaValues.From(key));
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Returns the rows of the first input that have, or have not, a match in the second under a
        /// condition the key does not express.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.semiJoinWithPredicate_</c>. This one does hold the inner rows -- the
        /// predicate is given a pair -- and it is <c>toLookup</c>, so unlike the equi path above the comparer
        /// does reach the lookup. Memoized on the first outer row for the same reason.
        /// </remarks>
        sealed class SemiJoinWithPredicateCursor<TSource, TInner, TKey>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool> predicate) : ClrDataCursor<TSource>
        {

            java.util.HashMap? lookup;

            /// <inheritdoc />
            public override TSource Current => outer.Current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (outer.Read())
                {
                    if (lookup == null)
                    {
                        lookup = new java.util.HashMap();

                        var rows = inner();
                        try
                        {
                            while (rows.Read())
                                Add(rows.Current);
                        }
                        finally
                        {
                            rows.Dispose();
                        }
                    }

                    if (Found(outer.Current) != anti)
                        return true;
                }

                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (lookup == null)
                    {
                        lookup = new java.util.HashMap();

                        var rows = await innerAsync(cancellationToken).ConfigureAwait(false);
                        try
                        {
                            while (await rows.ReadAsync(cancellationToken).ConfigureAwait(false))
                                Add(rows.Current);
                        }
                        finally
                        {
                            await rows.DisposeAsync().ConfigureAwait(false);
                        }
                    }

                    if (Found(outer.Current) != anti)
                        return true;
                }

                return false;
            }

            /// <summary>
            /// Adds an inner row to the bucket of its key.
            /// </summary>
            void Add(TInner innerRow)
            {
                var innerKey = JavaWrapped.Of(comparer, JavaValues.From(innerKeySelector(innerRow)));

                Bucket<TInner>(lookup!, innerKey).Add(innerRow);
            }

            /// <summary>
            /// Returns whether any inner row of an outer row's key satisfies the predicate with it.
            /// </summary>
            bool Found(TSource row)
            {
                var key = outerKeySelector(row);

                if (key != null && lookup!.get(JavaWrapped.Of(comparer, JavaValues.From(key))) is List<TInner> matches)
                {
                    foreach (var other in matches)
                    {
                        if (predicate(row, other))
                            return true;
                    }
                }

                return false;
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Returns whether a merge join can answer a join of this type.
        /// </summary>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static bool IsMergeJoinSupported(org.apache.calcite.linq4j.JoinType joinType)
        {
            return joinType.name() is nameof(org.apache.calcite.linq4j.JoinType.INNER)
                or nameof(org.apache.calcite.linq4j.JoinType.SEMI)
                or nameof(org.apache.calcite.linq4j.JoinType.ANTI)
                or nameof(org.apache.calcite.linq4j.JoinType.LEFT);
        }

        /// <summary>
        /// Joins two inputs that are already sorted on the key, ascending with nulls last.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="outerKeySelector"></param>
        /// <param name="innerKeySelector"></param>
        /// <param name="predicate">The part of the condition that is not an equality, or null.</param>
        /// <param name="resultSelector"></param>
        /// <param name="joinType"></param>
        /// <param name="comparator">Orders two keys; null means they compare themselves.</param>
        /// <param name="comparer">Decides whether two keys of one input are the same; null means they do.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.mergeJoin</c>, statement for statement, holding its state
        /// in <see cref="MergeJoinCursor{TSource, TInner, TKey, TResult}"/> the way linq4j's
        /// <c>MergeJoinEnumerator</c> holds it. Both inputs are walked once and only the rows of one key are
        /// held.
        ///
        /// <para>Two nulls must not compare equal, or a join of two null keys would return rows SQL says it
        /// does not. Calcite signals that out of its comparator by throwing, and catches it to advance the
        /// right side; the generated comparator this is called with is that comparator, so the same throw is
        /// caught here — by name, since the exception class is package private.</para>
        ///
        /// <para><c>MergeJoinEnumerator</c>'s constructor calls <c>start()</c>, which positions both inputs,
        /// so obtaining linq4j's enumerator reads each input as far as its first key run; the open does the
        /// same, and the awaiting open awaits it.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> MergeJoin<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            ClrDataCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, bool>? predicate,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            java.util.Comparator? comparator,
            EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);

            if (IsMergeJoinSupported(joinType) == false)
                throw new java.lang.UnsupportedOperationException($"MergeJoin unsupported for join type {joinType}");

            var cursor = new MergeJoinCursor<TSource, TInner, TKey, TResult>(
                outer, inner, outerKeySelector, innerKeySelector, predicate, resultSelector, joinType, comparator, comparer);

            cursor.Start();

            return cursor;
        }

        /// <summary>
        /// <see cref="MergeJoin"/>, over opens that await. The positioning <c>start()</c> does is awaited
        /// inside the open, which an <see cref="IAsyncEnumerable{T}"/> could not do and a cursor's open can.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> MergeJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            ValueTask<ClrDataCursor<TInner>> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, bool>? predicate,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            java.util.Comparator? comparator,
            EqualityComparer? comparer,
            CancellationToken cancellationToken)
        {
            if (IsMergeJoinSupported(joinType) == false)
                throw new java.lang.UnsupportedOperationException($"MergeJoinAsync unsupported for join type {joinType}");

            var cursor = new MergeJoinCursor<TSource, TInner, TKey, TResult>(
                await outer.ConfigureAwait(false), await inner.ConfigureAwait(false), outerKeySelector, innerKeySelector, predicate, resultSelector, joinType, comparator, comparer);

            await cursor.StartAsync(cancellationToken).ConfigureAwait(false);

            return cursor;
        }

        /// <summary>
        /// The state of one merge join: linq4j's <c>MergeJoinEnumerator</c>, with the fields its anonymous
        /// class holds and the methods it dispatches, each written twice — once stepping the inputs with
        /// <c>Read</c> and once with <c>ReadAsync</c> — over the one set of fields.
        /// </summary>
        sealed class MergeJoinCursor<TSource, TInner, TKey, TResult>(
            ClrDataCursor<TSource> outer,
            ClrDataCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, bool>? predicate,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            java.util.Comparator? comparator,
            EqualityComparer? comparer) : ClrDataCursor<TResult>
        {

            readonly IEqualityComparer<TKey> equality = JavaEqualityComparer<TKey>.Of(comparer);

            readonly bool isLeft = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.LEFT);
            readonly bool isAnti = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            readonly bool isSemi = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.SEMI);

            readonly List<TSource> lefts = [];
            readonly List<TInner> rights = [];
            bool done;
            bool remainingLeft;
            ClrDataCursor<TResult>? results;
            TResult current = default!;

            bool IsLeftOrAnti => isLeft || isAnti;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <summary>
            /// <c>start()</c>: positions both inputs and settles the initial state.
            /// </summary>
            internal void Start()
            {
                if (IsLeftOrAnti)
                {
                    if (LeftMoveNext() == false)
                        done = true;
                    else if (RightMoveNext() == false)
                        remainingLeft = true;
                    else if (Advance() == false)
                        done = true;
                }
                else if (LeftMoveNext() == false || RightMoveNext() == false || Advance() == false)
                {
                    done = true;
                }
            }

            /// <summary>
            /// <see cref="Start"/>, awaiting each input it steps.
            /// </summary>
            internal async ValueTask StartAsync(CancellationToken cancellationToken)
            {
                if (IsLeftOrAnti)
                {
                    if (await LeftMoveNextAsync(cancellationToken).ConfigureAwait(false) == false)
                        done = true;
                    else if (await RightMoveNextAsync(cancellationToken).ConfigureAwait(false) == false)
                        remainingLeft = true;
                    else if (await AdvanceAsync(cancellationToken).ConfigureAwait(false) == false)
                        done = true;
                }
                else if (await LeftMoveNextAsync(cancellationToken).ConfigureAwait(false) == false
                    || await RightMoveNextAsync(cancellationToken).ConfigureAwait(false) == false
                    || await AdvanceAsync(cancellationToken).ConfigureAwait(false) == false)
                {
                    done = true;
                }
            }

            // the left input advanced, and onto a row whose key is not null — a LEFT join reads its left
            // input to the end whatever the keys are, because every row of it is a result
            bool LeftMoveNext() => outer.Read() && (isLeft || outerKeySelector(outer.Current) != null);

            async ValueTask<bool> LeftMoveNextAsync(CancellationToken cancellationToken) =>
                await outer.ReadAsync(cancellationToken).ConfigureAwait(false) && (isLeft || outerKeySelector(outer.Current) != null);

            bool RightMoveNext() => inner.Read() && innerKeySelector(inner.Current) != null;

            async ValueTask<bool> RightMoveNextAsync(CancellationToken cancellationToken) =>
                await inner.ReadAsync(cancellationToken).ConfigureAwait(false) && innerKeySelector(inner.Current) != null;

            int Compare(TKey a, TKey b)
            {
                if (comparator == null)
                    return CompareNullsLastForMergeJoin(a, b);

                try
                {
                    return comparator.compare(JavaValues.From(a), JavaValues.From(b));
                }
                catch (java.lang.RuntimeException e) when (e.GetType().Name.Contains("BothValuesAreNull"))
                {
                    // two nulls: take the left as the bigger, so the right advances and the algorithm goes on
                    return 1;
                }
            }

            // the rows of one key on the left, and whether the input has more after them
            bool AdvanceLeft(TSource left, TKey leftKey)
            {
                lefts.Clear();
                lefts.Add(left);

                while (outer.Read())
                {
                    left = outer.Current;
                    var leftKey2 = outerKeySelector(left);
                    if (leftKey2 == null && isLeft == false)
                        break;
                    if (equality.Equals(leftKey, leftKey2) == false)
                        return true;

                    lefts.Add(left);
                }

                return false;
            }

            async ValueTask<bool> AdvanceLeftAsync(TSource left, TKey leftKey, CancellationToken cancellationToken)
            {
                lefts.Clear();
                lefts.Add(left);

                while (await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    left = outer.Current;
                    var leftKey2 = outerKeySelector(left);
                    if (leftKey2 == null && isLeft == false)
                        break;
                    if (equality.Equals(leftKey, leftKey2) == false)
                        return true;

                    lefts.Add(left);
                }

                return false;
            }

            bool AdvanceRight(TInner right, TKey rightKey)
            {
                rights.Clear();
                rights.Add(right);

                while (inner.Read())
                {
                    right = inner.Current;
                    var rightKey2 = innerKeySelector(right);
                    if (rightKey2 == null)
                        break;
                    if (equality.Equals(rightKey, rightKey2) == false)
                        return true;

                    rights.Add(right);
                }

                return false;
            }

            async ValueTask<bool> AdvanceRightAsync(TInner right, TKey rightKey, CancellationToken cancellationToken)
            {
                rights.Clear();
                rights.Add(right);

                while (await inner.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    right = inner.Current;
                    var rightKey2 = innerKeySelector(right);
                    if (rightKey2 == null)
                        break;
                    if (equality.Equals(rightKey, rightKey2) == false)
                        return true;

                    rights.Add(right);
                }

                return false;
            }

            // moves to the next key present on both sides, filling lefts and rights with its rows
            bool Advance()
            {
                while (true)
                {
                    var left = outer.Current;
                    var leftKey = outerKeySelector(left);
                    var right = inner.Current;
                    var rightKey = innerKeySelector(right);

                    while (true)
                    {
                        // the inputs are sorted with nulls last, so a null key means there is no more to match
                        if (leftKey == null || rightKey == null)
                        {
                            if (isLeft || (isAnti && leftKey != null))
                            {
                                remainingLeft = true;
                                return true;
                            }

                            done = true;
                            return false;
                        }

                        int c;

                        try
                        {
                            c = Compare(leftKey, rightKey);
                        }
                        catch (BothValuesAreNullException)
                        {
                            // take the left as the bigger, so the right advances and the algorithm carries on.
                            // Unreachable: the null guard above returns before either key can be null. Calcite
                            // has the same dead catch, and it is what decides the answer rather than the
                            // comparison, so it is written here too.
                            c = 1;
                        }

                        if (c == 0)
                            break;

                        if (c < 0)
                        {
                            if (IsLeftOrAnti)
                            {
                                // this row, and every other with the same key, is a result on its own
                                if (AdvanceLeft(left, leftKey) == false)
                                    done = true;

                                results = new CartesianCursor<TSource, TInner, TResult>(lefts, [default!], resultSelector);
                                return true;
                            }

                            if (outer.Read() == false)
                            {
                                done = true;
                                return false;
                            }

                            left = outer.Current;
                            leftKey = outerKeySelector(left);
                        }
                        else
                        {
                            if (inner.Read() == false)
                            {
                                if (IsLeftOrAnti)
                                {
                                    remainingLeft = true;
                                    return true;
                                }

                                done = true;
                                return false;
                            }

                            right = inner.Current;
                            rightKey = innerKeySelector(right);
                        }
                    }

                    if (AdvanceLeft(left, leftKey) == false)
                        done = true;

                    if (AdvanceRight(right, rightKey) == false)
                    {
                        if (done == false && IsLeftOrAnti)
                            remainingLeft = true;
                        else
                            done = true;
                    }

                    if (predicate == null)
                    {
                        if (isAnti)
                        {
                            // a key with a match on the right is not an anti join result
                            if (done)
                                return false;
                            if (remainingLeft)
                                return true;

                            continue;
                        }

                        // a semi join must not repeat a left row, so one right row of the key is enough
                        results = isSemi
                            ? new CartesianCursor<TSource, TInner, TResult>(lefts, [rights[0]], resultSelector)
                            : new CartesianCursor<TSource, TInner, TResult>(lefts, rights, resultSelector);
                    }
                    else
                    {
                        // the rest of the condition still has to hold, and a nested loop over the two runs is
                        // what decides it
                        results = Residual();
                    }

                    return true;
                }
            }

            async ValueTask<bool> AdvanceAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    var left = outer.Current;
                    var leftKey = outerKeySelector(left);
                    var right = inner.Current;
                    var rightKey = innerKeySelector(right);

                    while (true)
                    {
                        // the inputs are sorted with nulls last, so a null key means there is no more to match
                        if (leftKey == null || rightKey == null)
                        {
                            if (isLeft || (isAnti && leftKey != null))
                            {
                                remainingLeft = true;
                                return true;
                            }

                            done = true;
                            return false;
                        }

                        int c;

                        try
                        {
                            c = Compare(leftKey, rightKey);
                        }
                        catch (BothValuesAreNullException)
                        {
                            // take the left as the bigger, so the right advances and the algorithm carries on.
                            // Unreachable: the null guard above returns before either key can be null. Calcite
                            // has the same dead catch, and it is what decides the answer rather than the
                            // comparison, so it is written here too.
                            c = 1;
                        }

                        if (c == 0)
                            break;

                        if (c < 0)
                        {
                            if (IsLeftOrAnti)
                            {
                                // this row, and every other with the same key, is a result on its own
                                if (await AdvanceLeftAsync(left, leftKey, cancellationToken).ConfigureAwait(false) == false)
                                    done = true;

                                results = new CartesianCursor<TSource, TInner, TResult>(lefts, [default!], resultSelector);
                                return true;
                            }

                            if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                            {
                                done = true;
                                return false;
                            }

                            left = outer.Current;
                            leftKey = outerKeySelector(left);
                        }
                        else
                        {
                            if (await inner.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                            {
                                if (IsLeftOrAnti)
                                {
                                    remainingLeft = true;
                                    return true;
                                }

                                done = true;
                                return false;
                            }

                            right = inner.Current;
                            rightKey = innerKeySelector(right);
                        }
                    }

                    if (await AdvanceLeftAsync(left, leftKey, cancellationToken).ConfigureAwait(false) == false)
                        done = true;

                    if (await AdvanceRightAsync(right, rightKey, cancellationToken).ConfigureAwait(false) == false)
                    {
                        if (done == false && IsLeftOrAnti)
                            remainingLeft = true;
                        else
                            done = true;
                    }

                    if (predicate == null)
                    {
                        if (isAnti)
                        {
                            // a key with a match on the right is not an anti join result
                            if (done)
                                return false;
                            if (remainingLeft)
                                return true;

                            continue;
                        }

                        // a semi join must not repeat a left row, so one right row of the key is enough
                        results = isSemi
                            ? new CartesianCursor<TSource, TInner, TResult>(lefts, [rights[0]], resultSelector)
                            : new CartesianCursor<TSource, TInner, TResult>(lefts, rights, resultSelector);
                    }
                    else
                    {
                        // the rest of the condition still has to hold, and a nested loop over the two runs is
                        // what decides it
                        results = Residual();
                    }

                    return true;
                }
            }

            /// <summary>
            /// Joins the two runs of one key under the predicate, by the nested loop over copies of them.
            /// </summary>
            /// <remarks>
            /// Calcite writes <c>nestedLoopJoin(Linq4j.asEnumerable(lefts), Linq4j.asEnumerable(rights), …)</c>
            /// and obtains its enumerator on the spot; the openers below stand for the second of those, opened
            /// once per left row of the run.
            /// </remarks>
            ClrDataCursor<TResult> Residual()
            {
                var rightRun = new List<TInner>(rights);

                return NestedLoopJoin(
                    new ListCursor<TSource>([.. lefts]),
                    () => new ListCursor<TInner>(rightRun),
                    token => new ValueTask<ClrDataCursor<TInner>>(new ListCursor<TInner>(rightRun)),
                    resultSelector,
                    predicate!,
                    joinType);
            }

            /// <inheritdoc />
            public override bool Read()
            {
                while (true)
                {
                    if (results != null)
                    {
                        if (results.Read())
                        {
                            current = results.Current;
                            return true;
                        }

                        results = null;
                    }

                    if (remainingLeft)
                    {
                        current = resultSelector(outer.Current, default);

                        if (LeftMoveNext() == false)
                        {
                            remainingLeft = false;
                            done = true;
                        }

                        return true;
                    }

                    if (done)
                        return false;

                    if (Advance() == false)
                        return false;
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    if (results != null)
                    {
                        if (await results.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            current = results.Current;
                            return true;
                        }

                        results = null;
                    }

                    if (remainingLeft)
                    {
                        current = resultSelector(outer.Current, default);

                        if (await LeftMoveNextAsync(cancellationToken).ConfigureAwait(false) == false)
                        {
                            remainingLeft = false;
                            done = true;
                        }

                        return true;
                    }

                    if (done)
                        return false;

                    if (await AdvanceAsync(cancellationToken).ConfigureAwait(false) == false)
                        return false;
                }
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                outer.Dispose();
                inner.Dispose();
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                await outer.DisposeAsync().ConfigureAwait(false);
                await inner.DisposeAsync().ConfigureAwait(false);
            }

        }

        /// <summary>
        /// Every pairing of two lists, in order.
        /// </summary>
        /// <remarks>
        /// <c>CartesianProductJoinEnumerator</c>, which extends linq4j's <c>CartesianProductEnumerator</c> and
        /// holds nothing: it advances the last enumerator first and only falls back to the one before it when
        /// that runs out, which is this nesting. Being lazy couples this to the merge join: the two lists are
        /// its own buffers, reused and cleared per key run, so the pairings must be drained before the join
        /// advances. They are -- the driving loop reads all of <c>results</c> before it calls <c>Advance</c>.
        /// Calcite has the same coupling, <c>Linq4j.enumerator(lefts)</c> being a live view of the list it
        /// goes on clearing. Both advances step the same two indexes, and there is nothing to await.
        /// </remarks>
        sealed class CartesianCursor<TSource, TInner, TResult>(IReadOnlyList<TSource> outer, IReadOnlyList<TInner> inner, Func<TSource, TInner, TResult> resultSelector) : ClrDataCursor<TResult>
        {

            int i;
            int j = -1;
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                for (; ; )
                {
                    if (i >= outer.Count)
                        return false;

                    if (++j < inner.Count)
                    {
                        current = resultSelector(outer[i], inner[j]);
                        return true;
                    }

                    i++;
                    j = -1;
                }
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
        /// Raised where a merge join compares two null keys, which it must not call equal.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.BothValuesAreNullException</c>, which is private, so it is written again
        /// rather than reused. It carries no message and is never allowed out of <c>advance</c>.
        /// </remarks>
        sealed class BothValuesAreNullException : Exception
        {

        }

        /// <summary>
        /// Orders two keys with nulls last, refusing to call two nulls equal.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.compareNullsLastForMergeJoin</c>, reached only where no
        /// comparator was given.
        ///
        /// <para>Two nulls are a throw rather than an answer, because there is no answer this method could
        /// give that is right for both of its callers -- calling them equal would join them, and calling
        /// either one bigger is a decision about which side to advance. Calcite leaves that decision to
        /// <c>advance</c>, which catches this and takes 1.</para>
        /// </remarks>
        static int CompareNullsLastForMergeJoin<TKey>(TKey a, TKey b)
        {
            if (a == null && b == null)
                throw new BothValuesAreNullException();

            if (a == null)
                return 1;
            if (b == null)
                return -1;

            // IKVM maps java.lang.Comparable onto IComparable, so this is the key's own compareTo
            return ((IComparable)JavaValues.From(a)).CompareTo(JavaValues.From(b));
        }

        /// <summary>
        /// Returns every left row with a marker saying whether the right side had a match.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the right side synchronously.</param>
        /// <param name="innerAsync">Opens the right side with await.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.leftMarkNestedLoopJoin</c>, which is
        /// <c>leftMarkJoinInternal</c> with a constant inner. The marker is three-valued and the order it is
        /// resolved in matters: false until something is found, null if any comparison was unknown, and true
        /// on the first match, which stops the scan. So an unknown seen before a match is discarded, and one
        /// seen when there is no match is kept — which is what makes <c>IN</c> over a nullable column answer
        /// UNKNOWN rather than FALSE.
        ///
        /// <para>The right side is opened afresh for every left row, inside <c>moveNext</c>, so it arrives as
        /// openers of both kinds.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> LeftMarkNestedLoopJoin<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);

            return LeftMarkJoin<TSource, TInner, TResult>(outer, _ => inner(), (_, token) => innerAsync(token), predicate, resultSelector);
        }

        /// <summary>
        /// <see cref="LeftMarkNestedLoopJoin"/>, over an open that awaits. Nothing but the outer is acquired
        /// at this open; the right side is acquired inside each advance.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> LeftMarkNestedLoopJoinAsync<TSource, TInner, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            CancellationToken cancellationToken)
        {
            return LeftMarkNestedLoopJoin(await outer.ConfigureAwait(false), inner, innerAsync, predicate, resultSelector);
        }

        /// <summary>
        /// The walk both mark joins over a nested loop make.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the right side of one left row synchronously, or answers null for none.</param>
        /// <param name="innerAsync">Opens the right side of one left row with await, or answers null for none.</param>
        /// <param name="predicate"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.leftMarkJoinInternal</c>, which acquires the outer in a
        /// field initializer at <c>enumerator()</c> — the outer arrives opened — and builds and reads each
        /// right side at its left row's turn.
        /// </remarks>
        static ClrDataCursor<TResult> LeftMarkJoin<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(resultSelector);

            return new LeftMarkJoinCursor<TSource, TInner, TResult>(outer, inner, innerAsync, predicate, resultSelector);
        }

        /// <summary>
        /// The row loop of <see cref="LeftMarkJoin"/>.
        /// </summary>
        sealed class LeftMarkJoinCursor<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<TSource, ClrDataCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<ClrDataCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector) : ClrDataCursor<TResult>
        {

            java.lang.Boolean? marker;
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (outer.Read() == false)
                    return false;

                var left = outer.Current;
                marker = java.lang.Boolean.FALSE;
                var rows = inner(left);

                if (rows != null)
                {
                    try
                    {
                        while (rows.Read())
                        {
                            if (Test(left, rows.Current))
                                break;
                        }
                    }
                    finally
                    {
                        rows.Dispose();
                    }
                }

                current = resultSelector(left, marker);
                return true;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                    return false;

                var left = outer.Current;
                marker = java.lang.Boolean.FALSE;
                var rows = await innerAsync(left, cancellationToken).ConfigureAwait(false);

                if (rows != null)
                {
                    try
                    {
                        while (await rows.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            if (Test(left, rows.Current))
                                break;
                        }
                    }
                    finally
                    {
                        await rows.DisposeAsync().ConfigureAwait(false);
                    }
                }

                current = resultSelector(left, marker);
                return true;
            }

            /// <summary>
            /// Folds one comparison into the marker, and returns whether it was the match that ends the scan.
            /// </summary>
            bool Test(TSource left, TInner right)
            {
                var matched = predicate(left, right);

                if (matched == null)
                {
                    marker = null;
                    return false;
                }

                if (matched.booleanValue())
                {
                    marker = java.lang.Boolean.TRUE;
                    return true;
                }

                return false;
            }

            /// <inheritdoc />
            public override void Dispose() => outer.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => outer.DisposeAsync();

        }

        /// <summary>
        /// Joins two inputs on a condition, comparing every pair.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Opens the inner synchronously.</param>
        /// <param name="innerAsync">Opens the inner with await.</param>
        /// <param name="resultSelector"></param>
        /// <param name="predicate"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.nestedLoopJoin</c>, which is one dispatch over two
        /// bodies that are not the same walk, and they are two methods here as they are there. A join that
        /// generates nulls on the left — RIGHT and FULL — goes to <see cref="NestedLoopJoinAsList"/>, which
        /// buffers the inner and builds the whole result before it returns; everything else goes to
        /// <see cref="NestedLoopJoinOptimized"/>, which buffers nothing and streams. The asymmetry is
        /// Calcite's own and is reproduced rather than smoothed over.
        ///
        /// <para>The inner arrives as openers of both kinds because the streaming body opens it once per
        /// outer row, inside <c>moveNext</c>, by whichever advance reached that row; the buffering body opens
        /// it once, at the open, by the opener of the open's own kind.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> NestedLoopJoin<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);

            if (joinType.generatesNullsOnLeft() == false)
                return NestedLoopJoinOptimized(outer, inner, innerAsync, resultSelector, predicate, joinType);

            return NestedLoopJoinAsList(outer, inner, resultSelector, predicate, joinType);
        }

        /// <summary>
        /// <see cref="NestedLoopJoin"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> NestedLoopJoinAsync<TSource, TInner, TResult>(
            ValueTask<ClrDataCursor<TSource>> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);

            var outerCursor = await outer.ConfigureAwait(false);

            if (joinType.generatesNullsOnLeft() == false)
                return NestedLoopJoinOptimized(outerCursor, inner, innerAsync, resultSelector, predicate, joinType);

            return await NestedLoopJoinAsListAsync(outerCursor, innerAsync, resultSelector, predicate, joinType, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Joins two inputs on a condition by building the whole result as a list and returning a cursor
        /// over it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="resultSelector"></param>
        /// <param name="predicate"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.nestedLoopJoinAsList</c>, which is not lazy: the join
        /// runs when the method is called and what comes back is a list already filled, wrapped by
        /// <c>Linq4j.asEnumerable</c>. Here the call is the open, so the whole join runs at the open, and
        /// the inner is opened there by the opener of the open's own kind. This is the only path that reads
        /// the inner into a list, and it reads it once for every outer row from there.
        ///
        /// <para>The rows of the right side that never matched are held in an identity set, because Calcite
        /// holds them in <c>Sets.newIdentityHashSet()</c>. That set deduplicates by reference, so two
        /// unmatched right rows that are the same object are emitted once and not twice — which is not the
        /// answer SQL wants, and is the answer Calcite gives. This is a port and Calcite's behaviour is the
        /// specification, so the defect is reproduced.</para>
        /// </remarks>
        static ClrDataCursor<TResult> NestedLoopJoinAsList<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            var name = joinType.name();
            var generateNullsOnLeft = joinType.generatesNullsOnLeft();
            var generateNullsOnRight = joinType.generatesNullsOnRight();
            var result = new List<TResult>();

            var rightList = new List<TInner>();
            var rows = inner();
            try
            {
                while (rows.Read())
                    rightList.Add(rows.Current);
            }
            finally
            {
                rows.Dispose();
            }

            var rightUnmatched = RightUnmatched(generateNullsOnLeft, rightList);

            try
            {
                while (outer.Read())
                    NestedLoopJoinRow(outer.Current, rightList, rightUnmatched, name, generateNullsOnRight, resultSelector, predicate, result);
            }
            finally
            {
                outer.Dispose();
            }

            if (rightUnmatched != null)
                for (var i = rightUnmatched.iterator(); i.hasNext();)
                    result.Add(resultSelector(default, (TInner)i.next()));

            return new ListCursor<TResult>(result);
        }

        /// <summary>
        /// <see cref="NestedLoopJoinAsList"/>, awaiting each row it reads. The whole join still runs at the
        /// open, which an <see cref="IAsyncEnumerable{T}"/> could not do and a cursor's open can.
        /// </summary>
        static async ValueTask<ClrDataCursor<TResult>> NestedLoopJoinAsListAsync<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType,
            CancellationToken cancellationToken)
        {
            var name = joinType.name();
            var generateNullsOnLeft = joinType.generatesNullsOnLeft();
            var generateNullsOnRight = joinType.generatesNullsOnRight();
            var result = new List<TResult>();

            var rightList = new List<TInner>();
            var rows = await innerAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await rows.ReadAsync(cancellationToken).ConfigureAwait(false))
                    rightList.Add(rows.Current);
            }
            finally
            {
                await rows.DisposeAsync().ConfigureAwait(false);
            }

            var rightUnmatched = RightUnmatched(generateNullsOnLeft, rightList);

            try
            {
                while (await outer.ReadAsync(cancellationToken).ConfigureAwait(false))
                    NestedLoopJoinRow(outer.Current, rightList, rightUnmatched, name, generateNullsOnRight, resultSelector, predicate, result);
            }
            finally
            {
                await outer.DisposeAsync().ConfigureAwait(false);
            }

            if (rightUnmatched != null)
                for (var i = rightUnmatched.iterator(); i.hasNext();)
                    result.Add(resultSelector(default, (TInner)i.next()));

            return new ListCursor<TResult>(result);
        }

        /// <summary>
        /// Returns the set the unmatched right rows are held in, or null where the join owes them nothing.
        /// </summary>
        static java.util.Set? RightUnmatched<TInner>(bool generateNullsOnLeft, List<TInner> rightList)
        {
            if (generateNullsOnLeft == false)
                return null;

            // Sets.newIdentityHashSet(), which is Guava's, backed by an IdentityHashMap. Not a stand-in
            // for it: what comes out of here is the order that map's buckets give, keyed on
            // System.identityHashCode, and nothing written against CLR references reproduces that. We
            // run on IKVM, so the set Calcite uses is available and is the one used.
            var rightUnmatched = com.google.common.collect.Sets.newIdentityHashSet();

            foreach (var right in rightList)
                rightUnmatched.add(right);

            return rightUnmatched;
        }

        /// <summary>
        /// One outer row's turn of <see cref="NestedLoopJoinAsList"/>: its pairings, or the row alone where
        /// the join owes it one.
        /// </summary>
        static void NestedLoopJoinRow<TSource, TInner, TResult>(
            TSource left,
            List<TInner> rightList,
            java.util.Set? rightUnmatched,
            string name,
            bool generateNullsOnRight,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            List<TResult> result)
        {
            var leftMatchCount = 0;

            foreach (var right in rightList)
            {
                if (predicate(left, right))
                {
                    ++leftMatchCount;

                    if (name == nameof(org.apache.calcite.linq4j.JoinType.ANTI))
                    {
                        break;
                    }
                    else
                    {
                        rightUnmatched?.remove(right);

                        // a semi join emits the matched right row, not a null one, and then stops
                        result.Add(resultSelector(left, right));

                        if (name == nameof(org.apache.calcite.linq4j.JoinType.SEMI))
                            break;
                    }
                }
            }

            if (leftMatchCount == 0 && (generateNullsOnRight || name == nameof(org.apache.calcite.linq4j.JoinType.ANTI)))
                result.Add(resultSelector(left, default));
        }

        /// <summary>
        /// Joins two inputs on a condition without building the result as a list first.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="innerAsync"></param>
        /// <param name="resultSelector"></param>
        /// <param name="predicate"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.nestedLoopJoinOptimized</c>, and the same state machine:
        /// state 0 moves the outer, state 1 moves the inner. The inner is opened afresh for every outer
        /// row and never read into a list — <c>nestedLoopJoinAsList</c> is the only body that buffers, and
        /// <c>leftMarkJoinInternal</c> re-opens the same way this does.
        ///
        /// <para>A join type that is none of the six the switch names — ASOF, LEFT_ASOF, LEFT_MARK — falls to
        /// its default, which returns no pair and no unmatched row either, so the whole join is empty. That
        /// is what Calcite does and it is not treated as INNER here.</para>
        ///
        /// <para>Calcite refuses RIGHT and FULL before it returns its enumerable, and the refusal is here,
        /// at the open, rather than in the cursor.</para>
        /// </remarks>
        static ClrDataCursor<TResult> NestedLoopJoinOptimized<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            var name = joinType.name();

            if (name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL))
                throw new ArgumentException($"JoinType {name} is unsupported");

            // nestedLoopJoinOptimized acquires the outer enumerator in a field initializer, which runs at
            // enumerator() -- the outer arrives opened; each inner is opened at its outer row's turn, inside
            // the advance
            return new NestedLoopJoinOptimizedCursor<TSource, TInner, TResult>(outer, inner, innerAsync, resultSelector, predicate, name);
        }

        /// <summary>
        /// The state machine of <see cref="NestedLoopJoinOptimized"/>.
        /// </summary>
        sealed class NestedLoopJoinOptimizedCursor<TSource, TInner, TResult>(
            ClrDataCursor<TSource> outer,
            Func<ClrDataCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<ClrDataCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            string name) : ClrDataCursor<TResult>
        {

            ClrDataCursor<TInner>? innerCursor;
            bool outerMatch; // whether the outerValue has matched an innerValue
            TSource outerValue = default!;
            TInner innerValue = default!;
            int state; // 0 moving outer, 1 moving inner
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (true)
                {
                    switch (state)
                    {
                        case 0:
                            // move outer
                            if (outer.Read() == false)
                                return false;

                            outerValue = outer.Current;
                            innerCursor?.Dispose();
                            innerCursor = inner();
                            outerMatch = false;
                            state = 1;
                            continue;
                        case 1:
                            // move inner
                            if (innerCursor!.Read())
                            {
                                if (Matched(innerCursor.Current))
                                    return true;
                            }
                            else if (InnerOver())
                            {
                                return true;
                            }

                            break;
                        default:
                            break;
                    }
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (true)
                {
                    switch (state)
                    {
                        case 0:
                            // move outer
                            if (await outer.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                                return false;

                            outerValue = outer.Current;
                            if (innerCursor != null)
                                await innerCursor.DisposeAsync().ConfigureAwait(false);
                            innerCursor = await innerAsync(cancellationToken).ConfigureAwait(false);
                            outerMatch = false;
                            state = 1;
                            continue;
                        case 1:
                            // move inner
                            if (await innerCursor!.ReadAsync(cancellationToken).ConfigureAwait(false))
                            {
                                if (Matched(innerCursor.Current))
                                    return true;
                            }
                            else if (InnerOver())
                            {
                                return true;
                            }

                            break;
                        default:
                            break;
                    }
                }
            }

            /// <summary>
            /// Tests the inner row the advance moved onto, and returns whether the pair is a result; the
            /// state is left as the join type wants it for the next advance.
            /// </summary>
            bool Matched(TInner value)
            {
                innerValue = value;

                if (predicate(outerValue, innerValue) == false)
                    return false; // (predicate returned false) continue: move inner

                outerMatch = true;

                switch (name)
                {
                    case nameof(org.apache.calcite.linq4j.JoinType.ANTI): // try next outer row
                        state = 0;
                        return false;
                    case nameof(org.apache.calcite.linq4j.JoinType.SEMI): // return result, and try next outer row
                        state = 0;
                        // Calcite computes the row in current() from the fields the enumerator holds; the
                        // fields are what they would have been there — for SEMI that means the matched
                        // inner row, not a null one
                        current = resultSelector(outerValue, innerValue);
                        return true;
                    case nameof(org.apache.calcite.linq4j.JoinType.INNER):
                    case nameof(org.apache.calcite.linq4j.JoinType.LEFT): // INNER and LEFT just return result
                        current = resultSelector(outerValue, innerValue);
                        return true;
                    default:
                        return false;
                }
            }

            /// <summary>
            /// Moves on from an exhausted inner, and returns whether the outer row is a result on its own.
            /// </summary>
            bool InnerOver()
            {
                state = 0;
                innerValue = default!;

                if (outerMatch == false &&
                    name is nameof(org.apache.calcite.linq4j.JoinType.LEFT) or nameof(org.apache.calcite.linq4j.JoinType.ANTI))
                {
                    // No match detected: outerValue is a result for LEFT / ANTI join
                    current = resultSelector(outerValue, innerValue);
                    return true;
                }

                return false;
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                outer.Dispose();

                var closing = innerCursor;
                innerCursor = null;
                closing?.Dispose();
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                await outer.DisposeAsync().ConfigureAwait(false);

                var closing = innerCursor;
                innerCursor = null;
                if (closing != null)
                    await closing.DisposeAsync().ConfigureAwait(false);
            }

        }

        // ---- Recursion ----


        /// <summary>
        /// Passes every row through, and leaves them in a collection behind it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="collection"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.lazyCollectionSpool</c>. Rows are buffered as they are
        /// read and the collection is replaced by the advance that finds the input exhausted, so it holds one
        /// round rather than everything seen so far. That is what makes the next round of a recursive query
        /// read a delta.
        ///
        /// <para>The input is acquired in a field initializer of linq4j's enumerator, which runs at
        /// <c>enumerator()</c>; it arrives opened here, which is the same moment.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> LazyCollectionSpool<TSource>(java.util.Collection collection, ClrDataCursor<TSource> input)
        {
            ArgumentNullException.ThrowIfNull(collection);
            ArgumentNullException.ThrowIfNull(input);

            return new LazyCollectionSpoolCursor<TSource>(collection, input);
        }

        /// <summary>
        /// <see cref="LazyCollectionSpool{TSource}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> LazyCollectionSpoolAsync<TSource>(java.util.Collection collection, ValueTask<ClrDataCursor<TSource>> input, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(collection);

            return new LazyCollectionSpoolCursor<TSource>(collection, await input.ConfigureAwait(false));
        }

        /// <summary>
        /// The cursor of <see cref="LazyCollectionSpool{TSource}"/>: linq4j's enumerator, whose
        /// <c>moveNext</c> buffers the row it read and flushes the buffer into the collection on every advance
        /// that finds the input exhausted.
        /// </summary>
        sealed class LazyCollectionSpoolCursor<TSource>(java.util.Collection collection, ClrDataCursor<TSource> input) : ClrDataCursor<TSource>
        {

            readonly List<TSource> tempCollection = [];
            TSource current = default!;

            /// <inheritdoc />
            public override TSource Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (input.Read())
                {
                    current = input.Current;
                    tempCollection.Add(current);
                    return true;
                }

                Flush();
                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                if (await input.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    current = input.Current;
                    tempCollection.Add(current);
                    return true;
                }

                Flush();
                return false;
            }

            /// <summary>
            /// Replaces the collection's contents with the round's rows.
            /// </summary>
            void Flush()
            {
                // the collection belongs to the table, and what reads it back is Java — the interpreter, for a
                // transient table neither convention's scan will touch. So this is a boundary and it converts
                collection.clear();
                foreach (var row in tempCollection)
                    collection.add(JavaValues.From(row));

                tempCollection.Clear();
            }

            /// <inheritdoc />
            public override void Dispose() => input.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => input.DisposeAsync();

        }

        /// <summary>
        /// Returns the seed, then the iterative part over and over until it yields nothing.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="seed">The seed, opened.</param>
        /// <param name="iteration">Opens the iterative part synchronously, once per round.</param>
        /// <param name="iterationAsync">Opens the iterative part with await, once per round.</param>
        /// <param name="iterationLimit">A negative value for no limit.</param>
        /// <param name="all">Whether a row already returned is returned again.</param>
        /// <param name="comparer"></param>
        /// <param name="cleanUp">Run once the cursor is disposed, or null.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.repeatUnion</c>, which is what WITH RECURSIVE becomes. The
        /// seed is acquired at <c>enumerator()</c>, in a field initializer, so it arrives opened; the iterative
        /// part is acquired afresh each round inside <c>moveNext</c>, reading what the spool beneath it left
        /// behind, so it arrives as opens — both, because the advance that starts a round is whichever the
        /// consumer called, and the cursor opens with the one of that kind.
        ///
        /// <para>Transcribed from Calcite's enumerator rather than re-expressed, because its termination test
        /// is not the obvious one. It stops when <c>current</c> still holds the <c>DUMMY</c> sentinel after a
        /// round -- not when the round produced nothing. Those differ, and the difference is reproduced here
        /// deliberately: see the note at the seed/iteration boundary.</para>
        ///
        /// <para>The sentinel itself is a flag. Calcite casts a private <c>DUMMY</c> object to the row type and
        /// compares by reference; a row can never be that object, so a <see cref="bool"/> decides exactly what
        /// the reference comparison decides, without a cast that would be a lie about the row type.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> RepeatUnion<TSource>(
            ClrDataCursor<TSource> seed,
            Func<ClrDataCursor<TSource>> iteration,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> iterationAsync,
            int iterationLimit,
            bool all,
            EqualityComparer? comparer,
            Action? cleanUp)
        {
            ArgumentNullException.ThrowIfNull(seed);
            ArgumentNullException.ThrowIfNull(iteration);
            ArgumentNullException.ThrowIfNull(iterationAsync);

            return new RepeatUnionCursor<TSource>(seed, iteration, iterationAsync, iterationLimit, all, comparer, cleanUp);
        }

        /// <summary>
        /// <see cref="RepeatUnion{TSource}"/>, over a seed whose open awaits. The rounds are opened inside
        /// the advances, by the open of the advance's kind.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> RepeatUnionAsync<TSource>(
            ValueTask<ClrDataCursor<TSource>> seed,
            Func<ClrDataCursor<TSource>> iteration,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> iterationAsync,
            int iterationLimit,
            bool all,
            EqualityComparer? comparer,
            Action? cleanUp,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(iteration);
            ArgumentNullException.ThrowIfNull(iterationAsync);

            return new RepeatUnionCursor<TSource>(await seed.ConfigureAwait(false), iteration, iterationAsync, iterationLimit, all, comparer, cleanUp);
        }

        /// <summary>
        /// The cursor of <see cref="RepeatUnion{TSource}"/>: linq4j's repeat union enumerator, with the round
        /// opened by the open matching the advance.
        /// </summary>
        sealed class RepeatUnionCursor<TSource>(
            ClrDataCursor<TSource> seed,
            Func<ClrDataCursor<TSource>> iteration,
            Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> iterationAsync,
            int iterationLimit,
            bool all,
            EqualityComparer? comparer,
            Action? cleanUp) : ClrDataCursor<TSource>
        {

            TSource current = default!;

            // Calcite's `current == DUMMY`. Set false wherever `checkValue` passed and Calcite assigned
            // `current`, and back to true wherever Calcite put the sentinel back.
            bool currentIsDummy = true;

            bool seedProcessed;
            int currentIteration;
            ClrDataCursor<TSource>? iterativeCursor;

            // Calcite's set of wrapped rows, consulted only when `all` is false
            readonly HashSet<TSource>? processed = all ? null : new HashSet<TSource>(JavaEqualityComparer<TSource>.Of(comparer));

            bool disposed;

            /// <inheritdoc />
            public override TSource Current => currentIsDummy ? throw new InvalidOperationException("The cursor is not positioned on a row.") : current;

            /// <summary>
            /// Calcite's <c>checkValue</c>: whether a row is one to return.
            /// </summary>
            bool CheckValue(TSource value)
            {
                return processed == null || processed.Add(value);
            }

            /// <inheritdoc />
            public override bool Read()
            {
                // if we are not done with the seed, advance it
                while (seedProcessed == false)
                {
                    if (seed.Read())
                    {
                        var value = seed.Current;
                        if (CheckValue(value))
                        {
                            current = value;
                            currentIsDummy = false;
                            return true;
                        }
                    }
                    else
                    {
                        seedProcessed = true;
                    }
                }

                // The sentinel is NOT put back here, and that is Calcite's, not an oversight of the
                // transcription. A seed that emitted a row leaves `current` holding that row, so the first
                // iterative round to produce nothing does not stop the sequence -- it goes round once more,
                // and only the second empty round stops it. A recursive query whose step reads the working
                // table row by row cannot tell, because an empty table gives an empty round either way; one
                // whose step aggregates -- COUNT(*) yields a row over no rows -- can, and does.
                for (; ; )
                {
                    if (iterationLimit >= 0 && currentIteration == iterationLimit)
                    {
                        // max number of iterations reached, we are done
                        currentIsDummy = true;
                        return false;
                    }

                    var iterative = iterativeCursor ??= iteration();

                    while (iterative.Read())
                    {
                        var value = iterative.Current;
                        if (CheckValue(value))
                        {
                            current = value;
                            currentIsDummy = false;
                            return true;
                        }
                    }

                    if (currentIsDummy)
                    {
                        // current iteration did not return any value, we are done
                        return false;
                    }

                    // current iteration level (which returned some values) is finished, go to next one
                    currentIsDummy = true;
                    iterative.Dispose();
                    iterativeCursor = null;
                    currentIteration++;
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (seedProcessed == false)
                {
                    if (await seed.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var value = seed.Current;
                        if (CheckValue(value))
                        {
                            current = value;
                            currentIsDummy = false;
                            return true;
                        }
                    }
                    else
                    {
                        seedProcessed = true;
                    }
                }

                for (; ; )
                {
                    if (iterationLimit >= 0 && currentIteration == iterationLimit)
                    {
                        currentIsDummy = true;
                        return false;
                    }

                    var iterative = iterativeCursor ??= await iterationAsync(cancellationToken).ConfigureAwait(false);

                    while (await iterative.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var value = iterative.Current;
                        if (CheckValue(value))
                        {
                            current = value;
                            currentIsDummy = false;
                            return true;
                        }
                    }

                    if (currentIsDummy)
                        return false;

                    currentIsDummy = true;
                    await iterative.DisposeAsync().ConfigureAwait(false);
                    iterativeCursor = null;
                    currentIteration++;
                }
            }

            /// <inheritdoc />
            /// <remarks>
            /// Calcite's <c>close()</c> in its order: the clean-up first, then the two enumerators, once.
            /// </remarks>
            public override void Dispose()
            {
                if (disposed)
                    return;

                disposed = true;

                cleanUp?.Invoke();
                seed.Dispose();
                iterativeCursor?.Dispose();
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                if (disposed)
                    return;

                disposed = true;

                cleanUp?.Invoke();
                await seed.DisposeAsync().ConfigureAwait(false);
                if (iterativeCursor != null)
                    await iterativeCursor.DisposeAsync().ConfigureAwait(false);
            }

        }

        // ---- SetOp ----


        /// <summary>
        /// Returns the rows in both sources.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source">Opens the first source, which is acquired only once the second has been
        /// drained and closed.</param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <param name="all">Whether a row present more than once in each is returned more than once.</param>
        /// <returns></returns>
        /// <remarks>
        /// Drains both inputs <b>at the open</b>, which is linq4j's own timing: <c>EnumerableDefaults.intersect</c>
        /// runs <c>source1.into(set1)</c> in the method body and then reads <c>source0.enumerator()</c>
        /// against the set. So it is the <em>second</em> source that is read first, to completion and
        /// closed, and the first that is acquired afterwards — which is why the first arrives as an open and
        /// the second as a cursor.
        ///
        /// <para>The collections are Calcite's, a <c>java.util.HashSet</c> or Guava's <c>HashMultiset</c>,
        /// because the order a set operator yields its rows in is the order of the collection it held
        /// them in.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> Intersect<TSource>(Func<ClrDataCursor<TSource>> source, ClrDataCursor<TSource> other, EqualityComparer? comparer, bool all)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(other);

            // ALL keeps a row once per pairing, so the collection counts rather than merely holding
            var set1 = Collection(all);
            try
            {
                while (other.Read())
                    set1.add(JavaWrapped.Of(comparer, JavaValues.From(other.Current)));
            }
            finally
            {
                other.Dispose();
            }

            var result = Collection(all);
            var first = source();
            try
            {
                while (first.Read())
                {
                    var o = JavaWrapped.Of(comparer, JavaValues.From(first.Current));
                    if (set1.remove(o))
                        result.add(o);
                }
            }
            finally
            {
                first.Dispose();
            }

            return new ListCursor<TSource>(Unwrap<TSource>(result));
        }

        /// <summary>
        /// <see cref="Intersect{TSource}"/>, over opens that await.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> IntersectAsync<TSource>(Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> source, ValueTask<ClrDataCursor<TSource>> other, EqualityComparer? comparer, bool all, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(source);

            // ALL keeps a row once per pairing, so the collection counts rather than merely holding
            var set1 = Collection(all);
            var second = await other.ConfigureAwait(false);
            try
            {
                while (await second.ReadAsync(cancellationToken).ConfigureAwait(false))
                    set1.add(JavaWrapped.Of(comparer, JavaValues.From(second.Current)));
            }
            finally
            {
                await second.DisposeAsync().ConfigureAwait(false);
            }

            var result = Collection(all);
            var first = await source(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await first.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var o = JavaWrapped.Of(comparer, JavaValues.From(first.Current));
                    if (set1.remove(o))
                        result.add(o);
                }
            }
            finally
            {
                await first.DisposeAsync().ConfigureAwait(false);
            }

            return new ListCursor<TSource>(Unwrap<TSource>(result));
        }

        /// <summary>
        /// Returns the collection a set operator holds its rows in: one that counts them where duplicates are
        /// kept, and one that does not where they are not.
        /// </summary>
        /// <param name="all"></param>
        /// <returns></returns>
        static java.util.Collection Collection(bool all)
        {
            return all ? com.google.common.collect.HashMultiset.create() : new java.util.HashSet();
        }

        /// <summary>
        /// Returns the rows of the first source that are not in the second.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other">Opens the second source, which is acquired only once the first has been
        /// drained and closed.</param>
        /// <param name="comparer"></param>
        /// <param name="all">Whether a row is removed once per appearance in the second rather than entirely.</param>
        /// <returns></returns>
        /// <remarks>
        /// Drains both inputs <b>at the open</b>, which is linq4j's own timing: <c>EnumerableDefaults.except</c>
        /// runs <c>source0.into(collection)</c> in the method body and then reads <c>source1.enumerator()</c>
        /// against it, removing. The second is acquired after the first has been drained and closed, which
        /// is why it arrives as an open rather than as a cursor — the shape <see cref="Union{TSource}"/> has.
        /// </remarks>
        public static ClrDataCursor<TSource> Except<TSource>(ClrDataCursor<TSource> source, Func<ClrDataCursor<TSource>> other, EqualityComparer? comparer, bool all)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(other);

            var collection = Collection(all);
            try
            {
                while (source.Read())
                    collection.add(JavaWrapped.Of(comparer, JavaValues.From(source.Current)));
            }
            finally
            {
                source.Dispose();
            }

            var second = other();
            try
            {
                while (second.Read())
                    collection.remove(JavaWrapped.Of(comparer, JavaValues.From(second.Current)));
            }
            finally
            {
                second.Dispose();
            }

            return new ListCursor<TSource>(Unwrap<TSource>(collection));
        }

        /// <summary>
        /// <see cref="Except{TSource}"/>, over opens that await.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> ExceptAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> other, EqualityComparer? comparer, bool all, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(other);

            var collection = Collection(all);
            var first = await source.ConfigureAwait(false);
            try
            {
                while (await first.ReadAsync(cancellationToken).ConfigureAwait(false))
                    collection.add(JavaWrapped.Of(comparer, JavaValues.From(first.Current)));
            }
            finally
            {
                await first.DisposeAsync().ConfigureAwait(false);
            }

            var second = await other(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await second.ReadAsync(cancellationToken).ConfigureAwait(false))
                    collection.remove(JavaWrapped.Of(comparer, JavaValues.From(second.Current)));
            }
            finally
            {
                await second.DisposeAsync().ConfigureAwait(false);
            }

            return new ListCursor<TSource>(Unwrap<TSource>(collection));
        }

        /// <summary>
        /// Returns the rows of sources that are each already sorted on the key, in that order.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="sources">Opens each source, as a <c>Func&lt;ClrDataCursor&lt;TSource&gt;&gt;</c>.</param>
        /// <param name="sortKeySelector"></param>
        /// <param name="sortComparator"></param>
        /// <param name="all">Whether a row that repeats is kept.</param>
        /// <param name="comparer">Decides whether two rows are the same, where duplicates are dropped.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.mergeUnion</c> and its <c>MergeUnionEnumerator</c>: take
        /// the smallest row across the inputs, emit it, and advance that input alone.
        ///
        /// <para><c>MergeUnionEnumerator</c>'s constructor acquires every input's enumerator, in order, and
        /// then positions each on its first row, all inside <c>enumerator()</c>. Both happen here, at the
        /// open: each source's open is run in turn — the sources arrive as opens so that the acquisitions
        /// run inside this one, one after another, as the constructor's do — and then each cursor is
        /// advanced once. The awaiting open awaits each of those first advances, which is what a cursor lets
        /// an open do and an <c>IAsyncEnumerable</c> could not.</para>
        ///
        /// <para>Dropping duplicates does not need every row emitted so far, only the ones sharing the
        /// current key: the inputs are sorted, so a row that repeats one already emitted arrives before the
        /// key changes. That is Calcite's reasoning and its set is cleared the same way.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> MergeUnion<TSource, TKey>(
            java.util.List sources,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(sources);
            ArgumentNullException.ThrowIfNull(sortKeySelector);
            ArgumentNullException.ThrowIfNull(sortComparator);

            var inputs = new ClrDataCursor<TSource>[sources.size()];
            for (int i = 0; i < inputs.Length; i++)
                inputs[i] = ((Func<ClrDataCursor<TSource>>)sources.get(i))();

            var cursor = new MergeUnionCursor<TSource, TKey>(inputs, sortKeySelector, sortComparator, all, comparer);
            cursor.Init();

            return cursor;
        }

        /// <summary>
        /// <see cref="MergeUnion{TSource, TKey}"/>, over opens that await, each a
        /// <c>Func&lt;CancellationToken, ValueTask&lt;ClrDataCursor&lt;TSource&gt;&gt;&gt;</c>.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> MergeUnionAsync<TSource, TKey>(
            java.util.List sources,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(sources);
            ArgumentNullException.ThrowIfNull(sortKeySelector);
            ArgumentNullException.ThrowIfNull(sortComparator);

            var inputs = new ClrDataCursor<TSource>[sources.size()];
            for (int i = 0; i < inputs.Length; i++)
                inputs[i] = await ((Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>>)sources.get(i))(cancellationToken).ConfigureAwait(false);

            var cursor = new MergeUnionCursor<TSource, TKey>(inputs, sortKeySelector, sortComparator, all, comparer);
            await cursor.InitAsync(cancellationToken).ConfigureAwait(false);

            return cursor;
        }

        /// <summary>
        /// The cursor of <see cref="MergeUnion{TSource, TKey}"/>: linq4j's <c>MergeUnionEnumerator</c>, its
        /// fields held once and stepped by either advance.
        /// </summary>
        sealed class MergeUnionCursor<TSource, TKey>(
            ClrDataCursor<TSource>[] inputs,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer) : ClrDataCursor<TSource>
        {

            readonly TSource[] current = new TSource[inputs.Length];
            readonly bool[] finished = new bool[inputs.Length];
            int active = inputs.Length;

            // only where duplicates are dropped, and only ever holding the rows of one key
            readonly java.util.HashSet? processed = all ? null : new java.util.HashSet();
            object? keyInProcessed;

            TSource currentValue = default!;
            bool positioned;

            /// <inheritdoc />
            public override TSource Current => positioned ? currentValue : throw new InvalidOperationException("The cursor is not positioned on a row.");

            /// <summary>
            /// Positions every input on its first row, as the constructor's <c>initEnumerators</c> does.
            /// </summary>
            public void Init()
            {
                for (int i = 0; i < inputs.Length; i++)
                    Move(i);
            }

            /// <summary>
            /// <see cref="Init"/>, awaiting each input's first advance.
            /// </summary>
            public async ValueTask InitAsync(CancellationToken cancellationToken)
            {
                for (int i = 0; i < inputs.Length; i++)
                    await MoveAsync(i, cancellationToken).ConfigureAwait(false);
            }

            void Move(int i)
            {
                if (inputs[i].Read() == false)
                {
                    active--;
                    finished[i] = true;
                    current[i] = default!;
                }
                else
                {
                    current[i] = inputs[i].Current;
                    finished[i] = false;
                }
            }

            async ValueTask MoveAsync(int i, CancellationToken cancellationToken)
            {
                if (await inputs[i].ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                {
                    active--;
                    finished[i] = true;
                    current[i] = default!;
                }
                else
                {
                    current[i] = inputs[i].Current;
                    finished[i] = false;
                }
            }

            bool NotDuplicated(TSource value)
            {
                if (processed == null)
                    return true;

                var wrapped = JavaWrapped.Of(comparer, JavaValues.From(value));
                if (processed.contains(wrapped))
                    return false;

                var key = JavaValues.From(sortKeySelector(value));
                if (processed.isEmpty() == false)
                {
                    if (sortComparator.compare(key, keyInProcessed) != 0)
                    {
                        processed.clear();
                        keyInProcessed = key;
                    }
                }
                else
                {
                    keyInProcessed = key;
                }

                processed.add(wrapped);
                return true;
            }

            int Compare(TSource a, TSource b)
            {
                return sortComparator.compare(JavaValues.From(sortKeySelector(a)), JavaValues.From(sortKeySelector(b)));
            }

            /// <summary>
            /// Picks the input whose current row sorts first.
            /// </summary>
            int Candidate()
            {
                var candidate = -1;
                for (int i = 0; i < current.Length; i++)
                {
                    if (finished[i] == false)
                    {
                        candidate = i;
                        break;
                    }
                }

                if (active > 1)
                {
                    for (int i = candidate + 1; i < current.Length; i++)
                    {
                        if (finished[i])
                            continue;

                        if (Compare(current[candidate], current[i]) > 0)
                            candidate = i;
                    }
                }

                return candidate;
            }

            /// <inheritdoc />
            public override bool Read()
            {
                while (active > 0)
                {
                    var candidate = Candidate();

                    if (NotDuplicated(current[candidate]))
                    {
                        currentValue = current[candidate];
                        positioned = true;
                        Move(candidate);
                        return true;
                    }

                    Move(candidate);
                }

                return false;
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                while (active > 0)
                {
                    var candidate = Candidate();

                    if (NotDuplicated(current[candidate]))
                    {
                        currentValue = current[candidate];
                        positioned = true;
                        await MoveAsync(candidate, cancellationToken).ConfigureAwait(false);
                        return true;
                    }

                    await MoveAsync(candidate, cancellationToken).ConfigureAwait(false);
                }

                return false;
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                foreach (var input in inputs)
                    input.Dispose();
            }

            /// <inheritdoc />
            public override async ValueTask DisposeAsync()
            {
                foreach (var input in inputs)
                    await input.DisposeAsync().ConfigureAwait(false);
            }

        }

        /// <summary>
        /// Orders rows by a key, then skips and takes.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source">Opens the source, which is not acquired at all for a fetch of no rows.</param>
        /// <param name="keySelector"></param>
        /// <param name="comparator"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.orderBy</c> with a fetch and an offset, which is a sort carrying a limit
        /// rather than a sort followed by one -- and the difference is the whole point of it. CALCITE-3920
        /// and CALCITE-4157 made linq4j keep at most <c>offset + fetch</c> rows: a row whose key sorts at or
        /// after the last key held cannot reach the output and is dropped without being stored, and adding
        /// one evicts the last. <c>ORDER BY x FETCH 10</c> over a million rows holds ten.
        ///
        /// <para>linq4j does all of it inside <c>enumerator()</c>, which is this open: the fetch is tested
        /// first, and for a fetch of no rows it answers <c>Linq4j.emptyEnumerator()</c> <em>without calling
        /// <c>source.enumerator()</c></em>. That is why the source arrives as an open rather than as a
        /// cursor — a cursor would have been acquired already. Otherwise the source is opened, drained into
        /// the bounded map, closed, and the map trimmed by the offset, all before the cursor is handed back.
        /// The awaiting open awaits the drain, which a sequence's <c>GetAsyncEnumerator</c> could not.</para>
        ///
        /// <para>A <c>java.util.TreeMap</c> because that is what linq4j uses, and the reason its own comment
        /// gives: it behaves like the plain <c>orderBy</c> and does better than a heap where there are few
        /// distinct keys. Using Calcite's own structure settles three things at once. It takes the
        /// <c>java.util.Comparator</c> this method is handed, unwrapped. It accepts a null key and routes it
        /// through that comparator — which is the whole job of <c>Functions.nullsComparator</c>, and is how
        /// NULLS FIRST and NULLS LAST are expressed; a <c>SortedDictionary</c> rejects a null key before it
        /// ever consults the comparer. And <c>lastKey</c> and <c>headMap</c> are the operations the
        /// algorithm is written in terms of, in O(log n), where <c>SortedDictionary</c> offers neither.</para>
        ///
        /// <para>One deliberate difference: linq4j stores a one-row group as a <c>Collections.singletonList</c>
        /// and swaps in an <c>ArrayList</c> when a second row arrives. Ours is always a
        /// <see cref="List{T}"/>. That is an allocation difference, not a logical one.</para>
        /// </remarks>
        public static ClrDataCursor<TSource> OrderByWithFetchAndOffset<TSource, TKey>(Func<ClrDataCursor<TSource>> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, java.math.BigDecimal offset, java.math.BigDecimal fetch)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(offset);
            ArgumentNullException.ThrowIfNull(fetch);

            if (fetch.compareTo(java.math.BigDecimal.ZERO) <= 0)
                return new ListCursor<TSource>([]);

            var map = new BoundedMap<TSource>(comparator, offset, fetch);

            // read the input into a tree map
            var cursor = source();
            try
            {
                while (cursor.Read())
                    map.Add(keySelector(cursor.Current), cursor.Current);
            }
            finally
            {
                cursor.Dispose();
            }

            return new ListCursor<TSource>(map.Trimmed());
        }

        /// <summary>
        /// <see cref="OrderByWithFetchAndOffset{TSource, TKey}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TSource>> OrderByWithFetchAndOffsetAsync<TSource, TKey>(Func<CancellationToken, ValueTask<ClrDataCursor<TSource>>> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, java.math.BigDecimal offset, java.math.BigDecimal fetch, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(offset);
            ArgumentNullException.ThrowIfNull(fetch);

            if (fetch.compareTo(java.math.BigDecimal.ZERO) <= 0)
                return new ListCursor<TSource>([]);

            var map = new BoundedMap<TSource>(comparator, offset, fetch);

            // read the input into a tree map
            var cursor = await source(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await cursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                    map.Add(keySelector(cursor.Current), cursor.Current);
            }
            finally
            {
                await cursor.DisposeAsync().ConfigureAwait(false);
            }

            return new ListCursor<TSource>(map.Trimmed());
        }

        /// <summary>
        /// The <c>TreeMap</c> of linq4j's bounded <c>orderBy</c>, holding at most <c>offset + fetch</c> rows,
        /// with the per-row bound and the offset trim written once for both opens to step.
        /// </summary>
        sealed class BoundedMap<TSource>(java.util.Comparator? comparator, java.math.BigDecimal offset, java.math.BigDecimal fetch)
        {

            readonly java.util.TreeMap map = comparator == null ? new java.util.TreeMap() : new java.util.TreeMap(comparator);
            readonly java.math.BigDecimal actualOffset = offset.max(java.math.BigDecimal.ZERO);
            readonly java.math.BigDecimal needed = RowsRequired(offset.max(java.math.BigDecimal.ZERO)).add(RowsRequired(fetch));
            java.math.BigDecimal size = java.math.BigDecimal.ZERO;

            /// <summary>
            /// Adds a row under its key, evicting the last row held once the map is full and the key can
            /// still reach the output; a key that cannot is dropped without being stored.
            /// </summary>
            public void Add(object? key, TSource row)
            {
                if (needed.signum() >= 0 && size.compareTo(needed) >= 0)
                {
                    // the current row will never appear in the output, so just skip it
                    var lastKey = map.lastKey();
                    if (Compare(comparator, key, lastKey) >= 0)
                        return;

                    // remove last entry from tree map, so that we keep at most 'needed' rows
                    var last = (List<TSource>)map.get(lastKey);
                    if (last.Count == 1)
                        map.remove(lastKey);
                    else
                        last.RemoveAt(last.Count - 1);

                    size = size.subtract(java.math.BigDecimal.ONE);
                }

                // add the current element to the map
                if (map.get(key) is List<TSource> held)
                    held.Add(row);
                else
                    map.put(key, new List<TSource> { row });

                size = size.add(java.math.BigDecimal.ONE);
            }

            /// <summary>
            /// Skips the first <c>offset</c> rows by deleting them from the map, and returns what is left
            /// in order; nothing, where the offset is bigger than the number of rows held.
            /// </summary>
            public List<TSource> Trimmed()
            {
                if (actualOffset.compareTo(java.math.BigDecimal.ZERO) > 0)
                {
                    // search the key up to which we have to remove entries from the map
                    var skipped = java.math.BigDecimal.ZERO;
                    var rowsToSkip = RowsRequired(actualOffset);
                    var found = false;
                    var removeUntilInclusive = false;
                    object? until = null;

                    for (var i = map.entrySet().iterator(); i.hasNext();)
                    {
                        var entry = (java.util.Map.Entry)i.next();
                        var rows = (List<TSource>)entry.getValue();
                        skipped = skipped.add(java.math.BigDecimal.valueOf(rows.Count));

                        if (skipped.compareTo(rowsToSkip) >= 0)
                        {
                            // we might need to remove entries from the list
                            var keep = skipped.subtract(rowsToSkip);
                            if (keep.compareTo(java.math.BigDecimal.valueOf(rows.Count)) < 0)
                            {
                                if (keep.signum() == 0)
                                    removeUntilInclusive = true;
                                else
                                    rows.RemoveRange(0, rows.Count - keep.intValueExact());
                            }

                            until = entry.getKey();
                            found = true;
                            break;
                        }
                    }

                    // the offset is bigger than the number of rows in the map
                    if (found == false)
                        return [];

                    map.headMap(until, removeUntilInclusive).clear();
                }

                var ordered = new List<TSource>();
                for (var i = map.values().iterator(); i.hasNext();)
                    ordered.AddRange((List<TSource>)i.next());

                return ordered;
            }

        }

        /// <summary>
        /// The number of rows a FETCH or OFFSET count asks for.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.rowsRequired</c>. A count is a <c>BigDecimal</c> because the expression it
        /// came from need not be an integer, and a fractional one asks for the row it reaches into: CEILING,
        /// not truncation. A negative count asks for nothing.
        /// </remarks>
        static java.math.BigDecimal RowsRequired(java.math.BigDecimal count)
        {
            return count.max(java.math.BigDecimal.ZERO).setScale(0, java.math.RoundingMode.CEILING);
        }

        /// <summary>
        /// Compares two keys the way the map holding them does.
        /// </summary>
        /// <remarks>
        /// The comparator where there is one, which is every case Calcite reaches: <c>GenerateCollationKey</c>
        /// always produces one. The fallback exists because the parameter is nullable and matches what a
        /// <c>TreeMap</c> built without a comparator does — order by the keys themselves. It goes through
        /// <see cref="IComparable"/> rather than <c>java.lang.Comparable</c>, which is a ghost interface a
        /// cast cannot reach from C#.
        /// </remarks>
        static int Compare(java.util.Comparator? comparator, object? x, object? y)
        {
            if (comparator != null)
                return comparator.compare(x, y);

            return Comparer<object>.Default.Compare(x, y);
        }

        // ---- Window ----


        /// <summary>
        /// Evaluates a window's aggregates over every row of every partition.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TAccumulator"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="partitionSelector">Key of the PARTITION BY clause, or null where there is none.</param>
        /// <param name="comparator">Orders the rows of one partition, and compares two of them for EXCLUDE and for RANK.</param>
        /// <param name="exclude">Which rows of the frame the aggregates do not see.</param>
        /// <param name="lowerBound">First index of the frame, before it is clamped to the partition.</param>
        /// <param name="upperBound">Last index of the frame, before it is clamped to the partition.</param>
        /// <param name="alwaysNonEmpty">Whether the bounds can be taken as they are, because the frame always holds the current row.</param>
        /// <param name="clampStart">Whether the lower bound has to be brought back to the first row of the partition.</param>
        /// <param name="clampEnd">Whether the upper bound has to be brought back to the last row of the partition.</param>
        /// <param name="lowerBoundCanChange">Whether the frame's start moves at all, which UNBOUNDED PRECEDING settles.</param>
        /// <param name="accumulatorInitializer"></param>
        /// <param name="reset">Returns the accumulator to its starting value, or null where no aggregate has one.</param>
        /// <param name="adder">Folds one row into the accumulator, or null where no aggregate reads the rows.</param>
        /// <param name="cachedResult">Computes the results that only change when the frame does, or null where every aggregate is recomputed per row.</param>
        /// <param name="uncachedResult">Computes the results that change on every row, or null where there are none.</param>
        /// <param name="selector">Builds the output row from the input row and the results.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of the block <c>EnumerableWindow</c> generates. Everything an aggregate computes is
        /// still Calcite's — the implementors' reset, add and result, and the two frame bounds — and arrives
        /// here already translated; what is written once is the loop those pieces are called from, which
        /// generated Java source is the only place Calcite can put.
        ///
        /// <para>The accumulator carries each aggregate's state and its last result, so a result that does not
        /// change while the frame is intact is computed once and read again, which is the whole point of the
        /// frame bookkeeping. It is made once for the whole window, as Calcite declares its variables once.</para>
        ///
        /// <para>The input is drained into the partitions and the output rows go into a list, and the cursor
        /// handed back is over that list, because that is what the generated block does: it drains its source
        /// into the partition collection, appends each output row to an <c>ArrayList</c> and evaluates to
        /// <c>Linq4j.asEnumerable(list)</c>, once per window group, each group's list being the next group's
        /// source. So the whole window is computed where the expression is evaluated, which in this convention
        /// is the open — in both bodies. The enumerable convention's awaiting twin had to leave the drain to the
        /// first advance, because a <c>GetAsyncEnumerator</c> cannot await; an awaiting open can, and
        /// <see cref="WindowAsync"/> awaits the drain and hands back a cursor over the finished list, exactly as
        /// this does.</para>
        /// </remarks>
        public static ClrDataCursor<TResult> Window<TSource, TKey, TAccumulator, TResult>(
            ClrDataCursor<TSource> source,
            Func<TSource, TKey>? partitionSelector,
            java.util.Comparator comparator,
            org.apache.calcite.rex.RexWindowExclusion exclude,
            Func<WindowFrame, int> lowerBound,
            Func<WindowFrame, int> upperBound,
            bool alwaysNonEmpty,
            bool clampStart,
            bool clampEnd,
            bool lowerBoundCanChange,
            Func<TAccumulator> accumulatorInitializer,
            Func<WindowFrame, TAccumulator, TAccumulator>? reset,
            Func<WindowFrame, TAccumulator, TAccumulator>? adder,
            Func<WindowFrame, TAccumulator, TAccumulator>? cachedResult,
            Func<WindowFrame, TAccumulator, TAccumulator>? uncachedResult,
            Func<WindowFrame, TAccumulator, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(comparator);
            ArgumentNullException.ThrowIfNull(lowerBound);
            ArgumentNullException.ThrowIfNull(upperBound);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(selector);

            var (collection, iterator) = PartitionIterator(source, partitionSelector, comparator);

            return new ListCursor<TResult>(
                WindowRows(collection, iterator, comparator, exclude, lowerBound, upperBound, alwaysNonEmpty, clampStart, clampEnd, lowerBoundCanChange, accumulatorInitializer, reset, adder, cachedResult, uncachedResult, selector));
        }

        /// <summary>
        /// <see cref="Window{TSource, TKey, TAccumulator, TResult}"/>, over an open that awaits. The drain
        /// awaits each row, and the window is computed before the cursor is handed back, as it is in the
        /// synchronous open: there is no first advance for the work to be deferred to, and nothing here
        /// wants one.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> WindowAsync<TSource, TKey, TAccumulator, TResult>(
            ValueTask<ClrDataCursor<TSource>> source,
            Func<TSource, TKey>? partitionSelector,
            java.util.Comparator comparator,
            org.apache.calcite.rex.RexWindowExclusion exclude,
            Func<WindowFrame, int> lowerBound,
            Func<WindowFrame, int> upperBound,
            bool alwaysNonEmpty,
            bool clampStart,
            bool clampEnd,
            bool lowerBoundCanChange,
            Func<TAccumulator> accumulatorInitializer,
            Func<WindowFrame, TAccumulator, TAccumulator>? reset,
            Func<WindowFrame, TAccumulator, TAccumulator>? adder,
            Func<WindowFrame, TAccumulator, TAccumulator>? cachedResult,
            Func<WindowFrame, TAccumulator, TAccumulator>? uncachedResult,
            Func<WindowFrame, TAccumulator, TResult> selector,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(comparator);
            ArgumentNullException.ThrowIfNull(lowerBound);
            ArgumentNullException.ThrowIfNull(upperBound);
            ArgumentNullException.ThrowIfNull(accumulatorInitializer);
            ArgumentNullException.ThrowIfNull(selector);

            var cursor = await source.ConfigureAwait(false);
            var (collection, iterator) = await PartitionIteratorAsync(cursor, partitionSelector, comparator, cancellationToken).ConfigureAwait(false);

            return new ListCursor<TResult>(
                WindowRows(collection, iterator, comparator, exclude, lowerBound, upperBound, alwaysNonEmpty, clampStart, clampEnd, lowerBoundCanChange, accumulatorInitializer, reset, adder, cachedResult, uncachedResult, selector));
        }

        /// <summary>
        /// Walks every partition and evaluates the aggregates for each of its rows.
        /// </summary>
        /// <remarks>
        /// The loop of the generated block, from the first <c>while</c> over the partition iterator to the
        /// <c>clear</c> of the partition collection. Nothing in it reads the input again, so once the drain
        /// has run — synchronously or with await — the two opens share it.
        /// </remarks>
        static List<TResult> WindowRows<TAccumulator, TResult>(
            object collection,
            java.util.Iterator iterator,
            java.util.Comparator comparator,
            org.apache.calcite.rex.RexWindowExclusion exclude,
            Func<WindowFrame, int> lowerBound,
            Func<WindowFrame, int> upperBound,
            bool alwaysNonEmpty,
            bool clampStart,
            bool clampEnd,
            bool lowerBoundCanChange,
            Func<TAccumulator> accumulatorInitializer,
            Func<WindowFrame, TAccumulator, TAccumulator>? reset,
            Func<WindowFrame, TAccumulator, TAccumulator>? adder,
            Func<WindowFrame, TAccumulator, TAccumulator>? cachedResult,
            Func<WindowFrame, TAccumulator, TAccumulator>? uncachedResult,
            Func<WindowFrame, TAccumulator, TResult> selector)
        {
            // an exclusion that is not "no other" makes every frame a fresh one, because the same bounds do not
            // mean the same rows once the current row's peers are taken out of them
            var excluding = exclude == null || exclude.name() != nameof(org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_NO_OTHER);

            var frame = new WindowFrame();
            var accumulator = accumulatorInitializer();

            var list = new List<TResult>(PartitionCollectionSize(collection));

            while (iterator.hasNext())
            {
                var rows = (object[])iterator.next();

                frame.Rows = rows;
                frame.PartitionRowCount = rows.Length;

                var previousStart = -1;
                var previousEnd = int.MaxValue;

                for (int i = 0; i < rows.Length; i++)
                {
                    frame.Index = i;

                    var start = lowerBound(frame);
                    var end = upperBound(frame);

                    if (alwaysNonEmpty)
                    {
                        frame.HasRows = true;
                        frame.Start = start;
                        frame.End = end;
                    }
                    else
                    {
                        var startTmp = clampStart ? Math.Max(start, 0) : start;
                        var endTmp = clampEnd ? Math.Min(end, rows.Length - 1) : end;

                        frame.HasRows = startTmp <= endTmp;
                        frame.Start = frame.HasRows ? startTmp : -1;
                        frame.End = frame.HasRows ? endTmp : -1;
                    }

                    frame.FrameRowCount = frame.HasRows ? frame.End - frame.Start + 1 : 0;

                    // no cached result is no frame to maintain: Calcite drops the whole block in that case,
                    // because nothing would read what it kept
                    if (cachedResult != null)
                    {
                        var lowerChanged = lowerBoundCanChange && frame.Start != previousStart;

                        // the guard is Calcite's, and the exclusion is not part of it: it asks only whether the
                        // bounds moved, so a frame whose bounds are the same for every row — UNBOUNDED PRECEDING
                        // to UNBOUNDED FOLLOWING — is computed for row 0 and never again, and an EXCLUDE on it,
                        // which depends on which row is current, never excludes anything after that row.
                        // EnumerableWindow has the defect and this reproduces it rather than mending it
                        if (lowerChanged || frame.End != previousEnd)
                        {
                            var position = frame.Start;

                            // a frame that only grew at its end is carried on with rather than started again
                            if (excluding || lowerChanged || frame.End < previousEnd)
                                accumulator = reset != null ? reset(frame, accumulator) : accumulator;
                            else
                                position = previousEnd + 1;

                            if (lowerBoundCanChange)
                                previousStart = frame.Start;

                            previousEnd = frame.End;

                            if (adder != null && frame.HasRows)
                            {
                                for (int j = position; j <= frame.End; j++)
                                {
                                    if (Excluded(exclude, comparator, rows, i, j))
                                        continue;

                                    frame.Position = j;
                                    accumulator = adder(frame, accumulator);
                                }
                            }

                            accumulator = cachedResult(frame, accumulator);
                        }
                    }

                    if (uncachedResult != null)
                        accumulator = uncachedResult(frame, accumulator);

                    list.Add(selector(frame, accumulator));
                }
            }

            ClearPartitionCollection(collection);

            return list;
        }

        /// <summary>
        /// Drains the input into the collection the partitions are read from, and returns that collection
        /// and an iterator over the partitions in the window's order.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="partitionSelector"></param>
        /// <param name="comparator"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableWindow.getPartitionIterator</c>, which writes <c>source.into(tempList)</c> for a window
        /// with no PARTITION BY and a <c>foreach</c> over the source into a <c>SortedMultiMap</c> otherwise;
        /// both read the source to its end and close it, and so does this.
        ///
        /// <para><c>SortedMultiMap</c> itself, rather than a dictionary standing in for it. It is a runtime class
        /// of Calcite's and not a generated tree, and it is what decides the order the partitions come out in —
        /// a hash map's, which nothing else reproduces. Being feature compatible with
        /// <c>EnumerableConvention</c> means a query with no ORDER BY gives the rows in the same order, so the
        /// map is the one Calcite uses. It also settles the two questions underneath: a null key is a
        /// partition of its own, and <c>arrays</c> sorts with <c>Arrays.sort</c>, which is stable, so rows the
        /// collation does not separate stay in the order they arrived.</para>
        /// </remarks>
        static (object Collection, java.util.Iterator Iterator) PartitionIterator<TSource, TKey>(ClrDataCursor<TSource> source, Func<TSource, TKey>? partitionSelector, java.util.Comparator comparator)
        {
            try
            {
                if (partitionSelector == null)
                {
                    // one partition, which is iterated even when it is empty
                    var tempList = new java.util.ArrayList();
                    while (source.Read())
                        tempList.add(source.Current);

                    return (tempList, org.apache.calcite.runtime.SortedMultiMap.singletonArrayIterator(comparator, tempList));
                }

                var multiMap = new org.apache.calcite.runtime.SortedMultiMap();
                while (source.Read())
                    multiMap.putMulti(partitionSelector(source.Current), source.Current);

                return (multiMap, multiMap.arrays(comparator));
            }
            finally
            {
                source.Dispose();
            }
        }

        /// <summary>
        /// <see cref="PartitionIterator{TSource, TKey}"/>, awaiting each row of the drain.
        /// </summary>
        static async ValueTask<(object Collection, java.util.Iterator Iterator)> PartitionIteratorAsync<TSource, TKey>(ClrDataCursor<TSource> source, Func<TSource, TKey>? partitionSelector, java.util.Comparator comparator, CancellationToken cancellationToken)
        {
            try
            {
                if (partitionSelector == null)
                {
                    // one partition, which is iterated even when it is empty
                    var tempList = new java.util.ArrayList();
                    while (await source.ReadAsync(cancellationToken).ConfigureAwait(false))
                        tempList.add(source.Current);

                    return (tempList, org.apache.calcite.runtime.SortedMultiMap.singletonArrayIterator(comparator, tempList));
                }

                var multiMap = new org.apache.calcite.runtime.SortedMultiMap();
                while (await source.ReadAsync(cancellationToken).ConfigureAwait(false))
                    multiMap.putMulti(partitionSelector(source.Current), source.Current);

                return (multiMap, multiMap.arrays(comparator));
            }
            finally
            {
                await source.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns the size the generated block would size its output list by.
        /// </summary>
        /// <remarks>
        /// <c>new ArrayList&lt;&gt;(collectionExpr.size())</c>. Calcite names <c>Collection.size</c> on
        /// whichever of the two it has and lets javac resolve it against the receiver, which is the advisory
        /// <c>Method</c> trap in the other direction: over a <c>SortedMultiMap</c> that resolves to
        /// <c>HashMap.size</c> and counts partitions, and over the one-partition list it counts rows. Two
        /// different quantities from one written call, and C# has to dispatch on the type to get both.
        /// </remarks>
        static int PartitionCollectionSize(object collection)
        {
            return collection switch
            {
                java.util.Map map => map.size(),
                java.util.Collection rows => rows.size(),
                _ => 0,
            };
        }

        /// <summary>
        /// Drops the buffered input, as the generated block does before it hands the output list on.
        /// </summary>
        /// <remarks>
        /// <c>collectionExpr.clear()</c>, which Calcite writes as <c>BuiltInMethod.MAP_CLEAR</c> and comments
        /// "allows gc". It is the reason the whole window can be materialised without the input and the output
        /// being alive at once, and the same advisory resolution as the size above -- <c>Map.clear</c> named
        /// on a <c>List</c> is <c>List.clear</c> once javac has seen the receiver.
        /// </remarks>
        static void ClearPartitionCollection(object collection)
        {
            switch (collection)
            {
                case java.util.Map map:
                    map.clear();
                    break;
                case java.util.Collection rows:
                    rows.clear();
                    break;
            }
        }

        /// <summary>
        /// Returns whether a row of the frame is one the aggregates do not see.
        /// </summary>
        /// <param name="exclude"></param>
        /// <param name="comparator"></param>
        /// <param name="rows"></param>
        /// <param name="index">The row being evaluated.</param>
        /// <param name="position">The row that would be folded in.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableWindow.buildExcludeGuard</c>. A peer is a row the window's ordering
        /// does not separate from the current one, which is what the comparator answers.
        /// </remarks>
        static bool Excluded(org.apache.calcite.rex.RexWindowExclusion? exclude, java.util.Comparator comparator, object[] rows, int index, int position)
        {
            return exclude?.name() switch
            {
                nameof(org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_CURRENT_ROW) => index == position,
                nameof(org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_GROUP) => comparator.compare(rows[index], rows[position]) == 0,
                nameof(org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_TIES) => index != position && comparator.compare(rows[index], rows[position]) == 0,
                _ => false,
            };
        }

    }

}
