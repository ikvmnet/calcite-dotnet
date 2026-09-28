using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The operators a plan of the <see cref="ClrCursorConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>EnumerableDefaults</c>. A linq4j operator returns a lazy
    /// <c>Enumerable</c> and acquires its source inside <c>enumerator()</c>; an operator here corresponds to
    /// that <c>enumerator()</c> call. It takes opened cursors and returns an opened cursor, so evaluating a
    /// plan's tree of calls performs the same cascade of acquisitions linq4j performs when the root's
    /// enumerator is obtained. Where linq4j defers an acquisition — <c>concat</c> acquires each source at its
    /// turn inside <c>moveNext</c> — the operator takes that source as a delegate that opens it.
    ///
    /// <para>Each operator comes as a pair of opens. The unsuffixed one takes opened cursors and does any
    /// work it does at open (a sort drains its input) synchronously; the <c>Async</c>-suffixed one takes
    /// awaiting opens, awaits them, and does that work with await. Both return the same cursor class, whose
    /// <see cref="ClrCursor.Read"/> and <see cref="ClrCursor.ReadAsync"/> step one set of fields, each
    /// advancing the inputs with the advance of its own kind. How a plan was opened therefore does not
    /// constrain how it is read. The members below document each pair on the synchronous open; an
    /// <c>Async</c> member's own summary notes any difference beyond awaiting.</para>
    ///
    /// <para>The cursor classes are the operators' bodies, the counterpart of the anonymous
    /// <c>Enumerator</c> classes in <c>EnumerableDefaults</c>, and so live beside the operators.</para>
    /// </remarks>
    static class ClrCursorDefaults
    {

        /// <summary>
        /// Returns the first field of each row, converted to <typeparamref name="TRow"/>.
        /// </summary>
        /// <typeparam name="TRow">The type the first field of each row is converted to.</typeparam>
        /// <param name="source">The input, whose rows are object arrays; it is disposed with the returned
        /// cursor.</param>
        /// <returns>A cursor whose current value is the first field of the input's current row.</returns>
        /// <remarks>
        /// Used where a one-column result is to be read as the value rather than as a one-element row. Mirrors
        /// <c>Enumerables.slice0</c>, which is <c>select(elements -&gt; elements[0])</c>.
        /// </remarks>
        public static IClrCursor<TRow> Slice0<TRow>(IClrCursor<object[]> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new Slice0Cursor<TRow>(source);
        }

        /// <summary>
        /// <see cref="Slice0{TRow}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TRow">The type the first field of each row is converted to.</typeparam>
        /// <param name="source">The awaiting open of the input, whose rows are object arrays.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with a cursor over the first field of each input row once the input is
        /// open.</returns>
        public static async ValueTask<IClrCursor<TRow>> Slice0Async<TRow>(ValueTask<IClrCursor<object[]>> source, CancellationToken cancellationToken)
        {
            return new Slice0Cursor<TRow>(await source.ConfigureAwait(false));
        }

        /// <summary>
        /// The cursor of <see cref="Slice0{TRow}"/>.
        /// </summary>
        /// <typeparam name="TRow">The type the first field of each row is converted to.</typeparam>
        /// <param name="source">The opened input, disposed with this cursor.</param>
        sealed class Slice0Cursor<TRow>(IClrCursor<object[]> source) : ClrCursor<TRow>
        {

            TRow current = default!;

            /// <inheritdoc />
            public override TRow Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                if (source.Read() == false)
                    return false;

                // the field may hold a Java-boxed value, so it is converted rather than cast
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the projected rows.</typeparam>
        /// <param name="source">The opened input, disposed with the returned cursor.</param>
        /// <param name="predicate">Condition each row must satisfy, or null to keep every row.</param>
        /// <param name="selector">Projects an input row that satisfies the condition into an output row.</param>
        /// <returns>A cursor over the projections of the input rows that satisfy the condition, in input
        /// order.</returns>
        /// <remarks>
        /// The counterpart of the anonymous <c>Enumerator</c> Calcite's <c>EnumerableCalc</c> generates:
        /// advancing skips input rows until the condition holds, and the current row is the projection of that
        /// input row.
        /// </remarks>
        public static IClrCursor<TResult> Calc<TSource, TResult>(IClrCursor<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new CalcCursor<TSource, TResult>(source, predicate, selector);
        }

        /// <summary>
        /// <see cref="Calc{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the projected rows.</typeparam>
        /// <param name="source">The awaiting open of the input.</param>
        /// <param name="predicate">Condition each row must satisfy, or null to keep every row.</param>
        /// <param name="selector">Projects an input row that satisfies the condition into an output row.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the filtering and projecting cursor once the input is
        /// open.</returns>
        public static async ValueTask<IClrCursor<TResult>> CalcAsync<TSource, TResult>(ValueTask<IClrCursor<TSource>> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new CalcCursor<TSource, TResult>(await source.ConfigureAwait(false), predicate, selector);
        }

        /// <summary>
        /// The cursor of <see cref="Calc{TSource, TResult}"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the projected rows.</typeparam>
        /// <param name="source">The opened input, disposed with this cursor.</param>
        /// <param name="predicate">Condition each row must satisfy, or null to keep every row.</param>
        /// <param name="selector">Projects a kept input row into the current row.</param>
        sealed class CalcCursor<TSource, TResult>(IClrCursor<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector) : ClrCursor<TResult>
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the projected rows.</typeparam>
        /// <param name="source">The opened input, disposed with the returned cursor.</param>
        /// <param name="selector">Projects an input row into an output row.</param>
        /// <returns>A cursor over the projection of every input row, in input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.select</c>.
        /// </remarks>
        public static IClrCursor<TResult> Select<TSource, TResult>(IClrCursor<TSource> source, Func<TSource, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectCursor<TSource, TResult>(source, selector);
        }

        /// <summary>
        /// <see cref="Select{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the projected rows.</typeparam>
        /// <param name="source">The awaiting open of the input.</param>
        /// <param name="selector">Projects an input row into an output row.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the projecting cursor once the input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> SelectAsync<TSource, TResult>(ValueTask<IClrCursor<TSource>> source, Func<TSource, TResult> selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectCursor<TSource, TResult>(await source.ConfigureAwait(false), selector);
        }

        /// <summary>
        /// The cursor of <see cref="Select{TSource, TResult}"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the projected rows.</typeparam>
        /// <param name="source">The opened input, disposed with this cursor.</param>
        /// <param name="selector">Projects the input's current row into the current row.</param>
        sealed class SelectCursor<TSource, TResult>(IClrCursor<TSource> source, Func<TSource, TResult> selector) : ClrCursor<TResult>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="keySelector">Extracts the sort key from a row.</param>
        /// <param name="comparator">Comparison of two keys, or null to use the key type's default comparer.</param>
        /// <returns>A cursor over the input rows in key order, rows with equal keys keeping their input
        /// order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.orderBy</c>, which drains its whole input inside <c>enumerator()</c>:
        /// the input is drained and disposed here, at the open, and the returned cursor reads the sorted
        /// buffer. The sort is stable, as linq4j's <c>TreeMap</c> of lists is.
        ///
        /// <para>The comparator is a Java <c>Comparator</c> because that is what
        /// <c>PhysType.generateCollationKey</c> produces.</para>
        /// </remarks>
        public static IClrCursor<TSource> OrderBy<TSource, TKey>(IClrCursor<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator)
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
        /// <see cref="OrderBy{TSource, TKey}"/>, over an open that awaits. The drain awaits each row and
        /// completes before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="keySelector">Extracts the sort key from a row.</param>
        /// <param name="comparator">Comparison of two keys, or null to use the key type's default
        /// comparer.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor over the sorted rows once the input has been
        /// drained.</returns>
        public static async ValueTask<IClrCursor<TSource>> OrderByAsync<TSource, TKey>(ValueTask<IClrCursor<TSource>> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, CancellationToken cancellationToken)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="rows">The drained rows, in input order.</param>
        /// <param name="keySelector">Extracts the sort key from a row.</param>
        /// <param name="comparator">Comparison of two keys, or null to use the key type's default
        /// comparer.</param>
        /// <returns>A new list holding the rows in key order.</returns>
        static List<TSource> Sorted<TSource, TKey>(List<TSource> rows, Func<TSource, TKey> keySelector, java.util.Comparator? comparator)
        {
            return comparator == null
                ? rows.OrderBy(keySelector).ToList()
                : rows.OrderBy(keySelector, Comparer<TKey>.Create((x, y) => comparator.compare(x, y))).ToList();
        }

        /// <summary>
        /// A cursor over rows already in hand.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="rows">The rows to return, in order; the list is read, not copied.</param>
        /// <remarks>
        /// Returned by operators that buffer their result. Both advances step one index; there is nothing to
        /// await or dispose.
        /// </remarks>
        sealed class ListCursor<TSource>(IReadOnlyList<TSource> rows) : ClrCursor<TSource>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened input, disposed with the returned cursor.</param>
        /// <param name="count">The number of rows to bypass.</param>
        /// <returns>A cursor over the input rows after the first <paramref name="count"/>.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.skip(source, BigDecimal)</c>, which is <c>skipWhileBigDecimal</c> over
        /// <c>n &lt; count</c>. The count is a <c>BigDecimal</c> so that it holds whatever an OFFSET expression
        /// evaluates to.
        /// </remarks>
        public static IClrCursor<TSource> Skip<TSource>(IClrCursor<TSource> source, java.math.BigDecimal count)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(count);

            return new SkipCursor<TSource>(source, count);
        }

        /// <summary>
        /// <see cref="Skip{TSource}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the input.</param>
        /// <param name="count">The number of rows to bypass.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the skipping cursor once the input is open.</returns>
        public static async ValueTask<IClrCursor<TSource>> SkipAsync<TSource>(ValueTask<IClrCursor<TSource>> source, java.math.BigDecimal count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(count);

            return new SkipCursor<TSource>(await source.ConfigureAwait(false), count);
        }

        /// <summary>
        /// The cursor of <see cref="Skip{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened input, disposed with this cursor.</param>
        /// <param name="count">The number of rows to bypass before the first one returned.</param>
        sealed class SkipCursor<TSource>(IClrCursor<TSource> source, java.math.BigDecimal count) : ClrCursor<TSource>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened input, disposed with the returned cursor.</param>
        /// <param name="count">The largest number of rows to return.</param>
        /// <returns>A cursor over at most the first <paramref name="count"/> input rows.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.take(source, BigDecimal)</c>, which is <c>takeWhileBigDecimal</c>; its
        /// <c>moveNext</c> is <c>enumerator.moveNext() &amp;&amp; predicate(current, ++n)</c> and so draws a row
        /// before testing the count. A satisfied fetch therefore draws one row more than it returns, and a
        /// count of zero still opens the input and draws one row. This reproduces both.
        /// </remarks>
        public static IClrCursor<TSource> Take<TSource>(IClrCursor<TSource> source, java.math.BigDecimal count)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(count);

            return new TakeCursor<TSource>(source, count);
        }

        /// <summary>
        /// <see cref="Take{TSource}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the input.</param>
        /// <param name="count">The largest number of rows to return.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the limiting cursor once the input is open.</returns>
        public static async ValueTask<IClrCursor<TSource>> TakeAsync<TSource>(ValueTask<IClrCursor<TSource>> source, java.math.BigDecimal count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(count);

            return new TakeCursor<TSource>(await source.ConfigureAwait(false), count);
        }

        /// <summary>
        /// The cursor of <see cref="Take{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened input, disposed with this cursor.</param>
        /// <param name="count">The largest number of rows to return.</param>
        sealed class TakeCursor<TSource>(IClrCursor<TSource> source, java.math.BigDecimal count) : ClrCursor<TSource>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="first">Opens the first source synchronously.</param>
        /// <param name="firstAsync">Opens the first source with await.</param>
        /// <param name="second">Opens the second source synchronously.</param>
        /// <param name="secondAsync">Opens the second source with await.</param>
        /// <returns>A cursor over the rows of the first source followed by those of the second; neither source has
        /// been opened yet.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.concat</c>, which is <c>Linq4j.concat</c> over the two.
        /// <c>CompositeEnumerable</c>'s <c>enumerator()</c> acquires nothing and each source is acquired at its
        /// turn inside <c>moveNext</c>, so this takes the sources as opens and runs each when the cursor
        /// reaches it.
        ///
        /// <para>Each source is given with both of its opens because the acquisition happens inside whichever
        /// advance the consumer calls: <c>Read</c> reaching a source calls its synchronous open, and
        /// <c>ReadAsync</c> calls its awaiting open with the token that advance was given. The operator itself
        /// acquires nothing at open, so <see cref="ConcatAsync{TSource}"/> completes at once.</para>
        /// </remarks>
        public static IClrCursor<TSource> Concat<TSource>(
            Func<IClrCursor<TSource>> first,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> firstAsync,
            Func<IClrCursor<TSource>> second,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> secondAsync)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="first">Opens the first source synchronously.</param>
        /// <param name="firstAsync">Opens the first source with await.</param>
        /// <param name="second">Opens the second source synchronously.</param>
        /// <param name="secondAsync">Opens the second source with await.</param>
        /// <param name="cancellationToken">Unused: nothing is awaited at this open, and each advance of the
        /// returned cursor takes its own token.</param>
        /// <returns>An already completed open of the concatenating cursor.</returns>
        public static ValueTask<IClrCursor<TSource>> ConcatAsync<TSource>(
            Func<IClrCursor<TSource>> first,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> firstAsync,
            Func<IClrCursor<TSource>> second,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> secondAsync,
            CancellationToken cancellationToken)
        {
            return new ValueTask<IClrCursor<TSource>>(Concat(first, firstAsync, second, secondAsync));
        }

        /// <summary>
        /// The cursor of <see cref="Concat{TSource}"/>, the counterpart of <c>CompositeEnumerable</c>'s
        /// enumerator. Each source is opened by the open matching the advance that reaches it.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="opens">The synchronous opens of the sources, in order, called by <c>Read</c> as it reaches
        /// each.</param>
        /// <param name="opensAsync">The awaiting opens of the same sources, called by <c>ReadAsync</c> as it
        /// reaches each.</param>
        sealed class ConcatCursor<TSource>(Func<IClrCursor<TSource>>[] opens, Func<CancellationToken, ValueTask<IClrCursor<TSource>>>[] opensAsync) : ClrCursor<TSource>
        {

            IClrCursor<TSource>? current;
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The first source, drained and disposed at the open.</param>
        /// <param name="other">Opens the second source, which is acquired only once the first has been
        /// drained and disposed.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <returns>A cursor over the distinct rows of both inputs, in the iteration order of the set they were
        /// collected in.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.union</c>, which runs <c>source0.into(set)</c> and then
        /// <c>source1.into(set)</c> in the method body and returns <c>Linq4j.asEnumerable(set)</c>: both inputs
        /// are drained at the open. The set is a <c>java.util.HashSet</c> because the order rows are returned
        /// in is that set's iteration order.
        /// </remarks>
        public static IClrCursor<TSource> Union<TSource>(IClrCursor<TSource> source, Func<IClrCursor<TSource>> other, EqualityComparer? comparer)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the first source, which is drained and disposed at the
        /// open.</param>
        /// <param name="other">Opens the second source with await, once the first has been drained and
        /// disposed.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <param name="cancellationToken">Passed to the second source's open and to each advance of both
        /// drains.</param>
        /// <returns>The open, completing with a cursor over the distinct rows once both sources have been
        /// drained.</returns>
        public static async ValueTask<IClrCursor<TSource>> UnionAsync<TSource>(ValueTask<IClrCursor<TSource>> source, Func<CancellationToken, ValueTask<IClrCursor<TSource>>> other, EqualityComparer? comparer, CancellationToken cancellationToken)
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
        /// <typeparam name="TSource">The type each value is converted back to.</typeparam>
        /// <param name="collection">A Java collection of rows, each possibly wrapped by
        /// <c>JavaWrapped.Of</c>.</param>
        /// <returns>The unwrapped rows, in the collection's iteration order.</returns>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The rows; the array is read, not copied.</param>
        /// <returns>A cursor over the array's elements, in order.</returns>
        /// <remarks>
        /// Used for a VALUES clause, where Calcite uses <c>Linq4j.asEnumerable</c>.
        /// </remarks>
        public static IClrCursor<TSource> AsCursor<TSource>(TSource[] source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new ListCursor<TSource>(source);
        }

        /// <summary>
        /// <see cref="AsCursor{TSource}(TSource[])"/>, as an open that awaits. There is nothing to await, so
        /// it completes at once.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The rows; the array is read, not copied.</param>
        /// <param name="cancellationToken">Unused: nothing is awaited at this open, and each advance of the
        /// returned cursor takes its own token.</param>
        /// <returns>An already completed open of a cursor over the array's elements.</returns>
        public static ValueTask<IClrCursor<TSource>> AsCursorAsync<TSource>(TSource[] source, CancellationToken cancellationToken)
        {
            return new ValueTask<IClrCursor<TSource>>(AsCursor(source));
        }

        /// <summary>
        /// Returns a cursor over a .NET sequence, acquiring its enumerator here.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The sequence, whose enumerator is obtained before this method returns and disposed
        /// with the returned cursor.</param>
        /// <returns>A cursor over the sequence's elements, in enumeration order.</returns>
        /// <remarks>
        /// Used for a scan of an <see cref="Schema.IClrScannableTable"/> or an
        /// <see cref="Schema.IClrQueryableTable"/>. <see cref="IEnumerable{T}.GetEnumerator"/> is where the
        /// sequence starts running, so it is called at the open. The cursor's
        /// <see cref="ClrCursor.ReadAsync"/> pulls the enumerator synchronously.
        /// </remarks>
        public static IClrCursor<TSource> AsCursor<TSource>(IEnumerable<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new EnumeratorCursor<TSource>(source.GetEnumerator());
        }

        /// <summary>
        /// Returns a cursor over an asynchronous .NET sequence, acquiring its enumerator here.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The sequence, whose asynchronous enumerator is obtained before this method returns
        /// and disposed with the returned cursor.</param>
        /// <param name="cancellationToken">The token passed to <c>GetAsyncEnumerator</c>, under which the whole
        /// enumeration runs. The token given to each advance is only checked before that advance starts.</param>
        /// <returns>An already completed open of a cursor over the sequence's elements.</returns>
        /// <remarks>
        /// The counterpart of <see cref="AsCursor{TSource}(IEnumerable{TSource})"/> for the awaiting half of
        /// the table SPI. <c>GetAsyncEnumerator</c> does not await, so this open completes at once. The cursor's
        /// <see cref="ClrCursor.Read"/> blocks the calling thread for each row, with the synchronization
        /// context suppressed.
        /// </remarks>
        public static ValueTask<IClrCursor<TSource>> AsCursorAsync<TSource>(IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(source);

            return new ValueTask<IClrCursor<TSource>>(new AsyncEnumeratorCursor<TSource>(source.GetAsyncEnumerator(cancellationToken)));
        }

        /// <summary>
        /// Reads a cursor plan as a sequence, opening it at <see cref="IEnumerable{T}.GetEnumerator"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="open">The plan's synchronous open, run once per enumerator.</param>
        /// <returns>A sequence that opens the plan each time an enumerator is obtained and disposes the cursor
        /// with the enumerator.</returns>
        /// <remarks>
        /// Used where a plan's rows are handed to a caller as a sequence. The open is taken as a delegate so that
        /// each enumeration runs the plan afresh at <c>GetEnumerator</c>.
        /// </remarks>
        public static IEnumerable<TSource> AsEnumerable<TSource>(Func<IClrCursor<TSource>> open)
        {
            ArgumentNullException.ThrowIfNull(open);

            return new ClrEnumerable<TSource>(() => new CursorEnumerator<TSource>(open()));
        }

        /// <summary>
        /// Reads a cursor plan as an asynchronous sequence, opening it on the first advance.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="open">The plan's awaiting open, run once per enumerator.</param>
        /// <param name="cancellationToken">Unused. The token given to <c>GetAsyncEnumerator</c> is passed to the
        /// open and to every advance.</param>
        /// <returns>An asynchronous sequence that opens the plan on the first advance of each enumerator and
        /// disposes the cursor with the enumerator.</returns>
        /// <remarks>
        /// The awaiting counterpart of <see cref="AsEnumerable{TSource}"/>. <c>GetAsyncEnumerator</c> cannot
        /// await, so the open runs inside the first <c>MoveNextAsync</c>, later than linq4j's
        /// <c>enumerator()</c> would acquire it.
        /// </remarks>
        public static IAsyncEnumerable<TSource> AsAsyncEnumerable<TSource>(Func<CancellationToken, ValueTask<IClrCursor<TSource>>> open, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(open);

            return new ClrAsyncEnumerable<TSource>(token => new CursorAsyncEnumerator<TSource>(open, token));
        }

        /// <summary>
        /// A .NET enumerator over an opened cursor.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="cursor">The opened cursor, advanced by <c>MoveNext</c> and disposed with this
        /// enumerator.</param>
        sealed class CursorEnumerator<TSource>(IClrCursor<TSource> cursor) : IEnumerator<TSource>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="open">The plan's awaiting open, run on the first call to <c>MoveNextAsync</c>.</param>
        /// <param name="cancellationToken">The token given to <c>GetAsyncEnumerator</c>, passed to the open and to
        /// every advance.</param>
        sealed class CursorAsyncEnumerator<TSource>(Func<CancellationToken, ValueTask<IClrCursor<TSource>>> open, CancellationToken cancellationToken) : IAsyncEnumerator<TSource>
        {

            IClrCursor<TSource>? cursor;

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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The enumerator, already obtained; both advances call its <c>MoveNext</c>, and it
        /// is disposed with this cursor.</param>
        sealed class EnumeratorCursor<TSource>(IEnumerator<TSource> source) : ClrCursor<TSource>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The asynchronous enumerator, already obtained under the token of the open; it is
        /// disposed with this cursor.</param>
        sealed class AsyncEnumeratorCursor<TSource>(IAsyncEnumerator<TSource> source) : ClrCursor<TSource>
        {

            /// <inheritdoc />
            public override TSource Current => source.Current;

            /// <inheritdoc />
            /// <remarks>
            /// Blocks the calling thread for the row, with the synchronization context suppressed before the
            /// advance is called.
            /// </remarks>
            public override bool Read() => ClrCursors.BlockRead(this);

            /// <inheritdoc />
            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                // MoveNextAsync takes no token; the sequence runs under the one it was opened with, so this
                // advance's token can only stop the read before it starts
                cancellationToken.ThrowIfCancellationRequested();

                return source.MoveNextAsync();
            }

            /// <inheritdoc />
            public override void Dispose() => ClrCursors.BlockDispose(source);

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        // ---- Aggregate ----


        /// <summary>
        /// Returns the distinct rows of a cursor.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <returns>A cursor over the distinct rows, in the iteration order of the set they were collected
        /// in.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.distinct</c>, which drains its input into a <c>HashSet</c> where it is
        /// called and returns <c>Linq4j.asEnumerable(set)</c>: the input is drained and disposed at the open. The
        /// set is a <c>java.util.HashSet</c> because the order rows are returned in is that set's iteration
        /// order.
        /// </remarks>
        public static IClrCursor<TSource> Distinct<TSource>(IClrCursor<TSource> source, EqualityComparer? comparer)
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
        /// <see cref="Distinct{TSource}"/>, over an open that awaits. The drain awaits each row and completes
        /// before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor over the distinct rows once the input has been
        /// drained.</returns>
        public static async ValueTask<IClrCursor<TSource>> DistinctAsync<TSource>(ValueTask<IClrCursor<TSource>> source, EqualityComparer? comparer, CancellationToken cancellationToken)
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="keySelector">Extracts the grouping key from a row.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <returns>A cursor with one row per group, in the iteration order of the map the groups were folded
        /// in.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.groupBy</c>. The three accumulator functions are linq4j functional
        /// interfaces because Calcite's <c>AggregateLambdaFactory</c> produces them.
        /// <para>The fold runs at the open, as <c>groupBy_</c> drains its input into the map where it is called
        /// and returns a <c>LookupResultEnumerable</c> over the finished map. The returned cursor reads that map,
        /// applying the result selector a group at a time. The map is a <c>java.util.HashMap</c>, as Calcite's
        /// is, because groups are returned in its iteration order.</para>
        /// </remarks>
        public static IClrCursor<TResult> GroupBy<TSource, TKey, TResult>(
            IClrCursor<TSource> source,
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
        /// and completes before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="keySelector">Extracts the grouping key from a row.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor of one row per group once the input has been
        /// folded.</returns>
        public static async ValueTask<IClrCursor<TResult>> GroupByAsync<TSource, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> source,
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping keys; every selector produces this type.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="keySelectors">One selector per grouping set.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <returns>A cursor with one row per distinct key across all grouping sets, in the iteration order of the
        /// map.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.groupByMultiple</c>, which supports <c>GROUPING SETS</c>. Every row is
        /// offered to every selector, so one pass folds it into one group per grouping set, all in one map.
        /// <para>As with <see cref="GroupBy{TSource, TKey, TResult}"/>, the fold runs at the open and the map is
        /// a <c>java.util.HashMap</c>.</para>
        /// </remarks>
        public static IClrCursor<TResult> GroupByMultiple<TSource, TKey, TResult>(
            IClrCursor<TSource> source,
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
        /// each row and completes before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping keys; every selector produces this type.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="keySelectors">One selector per grouping set.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor of one row per distinct key once the input has been
        /// folded.</returns>
        public static async ValueTask<IClrCursor<TResult>> GroupByMultipleAsync<TSource, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> source,
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
        /// A cursor over a finished map of accumulators, applying the result selector a group at a time.
        /// </summary>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="map">The finished accumulators, keyed by the wrapped grouping key.</param>
        /// <param name="resultSelector">Turns an unwrapped key and its accumulator into the current row.</param>
        /// <remarks>
        /// The counterpart of linq4j's <c>LookupResultEnumerable</c> iterator: the map is walked in its own
        /// order and the selector is applied as each entry is reached. The input has already been drained and
        /// disposed, so there is nothing to dispose or await.
        /// </remarks>
        sealed class LookupResultCursor<TResult>(java.util.Map map, Function2 resultSelector) : ClrCursor<TResult>
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The opened input, whose rows with equal keys are adjacent; disposed with the
        /// returned cursor.</param>
        /// <param name="keySelector">Extracts the grouping key from a row.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparator">Decides where one group ends and the next begins.</param>
        /// <returns>A cursor with one row per run of equal keys, in input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.sortedGroupBy</c> and its <c>SortedAggregateEnumerator</c>. Only the
        /// accumulator of the current group is held, and groups are returned in input order. Nothing is read at
        /// the open; the walk happens in the advances.
        /// </remarks>
        public static IClrCursor<TResult> SortedGroupBy<TSource, TKey, TResult>(
            IClrCursor<TSource> source,
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
        /// <see cref="SortedGroupBy{TSource, TKey, TResult}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The awaiting open of the input, whose rows with equal keys are adjacent.</param>
        /// <param name="keySelector">Extracts the grouping key from a row.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparator">Decides where one group ends and the next begins.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the grouping cursor once the input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> SortedGroupByAsync<TSource, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> source,
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
        /// The cursor of <see cref="SortedGroupBy{TSource, TKey, TResult}"/>, mirroring linq4j's
        /// <c>SortedAggregateEnumerator</c>.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the grouping key.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The opened input, whose rows with equal keys are adjacent; disposed with this
        /// cursor.</param>
        /// <param name="keySelector">Extracts the grouping key from a row.</param>
        /// <param name="accumulatorInitializer">Makes an empty accumulator for a new group.</param>
        /// <param name="accumulatorAdder">Folds a row into a group's accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns a group's key and finished accumulator into an output row.</param>
        /// <param name="comparator">Compares two adjacent keys; a nonzero result ends the group.</param>
        /// <remarks>
        /// An advance folds the row the source is positioned on — the first row, or the row that ended the
        /// previous group — and reads on until the key changes or the input ends. On a key change the group's
        /// result is taken, a fresh accumulator is made, and the row that changed the key stays at the source's
        /// position for the next advance. At the end of input the accumulator is set to null, which tells the
        /// next advance there is nothing more. linq4j detects a result taken at a key change by testing the
        /// result for null; that test cannot be written on <typeparamref name="TResult"/>, so a flag is used.
        /// </remarks>
        sealed class SortedAggregateCursor<TSource, TKey, TResult>(
            IClrCursor<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            java.util.Comparator comparator) : ClrCursor<TResult>
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

                // null means no accumulator has been made yet; like linq4j, this assumes the adder never
                // returns null
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
                        // the row that changed the key stays at the source's position for the next advance
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
                    // input ended: the null accumulator makes the next advance return false
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

                // null means no accumulator has been made yet; like linq4j, this assumes the adder never
                // returns null
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
                        // the row that changed the key stays at the source's position for the next advance
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
                    // input ended: the null accumulator makes the next advance return false
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the folded result.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="seed">The initial accumulator, returned to the result selector unchanged if the input is
        /// empty.</param>
        /// <param name="accumulatorAdder">Folds a row into the accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns the final accumulator into the result.</param>
        /// <returns>The result of folding every row, converted to <typeparamref name="TResult"/>.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.aggregate</c>, used for an aggregate with no GROUP BY. Like linq4j's,
        /// it returns the value rather than a sequence: the input is drained and disposed where it is called,
        /// and <see cref="Singleton{TSource}"/> turns the result into a cursor.
        /// </remarks>
        public static TResult Aggregate<TSource, TResult>(IClrCursor<TSource> source, object seed, Function2 accumulatorAdder, Function1 resultSelector)
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
        /// <typeparam name="TSource">The type of the row.</typeparam>
        /// <param name="element">The one row.</param>
        /// <returns>A cursor that returns <paramref name="element"/> once.</returns>
        /// <remarks>
        /// Mirrors <c>Linq4j.singletonEnumerable</c>.
        /// </remarks>
        public static IClrCursor<TSource> Singleton<TSource>(TSource element)
        {
            return new ListCursor<TSource>([element]);
        }

        /// <summary>
        /// Folds every row into one, over an open that awaits, and returns the cursor of that one row.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the folded result.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="seed">The initial accumulator, returned to the result selector unchanged if the input is
        /// empty.</param>
        /// <param name="accumulatorAdder">Folds a row into the accumulator, returning the accumulator to
        /// keep.</param>
        /// <param name="resultSelector">Turns the final accumulator into the result.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor of the one folded row once the input has been
        /// drained.</returns>
        /// <remarks>
        /// The awaiting counterpart of <c>Singleton(Aggregate(source, …))</c>. The synchronous body composes
        /// those two calls in the expression tree; an expression tree cannot await the fold, so the awaiting
        /// body calls this single operator instead. The fold runs once, at the open.
        /// </remarks>
        public static async ValueTask<IClrCursor<TResult>> SingletonAggregateAsync<TSource, TResult>(
            ValueTask<IClrCursor<TSource>> source,
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
        // Operators for collect, uncollect and combine.


        /// <summary>
        /// Reads every row into a Java list, disposing the cursor once it is read.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened cursor, drained and disposed before this method returns.</param>
        /// <returns>A new <c>java.util.ArrayList</c> holding every row, in order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.toList</c>, which is <c>source.into(new ArrayList())</c>. The result is
        /// a <c>java.util.List</c> because it becomes a field value that Calcite's code reads.
        /// </remarks>
        public static java.util.List ToJavaList<TSource>(IClrCursor<TSource> source)
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
        /// Reads every row into a Java map, keeping the order the keys were first seen in, and disposes the
        /// cursor once it is read.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened cursor, drained and disposed before this method returns.</param>
        /// <param name="keySelector">Extracts a row's key; a later row with an equal key replaces the earlier
        /// row's value.</param>
        /// <param name="valueSelector">Extracts a row's value.</param>
        /// <returns>A new <c>java.util.LinkedHashMap</c> from each key to the value of the last row carrying
        /// it.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.toMap</c>, which drains into a <c>LinkedHashMap</c>.
        /// </remarks>
        public static java.util.Map ToJavaMap<TSource>(IClrCursor<TSource> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector)
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the rows of each yielded sequence.</typeparam>
        /// <param name="source">The opened input, disposed with the returned cursor.</param>
        /// <param name="selector">Returns a linq4j <c>Enumerable</c> for one row; Calcite generates it.</param>
        /// <returns>A cursor over the rows of every yielded sequence, in input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.selectMany</c>: each row's sequence is built and acquired at its turn,
        /// inside the advance.
        /// </remarks>
        public static IClrCursor<TResult> SelectMany<TSource, TResult>(IClrCursor<TSource> source, Function1 selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectManyCursor<TSource, TResult>(source, selector);
        }

        /// <summary>
        /// The cursor of <see cref="SelectMany{TSource, TResult}"/>, mirroring linq4j's <c>selectMany</c>
        /// enumerator, with each row's sequence read through <see cref="JavaCursors.FromJava{TSource}"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the rows of each yielded sequence.</typeparam>
        /// <param name="source">The opened input, disposed with this cursor.</param>
        /// <param name="selector">Returns a linq4j <c>Enumerable</c> for one input row.</param>
        /// <remarks>
        /// The inner sequence is a linq4j <c>Enumerable</c> and is pulled synchronously by either advance.
        /// A null inner cursor stands where linq4j holds <c>Linq4j.emptyEnumerator()</c>: before the first row
        /// and between one row's sequence and the next.
        /// </remarks>
        sealed class SelectManyCursor<TSource, TResult>(IClrCursor<TSource> source, Function1 selector) : ClrCursor<TResult>
        {

            IClrCursor<TResult>? result;

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
        /// <typeparam name="TSource">The type the list's elements are read as.</typeparam>
        /// <param name="source">The list, read in order and not copied.</param>
        /// <returns>A cursor over the list's elements.</returns>
        /// <remarks>
        /// <c>Linq4j.asEnumerable(List)</c>, read through <see cref="JavaCursors.FromJava{TSource}"/>.
        /// </remarks>
        public static IClrCursor<TSource> FromJavaList<TSource>(java.util.List source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return JavaCursors.FromJava<TSource>(org.apache.calcite.linq4j.Linq4j.asEnumerable(source));
        }

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="SelectMany{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TResult">The type of the rows of each yielded sequence.</typeparam>
        /// <param name="source">The awaiting open of the input.</param>
        /// <param name="selector">Returns a linq4j <c>Enumerable</c> for one row; Calcite generates it.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the flattening cursor once the input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> SelectManyAsync<TSource, TResult>(ValueTask<IClrCursor<TSource>> source, Function1 selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectManyCursor<TSource, TResult>(await source.ConfigureAwait(false), selector);
        }

        /// <summary>
        /// Reads a whole cursor into a Java list and hands back the one row holding it.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor of one row, the list of every input row.</returns>
        /// <remarks>
        /// The awaiting counterpart of <c>Singleton(ToJavaList(source))</c>, which the synchronous body composes
        /// in the expression tree; an expression tree cannot await the drain, so the awaiting body calls this
        /// single operator. The drain completes at the open.
        /// </remarks>
        public static async ValueTask<IClrCursor<java.util.List>> SingletonJavaListAsync<TSource>(ValueTask<IClrCursor<TSource>> source, CancellationToken cancellationToken)
        {
            return Singleton(await ToJavaListAsync(source, cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Reads a whole cursor into a Java map and hands back the one row holding it.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="keySelector">Extracts a row's key; a later row with an equal key replaces the earlier
        /// row's value.</param>
        /// <param name="valueSelector">Extracts a row's value.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor of one row, the map built from every input row.</returns>
        /// <remarks>
        /// The awaiting counterpart of <c>Singleton(ToJavaMap(source, …))</c>, for the reason
        /// <see cref="SingletonJavaListAsync{TSource}"/> gives. The map is a <c>LinkedHashMap</c>, as in
        /// <see cref="ToJavaMap{TSource}"/>.
        /// </remarks>
        public static async ValueTask<IClrCursor<java.util.Map>> SingletonJavaMapAsync<TSource>(ValueTask<IClrCursor<TSource>> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector, CancellationToken cancellationToken)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the input, which is drained and disposed before the returned
        /// task completes.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>A task completing with a new <c>java.util.ArrayList</c> holding every row, in order.</returns>
        /// <remarks>
        /// <see cref="ToJavaList{TSource}"/>, over an open that awaits. A helper for the awaiting operators
        /// that drain their input before returning a row; plans do not call it directly, because an expression
        /// tree cannot await its result, so <see cref="ClrCursorBuiltInMethod"/> does not name it.
        /// </remarks>
        public static async ValueTask<java.util.List> ToJavaListAsync<TSource>(ValueTask<IClrCursor<TSource>> source, CancellationToken cancellationToken)
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
        /// Opens and reads each input into a Java list, one after another, combines the lists, and returns a
        /// cursor over the combined rows.
        /// </summary>
        /// <typeparam name="TResult">The type the combined list's elements are read as.</typeparam>
        /// <param name="sources">Opens each input, called at its turn.</param>
        /// <param name="combine">Combines the lists once all are read; the node passes
        /// <c>SqlFunctions.combineQueryResults</c>.</param>
        /// <param name="cancellationToken">Passed to each input's open and to each advance of its drain.</param>
        /// <returns>The open, completing with a cursor over the combined list once every input has been
        /// read.</returns>
        /// <remarks>
        /// The awaiting body of a combine. The synchronous body reads each input into a list within the
        /// expression tree; an expression tree cannot await, so the awaiting body calls this operator.
        ///
        /// <para>The inputs are taken as opens so that each is opened only after the one before it has been
        /// read and disposed. That is the order of Calcite's generated <c>bind</c>, which reads <c>list0</c>
        /// to completion before touching <c>child1</c>, and of the synchronous body, where each
        /// <c>ToJavaList</c> in the array initializer completes before the next element is evaluated. Awaiting
        /// opens passed as values would already have started every input.</para>
        /// </remarks>
        public static async ValueTask<IClrCursor<TResult>> CombineQueryResultsAsync<TResult>(
            Func<CancellationToken, ValueTask<IClrCursor<java.util.Map>>>[] sources,
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
        /// <see cref="FromJavaList{TSource}"/>, as an open that awaits. There is nothing to await, so it
        /// completes at once.
        /// </summary>
        /// <typeparam name="TSource">The type the list's elements are read as.</typeparam>
        /// <param name="source">The list, read in order and not copied.</param>
        /// <param name="cancellationToken">Unused: nothing is awaited at this open, and each advance of the
        /// returned cursor takes its own token.</param>
        /// <returns>An already completed open of a cursor over the list's elements.</returns>
        public static ValueTask<IClrCursor<TSource>> FromJavaListAsync<TSource>(java.util.List source, CancellationToken cancellationToken)
        {
            return new ValueTask<IClrCursor<TSource>>(FromJavaList<TSource>(source));
        }

        // ---- Correlate ----


        /// <summary>
        /// Joins each row of a cursor to the rows a function of it opens.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the inner cursor for one outer row synchronously, or returns null for no
        /// rows.</param>
        /// <param name="innerAsync">Opens the inner cursor for one outer row with await, or returns null for no
        /// rows.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which may be the default
        /// value where the join type supplies none.</param>
        /// <param name="joinType">INNER, LEFT, SEMI or ANTI.</param>
        /// <returns>A cursor over the joined rows, grouped by outer row in outer order.</returns>
        /// <exception cref="ArgumentException"><paramref name="joinType"/> is RIGHT or FULL.</exception>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.correlateJoin</c>, which runs the inner for each outer row inside
        /// <c>moveNext</c>. That acquisition happens inside whichever advance the consumer calls, so the inner
        /// is given as both opens and the cursor calls the one matching the advance, with that advance's token.
        /// A null inner is read as empty, as linq4j does.
        ///
        /// <para>The right value of a SEMI join's row is whatever <c>innerValue</c> last held, because linq4j's
        /// enumerator returns without assigning it; for a SEMI join that is always null. This reproduces
        /// that.</para>
        /// </remarks>
        public static IClrCursor<TResult> CorrelateJoin<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// <see cref="CorrelateJoin{TSource, TInner, TResult}"/>, over an outer open that awaits. RIGHT and FULL
        /// are refused before the outer is awaited, as <c>correlateJoin</c> refuses them before acquiring
        /// anything.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the outer input.</param>
        /// <param name="inner">Opens the inner cursor for one outer row synchronously, or returns null for no
        /// rows.</param>
        /// <param name="innerAsync">Opens the inner cursor for one outer row with await, or returns null for no
        /// rows.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which may be the default
        /// value where the join type supplies none.</param>
        /// <param name="joinType">INNER, LEFT, SEMI or ANTI; RIGHT and FULL throw <see
        /// cref="ArgumentException"/>.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the correlating cursor once the outer input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> CorrelateJoinAsync<TSource, TInner, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// The cursor of <see cref="CorrelateJoin{TSource, TInner, TResult}"/>, mirroring
        /// <c>correlateJoin</c>'s enumerator state for state.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, disposed with this cursor.</param>
        /// <param name="inner">Opens the inner cursor for an outer row, called from <c>Read</c>; null is read as
        /// empty.</param>
        /// <param name="innerAsync">Opens the inner cursor for an outer row with await, called from
        /// <c>ReadAsync</c>; null is read as empty.</param>
        /// <param name="resultSelector">Combines the outer row and the inner row into the current row.</param>
        /// <param name="joinType">INNER, LEFT, SEMI or ANTI, already checked by the operator.</param>
        /// <remarks>
        /// State 0 moves the outer and state 1 moves the inner, as in linq4j. The previous inner is disposed
        /// before the next is opened, in linq4j's order.
        /// </remarks>
        sealed class CorrelateJoinCursor<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType) : ClrCursor<TResult>
        {

            readonly bool semi = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            readonly bool anti = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            readonly bool nullsOnRight = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.LEFT);

            IClrCursor<TInner>? innerCursor;
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

                            // initial move inner; a null inner is read as empty
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
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened left input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the right rows for one left row synchronously.</param>
        /// <param name="innerAsync">Opens the right rows for one left row with await.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <returns>A cursor with one row per left row, in left input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.correlateLeftMarkJoin</c>, which is <c>leftMarkJoinInternal</c> over a
        /// correlated inner. Each right side is opened, read and disposed at its left row's turn, inside the
        /// advance, by the open matching that advance.
        ///
        /// <para>The marker starts false, becomes null when a comparison is unknown, and becomes true on the
        /// first match, which stops the scan. An unknown seen before a match is therefore discarded, and one
        /// seen with no match is kept, so <c>IN</c> over a nullable column yields UNKNOWN rather than
        /// FALSE.</para>
        /// </remarks>
        public static IClrCursor<TResult> CorrelateLeftMarkJoin<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the left input.</param>
        /// <param name="inner">Opens the right rows for one left row synchronously, or returns null for no
        /// rows.</param>
        /// <param name="innerAsync">Opens the right rows for one left row with await, or returns null for no
        /// rows.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the marking cursor once the left input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> CorrelateLeftMarkJoinAsync<TSource, TInner, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// The cursor of <see cref="CorrelateLeftMarkJoin{TSource, TInner, TResult}"/>, mirroring
        /// <c>leftMarkJoinInternal</c>'s enumerator.
        /// </summary>
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened left input, disposed with this cursor.</param>
        /// <param name="inner">Opens the right rows for a left row, called from <c>Read</c>; null is read as
        /// empty.</param>
        /// <param name="innerAsync">Opens the right rows for a left row with await, called from <c>ReadAsync</c>;
        /// null is read as empty.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        sealed class CorrelateLeftMarkJoinCursor<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector) : ClrCursor<TResult>
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

                // opened, read and disposed at this row's turn, as linq4j's try-with-resources does
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
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="joinType">INNER, LEFT, SEMI or ANTI.</param>
        /// <param name="outer">The opened left input, read a batch at a time and disposed with the returned
        /// cursor.</param>
        /// <param name="inner">Opens the right rows for a batch of left rows synchronously.</param>
        /// <param name="innerAsync">Opens the right rows for a batch of left rows with await.</param>
        /// <param name="resultSelector">Combines a left row and a right row; the right row is the default value
        /// for an unmatched LEFT or ANTI row.</param>
        /// <param name="predicate">Decides whether a right row belongs to a given left row of the batch.</param>
        /// <param name="batchSize">The number of left rows per batch, which is also the length of every list
        /// passed to the inner open.</param>
        /// <returns>A cursor over the joined rows, grouped by left row in left order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.correlateBatchJoin</c>. The right input filters on a disjunction of the
        /// batch's conditions, so one pass of it serves every row of the batch. Each batch's right side is
        /// opened at that batch's turn, inside the advance, by the open matching that advance.
        ///
        /// <para>As in Calcite, the batch's first left row pulls from the right cursor and caches each row as
        /// it goes, and every later left row reads the cache, so the right input is read no further than the
        /// first left row needs. When a SEMI or ANTI join's first left row finds its match, it reads the rest
        /// of the right cursor into the cache before moving on, because the rest of the batch reads the
        /// cache.</para>
        /// </remarks>
        public static IClrCursor<TResult> CorrelateBatchJoin<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            IClrCursor<TSource> outer,
            Func<java.util.List, IClrCursor<TInner>?> inner,
            Func<java.util.List, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="joinType">INNER, LEFT, SEMI or ANTI.</param>
        /// <param name="outer">The awaiting open of the left input.</param>
        /// <param name="inner">Opens the right rows for a batch of left rows synchronously.</param>
        /// <param name="innerAsync">Opens the right rows for a batch of left rows with await.</param>
        /// <param name="resultSelector">Combines a left row and a right row; the right row is the default value
        /// for an unmatched LEFT or ANTI row.</param>
        /// <param name="predicate">Decides whether a right row belongs to a given left row of the batch.</param>
        /// <param name="batchSize">The number of left rows per batch, which is also the length of every list
        /// passed to the inner open.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the batching cursor once the left input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> CorrelateBatchJoinAsync<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            ValueTask<IClrCursor<TSource>> outer,
            Func<java.util.List, IClrCursor<TInner>?> inner,
            Func<java.util.List, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// The cursor of <see cref="CorrelateBatchJoin{TSource, TInner, TResult}"/>, mirroring
        /// <c>correlateBatchJoin</c>'s enumerator field for field.
        /// </summary>
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="joinType">INNER, LEFT, SEMI or ANTI.</param>
        /// <param name="outer">The opened left input, disposed with this cursor.</param>
        /// <param name="inner">Opens the right rows for a padded batch, called from <c>Read</c>; null is read as
        /// empty.</param>
        /// <param name="innerAsync">Opens the right rows for a padded batch with await, called from
        /// <c>ReadAsync</c>; null is read as empty.</param>
        /// <param name="resultSelector">Combines a left row and a right row into the current row.</param>
        /// <param name="predicate">Decides whether a right row belongs to a given left row of the batch.</param>
        /// <param name="batchSize">The number of left rows per batch.</param>
        /// <remarks>
        /// <c>i</c> is the position in the batch and <c>j</c> the position in the cached right rows, as linq4j
        /// names them. The right cursor's first row is drawn as soon as the batch's right side is opened, so a
        /// batch with no right rows can be skipped whole for a SEMI or INNER join.
        ///
        /// <para>A short batch is padded by repeating its first row, as Calcite pads it; the condition is a
        /// disjunction, so a repeated row does not change it.</para>
        /// </remarks>
        sealed class CorrelateBatchJoinCursor<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            IClrCursor<TSource> outer,
            Func<java.util.List, IClrCursor<TInner>?> inner,
            Func<java.util.List, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            int batchSize) : ClrCursor<TResult>
        {

            readonly bool isSemi = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            readonly bool isAnti = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            readonly bool isLeft = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.LEFT);
            readonly bool isInner = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.INNER);

            readonly List<TSource> outerValues = new(batchSize);
            readonly List<TInner> innerValues = [];
            TSource? outerValue;
            TInner? innerValue;
            IClrCursor<TInner>? innerCursor;
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
            /// Returns the batch as the right side receives it: <c>batchSize</c> rows, a short batch filled out
            /// with its first row.
            /// </summary>
            /// <returns>A new Java list of exactly <c>batchSize</c> left rows, each converted to its Java
            /// value.</returns>
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
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TKey">The type of the equality key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened first cursor, drained and disposed before the second is opened.</param>
        /// <param name="inner">Opens the second cursor, which is acquired only once the first has been
        /// drained and disposed.</param>
        /// <param name="outerKeySelector">Extracts the equality key of an outer row, null where a key field is
        /// null.</param>
        /// <param name="innerKeySelector">Extracts the equality key of an inner row, null where a key field is
        /// null.</param>
        /// <param name="resultSelector">Combines an outer row with its best inner row, which is the default value
        /// where there is none.</param>
        /// <param name="matchComparator">Decides whether an inner row satisfies the match condition for an outer
        /// row.</param>
        /// <param name="timestampComparator">Orders two inner rows by timestamp; the greater of two candidates is
        /// kept.</param>
        /// <param name="emitNullsOnRight">Whether an outer row with no match is emitted against null.</param>
        /// <returns>A cursor over the joined rows, grouped by key in the iteration order of the index, then the
        /// outer rows whose key was null.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.asofJoin</c>: index the left rows by key, hold the best right row for
        /// each left row, scan the right updating it, then emit.
        ///
        /// <para>The index is a <c>java.util.HashMap</c> because rows are emitted in its iteration order, as
        /// linq4j's are.</para>
        ///
        /// <para>Both scans run at the open, as <c>asofJoin</c> builds its indexes in the method body before
        /// returning the enumerable that walks them. The outer is drained and disposed before the inner is
        /// acquired, which is why the inner is taken as an open.</para>
        /// </remarks>
        public static IClrCursor<TResult> AsofJoin<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
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
        /// <see cref="AsofJoin{TSource, TInner, TKey, TResult}"/>, over opens that await. Both scans await each
        /// row and complete before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TKey">The type of the equality key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the first cursor, drained and disposed before the second is
        /// opened.</param>
        /// <param name="inner">Opens the second cursor with await, once the first has been drained and
        /// disposed.</param>
        /// <param name="outerKeySelector">Extracts the equality key of an outer row, null where a key field is
        /// null.</param>
        /// <param name="innerKeySelector">Extracts the equality key of an inner row, null where a key field is
        /// null.</param>
        /// <param name="resultSelector">Combines an outer row with its best inner row, which is the default value
        /// where there is none.</param>
        /// <param name="matchComparator">Decides whether an inner row satisfies the match condition for an outer
        /// row.</param>
        /// <param name="timestampComparator">Orders two inner rows by timestamp; the greater of two candidates is
        /// kept.</param>
        /// <param name="emitNullsOnRight">Whether an outer row with no match is emitted against null.</param>
        /// <param name="cancellationToken">Passed to the second cursor's open and to each advance of both
        /// scans.</param>
        /// <returns>The open, completing with a cursor over the joined rows once both inputs have been
        /// scanned.</returns>
        public static async ValueTask<IClrCursor<TResult>> AsofJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> inner,
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
        /// Joins two cursors on two inequalities, one key of each side per inequality.
        /// </summary>
        /// <typeparam name="TLeft">The type of the left rows.</typeparam>
        /// <typeparam name="TRight">The type of the right rows.</typeparam>
        /// <typeparam name="TKey1">The type of the first predicate's keys.</typeparam>
        /// <typeparam name="TKey2">The type of the second predicate's keys.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="left">The opened left cursor, drained and disposed before the right is opened.</param>
        /// <param name="right">Opens the right cursor, which is acquired only once the left has been
        /// drained and disposed.</param>
        /// <param name="leftKeySelector1">Extracts a left row's key for the first predicate.</param>
        /// <param name="rightKeySelector1">Extracts a right row's key for the first predicate.</param>
        /// <param name="leftKeySelector2">Extracts a left row's key for the second predicate.</param>
        /// <param name="rightKeySelector2">Extracts a right row's key for the second predicate.</param>
        /// <param name="comparator1">Orders two keys of the first predicate.</param>
        /// <param name="comparator2">Orders two keys of the second predicate.</param>
        /// <param name="operator1">The first predicate's comparison, left key against right key.</param>
        /// <param name="operator2">The second predicate's comparison, left key against right key.</param>
        /// <param name="resultSelector">Combines a left row and a right row that satisfy both predicates.</param>
        /// <returns>A cursor over every pair satisfying both predicates; both inputs have been read and
        /// disposed.</returns>
        /// <exception cref="java.lang.IllegalArgumentException">An operator is not <c>&lt;</c>, <c>&lt;=</c>,
        /// <c>&gt;</c> or <c>&gt;=</c>; the left cursor is disposed first.</exception>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.ieJoin</c>. Each operator compares a left key with a right key through
        /// the comparator of the same number; a row with a null key matches nothing.
        ///
        /// <para>Both inputs are read at the open. In linq4j the <c>IEJoinEnumerator</c> constructor, run by
        /// <c>enumerator()</c>, drains and closes the left, then drains and closes the right, then sorts; the
        /// right is taken as an open so that it is acquired after the left is disposed.</para>
        ///
        /// <para>The operators are checked before anything is read, as linq4j checks them in the method
        /// body.</para>
        /// </remarks>
        public static IClrCursor<TResult> IeJoin<TLeft, TRight, TKey1, TKey2, TResult>(
            IClrCursor<TLeft> left,
            Func<IClrCursor<TRight>> right,
            Func<TLeft, TKey1> leftKeySelector1,
            Func<TRight, TKey1> rightKeySelector1,
            Func<TLeft, TKey2> leftKeySelector2,
            Func<TRight, TKey2> rightKeySelector2,
            java.util.Comparator comparator1,
            java.util.Comparator comparator2,
            System.Linq.Expressions.ExpressionType operator1,
            System.Linq.Expressions.ExpressionType operator2,
            Func<TLeft, TRight, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(left);
            ArgumentNullException.ThrowIfNull(right);

            try
            {
                IeJoinState<TLeft, TRight, TKey1, TKey2, TResult>.CheckOperators(operator1, operator2);
            }
            catch
            {
                left.Dispose();
                throw;
            }

            var state = new IeJoinState<TLeft, TRight, TKey1, TKey2, TResult>(comparator1, comparator2, operator1, operator2, resultSelector);

            try
            {
                while (left.Read())
                {
                    var row = left.Current;
                    state.AddLeft(row, leftKeySelector1(row), leftKeySelector2(row));
                }
            }
            finally
            {
                left.Dispose();
            }

            var second = right();
            try
            {
                while (second.Read())
                {
                    var row = second.Current;
                    state.AddRight(row, rightKeySelector1(row), rightKeySelector2(row));
                }
            }
            finally
            {
                second.Dispose();
            }

            state.Order();

            return new IeJoinCursor<TLeft, TRight, TKey1, TKey2, TResult>(state);
        }

        /// <summary>
        /// <see cref="IeJoin{TLeft, TRight, TKey1, TKey2, TResult}"/>, over opens that await. Both drains await
        /// each row and complete before the open does.
        /// </summary>
        /// <typeparam name="TLeft">The type of the left rows.</typeparam>
        /// <typeparam name="TRight">The type of the right rows.</typeparam>
        /// <typeparam name="TKey1">The type of the first predicate's keys.</typeparam>
        /// <typeparam name="TKey2">The type of the second predicate's keys.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="left">The awaiting open of the left cursor, drained and disposed before the right is
        /// opened.</param>
        /// <param name="right">Opens the right cursor with await, once the left has been drained and
        /// disposed.</param>
        /// <param name="leftKeySelector1">Extracts a left row's key for the first predicate.</param>
        /// <param name="rightKeySelector1">Extracts a right row's key for the first predicate.</param>
        /// <param name="leftKeySelector2">Extracts a left row's key for the second predicate.</param>
        /// <param name="rightKeySelector2">Extracts a right row's key for the second predicate.</param>
        /// <param name="comparator1">Orders two keys of the first predicate.</param>
        /// <param name="comparator2">Orders two keys of the second predicate.</param>
        /// <param name="operator1">The first predicate's comparison, left key against right key.</param>
        /// <param name="operator2">The second predicate's comparison, left key against right key.</param>
        /// <param name="resultSelector">Combines a left row and a right row that satisfy both predicates.</param>
        /// <param name="cancellationToken">Passed to the right cursor's open and to each advance of both
        /// drains.</param>
        /// <returns>The open, completing with a cursor over every pair satisfying both predicates once both inputs
        /// have been read.</returns>
        public static async ValueTask<IClrCursor<TResult>> IeJoinAsync<TLeft, TRight, TKey1, TKey2, TResult>(
            ValueTask<IClrCursor<TLeft>> left,
            Func<CancellationToken, ValueTask<IClrCursor<TRight>>> right,
            Func<TLeft, TKey1> leftKeySelector1,
            Func<TRight, TKey1> rightKeySelector1,
            Func<TLeft, TKey2> leftKeySelector2,
            Func<TRight, TKey2> rightKeySelector2,
            java.util.Comparator comparator1,
            java.util.Comparator comparator2,
            System.Linq.Expressions.ExpressionType operator1,
            System.Linq.Expressions.ExpressionType operator2,
            Func<TLeft, TRight, TResult> resultSelector,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(right);

            var first = await left.ConfigureAwait(false);

            try
            {
                IeJoinState<TLeft, TRight, TKey1, TKey2, TResult>.CheckOperators(operator1, operator2);
            }
            catch
            {
                await first.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            var state = new IeJoinState<TLeft, TRight, TKey1, TKey2, TResult>(comparator1, comparator2, operator1, operator2, resultSelector);

            try
            {
                while (await first.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = first.Current;
                    state.AddLeft(row, leftKeySelector1(row), leftKeySelector2(row));
                }
            }
            finally
            {
                await first.DisposeAsync().ConfigureAwait(false);
            }

            var second = await right(cancellationToken).ConfigureAwait(false);
            try
            {
                while (await second.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = second.Current;
                    state.AddRight(row, rightKeySelector1(row), rightKeySelector2(row));
                }
            }
            finally
            {
                await second.DisposeAsync().ConfigureAwait(false);
            }

            state.Order();

            return new IeJoinCursor<TLeft, TRight, TKey1, TKey2, TResult>(state);
        }

        /// <summary>
        /// The state of one IE join: the rows and the two sort orders linq4j's <c>IEJoinEnumerator</c> builds,
        /// without its advance. Shared by <see cref="IeJoin{TLeft, TRight, TKey1, TKey2, TResult}"/> and
        /// <see cref="IeJoinAsync{TLeft, TRight, TKey1, TKey2, TResult}"/>.
        /// </summary>
        /// <typeparam name="TLeft">The type of the left rows.</typeparam>
        /// <typeparam name="TRight">The type of the right rows.</typeparam>
        /// <typeparam name="TKey1">The type of the first predicate's keys.</typeparam>
        /// <typeparam name="TKey2">The type of the second predicate's keys.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <remarks>
        /// Both inputs are held and a row with either key null is dropped. The entries are sorted by each
        /// key. A right entry satisfies predicate 1 exactly when it follows a left entry in the first order,
        /// and predicate 2 exactly when it precedes that left entry in the second order. The permutation
        /// maps a position in the second order to a position in the first: scanning in the second order,
        /// each right entry sets its first-order position in the active set, and for each left entry the set
        /// bits after its own first-order position are the right entries satisfying both predicates.
        ///
        /// <para>The union-array algorithm of section 4.2 of Khayyat et al., "Lightning Fast and Space
        /// Efficient Inequality Joins", PVLDB 8(13), 2015.</para>
        ///
        /// <para>Both sorts must be stable, as Java's list sort is. The comparator returns 0 for equal keys
        /// from the same input, so the relative order of such entries comes from stability alone and reaches
        /// the output order. <see cref="List{T}.Sort()"/> is not stable, so both sorts use
        /// <c>OrderBy</c>.</para>
        /// </remarks>
        sealed class IeJoinState<TLeft, TRight, TKey1, TKey2, TResult>
        {

            /// <summary>
            /// Throws <c>IllegalArgumentException</c> for an operator other than <c>&lt;</c>, <c>&lt;=</c>,
            /// <c>&gt;</c> or <c>&gt;=</c>. Called before either input is read, as linq4j checks.
            /// </summary>
            /// <param name="operator1">The first predicate's comparison.</param>
            /// <param name="operator2">The second predicate's comparison.</param>
            internal static void CheckOperators(System.Linq.Expressions.ExpressionType operator1, System.Linq.Expressions.ExpressionType operator2)
            {
                foreach (var op in new[] { operator1, operator2 })
                {
                    switch (op)
                    {
                        case System.Linq.Expressions.ExpressionType.LessThan:
                        case System.Linq.Expressions.ExpressionType.LessThanOrEqual:
                        case System.Linq.Expressions.ExpressionType.GreaterThan:
                        case System.Linq.Expressions.ExpressionType.GreaterThanOrEqual:
                            break;
                        default:
                            throw new java.lang.IllegalArgumentException($"Unsupported IEJoin operator: {op}");
                    }
                }
            }

            readonly java.util.Comparator comparator1;
            readonly java.util.Comparator comparator2;
            readonly System.Linq.Expressions.ExpressionType operator1;
            readonly System.Linq.Expressions.ExpressionType operator2;

            readonly List<Entry> entries = [];

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="comparator1">Orders two keys of the first predicate.</param>
            /// <param name="comparator2">Orders two keys of the second predicate.</param>
            /// <param name="operator1">The first predicate's comparison, already checked by <see
            /// cref="CheckOperators"/>.</param>
            /// <param name="operator2">The second predicate's comparison, already checked by <see
            /// cref="CheckOperators"/>.</param>
            /// <param name="resultSelector">Builds a result row from a left row and a right row.</param>
            internal IeJoinState(
                java.util.Comparator comparator1,
                java.util.Comparator comparator2,
                System.Linq.Expressions.ExpressionType operator1,
                System.Linq.Expressions.ExpressionType operator2,
                Func<TLeft, TRight, TResult> resultSelector)
            {
                this.comparator1 = comparator1;
                this.comparator2 = comparator2;
                this.operator1 = operator1;
                this.operator2 = operator2;
                ResultSelector = resultSelector;
            }

            /// <summary>
            /// Builds a result row from a left row and a right row.
            /// </summary>
            internal Func<TLeft, TRight, TResult> ResultSelector { get; }

            /// <summary>
            /// The rows of the left input that were kept.
            /// </summary>
            internal List<TLeft> LeftRows { get; } = [];

            /// <summary>
            /// The rows of the right input that were kept.
            /// </summary>
            internal List<TRight> RightRows { get; } = [];

            /// <summary>
            /// The entries in the first order, once <see cref="Order"/> has run.
            /// </summary>
            internal List<Entry> FirstOrder { get; private set; } = [];

            /// <summary>
            /// Each position of the second order mapped to its position in the first, once
            /// <see cref="Order"/> has run.
            /// </summary>
            internal int[] Permutation { get; private set; } = [];

            /// <summary>
            /// Holds one row of the left input, unless either of its keys is null.
            /// </summary>
            /// <param name="row">The left row.</param>
            /// <param name="key1">The row's key for the first predicate.</param>
            /// <param name="key2">The row's key for the second predicate.</param>
            internal void AddLeft(TLeft row, TKey1 key1, TKey2 key2)
            {
                if (key1 is null || key2 is null)
                    return;

                entries.Add(new Entry(true, LeftRows.Count, key1, key2));
                LeftRows.Add(row);
            }

            /// <summary>
            /// Holds one row of the right input, unless either of its keys is null.
            /// </summary>
            /// <param name="row">The right row.</param>
            /// <param name="key1">The row's key for the first predicate.</param>
            /// <param name="key2">The row's key for the second predicate.</param>
            internal void AddRight(TRight row, TKey1 key1, TKey2 key2)
            {
                if (key1 is null || key2 is null)
                    return;

                entries.Add(new Entry(false, RightRows.Count, key1, key2));
                RightRows.Add(row);
            }

            /// <summary>
            /// Builds the two sort orders and the permutation between them, once both inputs are held.
            /// </summary>
            internal void Order()
            {
                FirstOrder = [.. entries.OrderBy(e => e, EntryComparer(comparator1, operator1, true))];
                for (int i = 0; i < FirstOrder.Count; i++)
                    FirstOrder[i].FirstPosition = i;

                Permutation = new int[entries.Count];

                var next = 0;
                foreach (var entry in entries.OrderBy(e => e, EntryComparer(comparator2, operator2, false)))
                    Permutation[next++] = entry.FirstPosition;
            }

            /// <summary>
            /// Orders entries by key 1 where <paramref name="isFirstOrder"/>, and by key 2 otherwise.
            /// </summary>
            /// <param name="comparator">Orders two keys of the predicate being sorted on.</param>
            /// <param name="op">That predicate's comparison, which decides the direction and the
            /// tie-break.</param>
            /// <param name="isFirstOrder">Whether this is the first order, on key 1, rather than the second, on
            /// key 2.</param>
            /// <returns>A comparer over entries for a stable sort.</returns>
            /// <remarks>
            /// For equal keys from different inputs, a strict operator puts the right entry first in the
            /// first order and last in the second, which excludes the pair; a non-strict one reverses both
            /// tie-breaks, which includes it.
            /// </remarks>
            static IComparer<Entry> EntryComparer(java.util.Comparator comparator, System.Linq.Expressions.ExpressionType op, bool isFirstOrder)
            {
                var greaterThan = op is System.Linq.Expressions.ExpressionType.GreaterThan or System.Linq.Expressions.ExpressionType.GreaterThanOrEqual;
                var descending = isFirstOrder ? greaterThan : greaterThan == false;
                var strict = op is System.Linq.Expressions.ExpressionType.LessThan or System.Linq.Expressions.ExpressionType.GreaterThan;
                var leftSideFirst = isFirstOrder != strict;

                return Comparer<Entry>.Create((entry1, entry2) =>
                {
                    var key1 = isFirstOrder ? (object?)entry1.Key1 : entry1.Key2;
                    var key2 = isFirstOrder ? (object?)entry2.Key1 : entry2.Key2;

                    var c = descending
                        ? comparator.compare(key2, key1)
                        : comparator.compare(key1, key2);

                    if (c != 0 || entry1.IsLeft == entry2.IsLeft)
                        return c;

                    return entry1.IsLeft == leftSideFirst ? -1 : 1;
                });
            }

            /// <summary>
            /// One row's place in the two sorted orders.
            /// </summary>
            /// <param name="isLeft">Whether the row came from the left input.</param>
            /// <param name="rowIndex">The index into the left rows or the right rows, by <paramref name="isLeft"/>.</param>
            /// <param name="key1">The row's key for the first predicate.</param>
            /// <param name="key2">The row's key for the second predicate.</param>
            internal sealed class Entry(bool isLeft, int rowIndex, TKey1 key1, TKey2 key2)
            {

                /// <summary>
                /// Whether the row came from the left input.
                /// </summary>
                internal bool IsLeft { get; } = isLeft;

                /// <summary>
                /// The index into the left rows or the right rows, by <see cref="IsLeft"/>.
                /// </summary>
                internal int RowIndex { get; } = rowIndex;

                /// <summary>
                /// The first predicate's key.
                /// </summary>
                internal TKey1 Key1 { get; } = key1;

                /// <summary>
                /// The second predicate's key.
                /// </summary>
                internal TKey2 Key2 { get; } = key2;

                /// <summary>
                /// The index in the first order, assigned after that sort.
                /// </summary>
                internal int FirstPosition { get; set; }

            }

        }

        /// <summary>
        /// The cursor of <see cref="IeJoin{TLeft, TRight, TKey1, TKey2, TResult}"/>, mirroring linq4j's
        /// <c>IEJoinEnumerator.moveNext</c> over the finished state.
        /// </summary>
        /// <typeparam name="TLeft">The type of the left rows.</typeparam>
        /// <typeparam name="TRight">The type of the right rows.</typeparam>
        /// <typeparam name="TKey1">The type of the first predicate's keys.</typeparam>
        /// <typeparam name="TKey2">The type of the second predicate's keys.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="state">The finished state, both inputs held and ordered.</param>
        /// <remarks>
        /// Walks the permutation, which is the second order. A right entry sets its first-order position in
        /// the active set; a left entry becomes the current left, and every set bit after its own first-order
        /// position is a right entry satisfying both predicates. The active set is a <c>java.util.BitSet</c>
        /// for its <c>nextSetBit</c>, which <see cref="System.Collections.BitArray"/> lacks. Both inputs were
        /// disposed at the open, so there is nothing to await or dispose.
        /// </remarks>
        sealed class IeJoinCursor<TLeft, TRight, TKey1, TKey2, TResult>(IeJoinState<TLeft, TRight, TKey1, TKey2, TResult> state) : ClrCursor<TResult>
        {

            readonly java.util.BitSet activeRights = new();

            IeJoinState<TLeft, TRight, TKey1, TKey2, TResult>.Entry? currentLeft;
            int secondPosition;
            int nextBit;
            TResult current = default!;

            /// <inheritdoc />
            public override TResult Current => current;

            /// <inheritdoc />
            public override bool Read()
            {
                while (true)
                {
                    // rights already seen in the second order satisfy predicate 2; active bits after the
                    // current left's position in the first order also satisfy predicate 1
                    if (currentLeft != null)
                    {
                        var bit = activeRights.nextSetBit(nextBit);
                        if (bit >= 0)
                        {
                            nextBit = bit + 1;
                            var right = state.FirstOrder[bit];
                            current = state.ResultSelector(state.LeftRows[currentLeft.RowIndex], state.RightRows[right.RowIndex]);
                            return true;
                        }

                        currentLeft = null;
                    }

                    if (secondPosition >= state.Permutation.Length)
                    {
                        current = default!;
                        return false;
                    }

                    var firstPosition = state.Permutation[secondPosition++];
                    var entry = state.FirstOrder[firstPosition];
                    if (entry.IsLeft)
                    {
                        currentLeft = entry;
                        nextBit = firstPosition + 1;
                    }
                    else
                    {
                        activeRights.set(firstPosition);
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

            }

        }

        /// <summary>
        /// The cursor of <see cref="AsofJoin{TSource, TInner, TKey, TResult}"/>, mirroring the enumerator
        /// <c>asofJoin</c> returns over its finished indexes.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="leftIndex">The outer rows grouped by key, each a list of rows.</param>
        /// <param name="rightIndex">For each key, the best inner row of each outer row in the same position, or
        /// null where none matched.</param>
        /// <param name="outerWithNullKeys">The outer rows whose key was null, emitted last against null.</param>
        /// <param name="resultSelector">Combines an outer row with its best inner row into the current
        /// row.</param>
        /// <param name="emitNullsOnRight">Whether an outer row with no match is emitted against null.</param>
        /// <remarks>
        /// Walks the left index's entries and, within each, the left rows beside their best right rows; then
        /// emits the outer rows whose key was null. Both inputs were disposed at the open, so there is nothing
        /// to await or dispose.
        /// </remarks>
        sealed class AsofJoinCursor<TSource, TInner, TResult>(
            java.util.HashMap leftIndex,
            java.util.HashMap rightIndex,
            List<TSource> outerWithNullKeys,
            Func<TSource?, TInner?, TResult> resultSelector,
            bool emitNullsOnRight) : ClrCursor<TResult>
        {

            readonly java.util.Iterator entries = leftIndex.entrySet().iterator();

            bool emittingNullKeys; // true while emitting the rows with null keys
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
        // Hash, semi, mark, merge and nested loop joins. Each acquires its inputs when its linq4j original
        // does. A hash join drains its build side inside enumerator(), so the open drains it and the cursor
        // probes it with the other input. An operator whose linq4j original acquires an input inside
        // moveNext takes that input as opens of both kinds and calls the one matching the advance. A merge
        // join positions both inputs inside enumerator(), so the open does. nestedLoopJoinAsList builds the
        // whole result where it is called, so the open does.


        /// <summary>
        /// Returns every left row with a marker saying whether the right side had a match, using a hash table.
        /// </summary>
        /// <typeparam name="TSource">The type of the left (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the right (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the EQUALS keys.</typeparam>
        /// <typeparam name="TNsKey">The type of the IS NOT DISTINCT FROM keys.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The opened left input, probed row by row and disposed with the returned
        /// cursor.</param>
        /// <param name="inner">The opened right input, drained into the lookup and disposed before this method
        /// returns.</param>
        /// <param name="outerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="innerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="outerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are none.</param>
        /// <param name="innerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are none.</param>
        /// <param name="atMostOneNotNullSafeKey">Whether at most one join key uses EQUALS.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <param name="comparer">Equality of the EQUALS keys, or null for the keys' own equality.</param>
        /// <param name="nullSafeComparer">Equality of the IS NOT DISTINCT FROM keys, or null for the keys' own
        /// equality.</param>
        /// <param name="nonEquiPredicate">Three-valued, or null where the condition is all equalities.</param>
        /// <param name="equiPredicate">Three-valued.</param>
        /// <returns>A cursor with one row per left row, in left input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.leftMarkHashJoin</c> and its two algorithms. A lookup that finds
        /// nothing cannot by itself tell FALSE from UNKNOWN. Where at most one key uses EQUALS, a probe that
        /// finds no bucket is UNKNOWN if that key was ever null on the build side and FALSE otherwise. Where
        /// several do, the equi-predicate is run against the rows whose key is null, since only it can say
        /// whether a comparison is unknown.
        ///
        /// <para>The lookup is a <c>java.util.HashMap</c> so that keys hash as Calcite values do. Its
        /// iteration order does not reach the output: a mark join emits in outer input order.</para>
        ///
        /// <para><c>leftMarkHashJoin</c> builds its hash table inside <c>enumerator()</c>, so the build side is
        /// drained and disposed at the open.</para>
        /// </remarks>
        public static IClrCursor<TResult> LeftMarkHashJoin<TSource, TInner, TKey, TNsKey, TResult>(
            IClrCursor<TSource> outer,
            IClrCursor<TInner> inner,
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
        /// <see cref="LeftMarkHashJoin"/>, over opens that await. The build side is drained with await and
        /// the drain completes before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the left (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the right (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the EQUALS keys.</typeparam>
        /// <typeparam name="TNsKey">The type of the IS NOT DISTINCT FROM keys.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The awaiting open of the left input.</param>
        /// <param name="inner">The awaiting open of the right input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="outerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="innerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="outerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are
        /// none.</param>
        /// <param name="innerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are
        /// none.</param>
        /// <param name="atMostOneNotNullSafeKey">Whether at most one join key uses EQUALS.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <param name="comparer">Equality of the EQUALS keys, or null for the keys' own equality.</param>
        /// <param name="nullSafeComparer">Equality of the IS NOT DISTINCT FROM keys, or null for the keys' own
        /// equality.</param>
        /// <param name="nonEquiPredicate">Three-valued, or null where the condition is all equalities.</param>
        /// <param name="equiPredicate">Three-valued.</param>
        /// <param name="cancellationToken">Passed to each advance of the build side's drain.</param>
        /// <returns>The open, completing with the probing cursor once the right input has been drained.</returns>
        public static async ValueTask<IClrCursor<TResult>> LeftMarkHashJoinAsync<TSource, TInner, TKey, TNsKey, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            ValueTask<IClrCursor<TInner>> inner,
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
        /// <typeparam name="TSource">The type of the left (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the right (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the EQUALS keys.</typeparam>
        /// <typeparam name="TNsKey">The type of the IS NOT DISTINCT FROM keys.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The opened left input, disposed with this cursor.</param>
        /// <param name="outerKeyNullAwareSelector">Yields null where a not null-safe key is null.</param>
        /// <param name="outerNullSafeKeySelector">The IS NOT DISTINCT FROM keys, or null where there are
        /// none.</param>
        /// <param name="atMostOneNotNullSafeKey">Whether at most one join key uses EQUALS.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <param name="comparer">Equality of the EQUALS keys, or null for the keys' own equality.</param>
        /// <param name="nullSafeComparer">Equality of the IS NOT DISTINCT FROM keys, or null for the keys' own
        /// equality.</param>
        /// <param name="nonEquiPredicate">Three-valued, or null where the condition is all equalities.</param>
        /// <param name="equiPredicate">Three-valued; run against rows whose key is null where several keys use
        /// EQUALS.</param>
        /// <param name="lookup">The right rows bucketed by wrapped key, the rows with a null key under
        /// null.</param>
        /// <param name="nullSafeKeys">The wrapped IS NOT DISTINCT FROM keys the right side carries.</param>
        sealed class LeftMarkHashJoinCursor<TSource, TInner, TKey, TNsKey, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, TKey> outerKeyNullAwareSelector,
            Func<TSource, TNsKey>? outerNullSafeKeySelector,
            bool atMostOneNotNullSafeKey,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            EqualityComparer? comparer,
            EqualityComparer? nullSafeComparer,
            Func<TSource, TInner, java.lang.Boolean?>? nonEquiPredicate,
            Func<TSource, TInner, java.lang.Boolean?> equiPredicate,
            java.util.HashMap lookup,
            java.util.HashSet nullSafeKeys) : ClrCursor<TResult>
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
            /// <param name="row">The outer row.</param>
            /// <returns>True if a right row matches, false if none does, and null if the answer is
            /// unknown.</returns>
            java.lang.Boolean? Mark(TSource row)
            {
                java.lang.Boolean? marker = java.lang.Boolean.FALSE;

                if (outerNullSafeKeySelector != null
                    && nullSafeKeys.contains(JavaWrapped.Of(nullSafeComparer, JavaValues.From(outerNullSafeKeySelector(row)))) == false)
                {
                    // rows whose null-safe keys differ are unequal whatever the rest of the condition says
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
                        // comparison is unknown rather than false
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

                // an empty build side yields FALSE, never UNKNOWN
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
        /// <typeparam name="TInner">The type of the rows in the buckets.</typeparam>
        /// <param name="lookup">The lookup, whose values are lists of rows.</param>
        /// <returns>The buckets, in the lookup's iteration order, read lazily.</returns>
        static IEnumerable<List<TInner>> Buckets<TInner>(java.util.HashMap lookup)
        {
            for (var i = lookup.values().iterator(); i.hasNext();)
                if (i.next() is List<TInner> bucket)
                    yield return bucket;
        }

        /// <summary>
        /// Returns the bucket of a lookup under a key, adding an empty one where there was none.
        /// </summary>
        /// <typeparam name="TInner">The type of the rows in the buckets.</typeparam>
        /// <param name="lookup">The lookup, whose values are lists of rows.</param>
        /// <param name="key">The key, wrapped for its comparer, or null for the rows whose key is null.</param>
        /// <returns>The bucket under <paramref name="key"/>, which the caller adds to.</returns>
        static List<TInner> Bucket<TInner>(java.util.HashMap lookup, object? key)
        {
            if (lookup.get(key) is not List<TInner> bucket)
                lookup.put(key, bucket = []);

            return bucket;
        }

        /// <summary>
        /// Joins two inputs on a key.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened probe input, disposed with the returned cursor.</param>
        /// <param name="inner">The opened build input, drained into the lookup and disposed before this method
        /// returns.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null right.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null when there is none.</param>
        /// <returns>A cursor over the joined rows, in probe input order, followed by any unmatched build
        /// rows.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.hashJoin</c>. A null key matches nothing; the null-aware key accessor
        /// of a physical type returns null for the whole key when any key field is null. A null-keyed build
        /// row is still kept, under a null key that nothing probes, so a RIGHT or FULL join returns it among
        /// the unmatched rows.
        ///
        /// <para>As in Calcite, this dispatches to <c>hashEquiJoin_</c> when the condition is equalities only
        /// and to <c>hashJoinWithPredicate_</c> otherwise. The two track unmatched build rows differently: by
        /// key in the first, by row in the second.</para>
        /// </remarks>
        public static IClrCursor<TResult> HashJoin<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            IClrCursor<TInner> inner,
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
        /// <see cref="HashJoin"/>, over opens that await. The build side is drained with await and the drain
        /// completes before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the probe input.</param>
        /// <param name="inner">The awaiting open of the build input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null
        /// left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null when there is
        /// none.</param>
        /// <param name="cancellationToken">Passed to each advance of the build side's drain.</param>
        /// <returns>The open, completing with the probing cursor once the build input has been drained.</returns>
        public static ValueTask<IClrCursor<TResult>> HashJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            ValueTask<IClrCursor<TInner>> inner,
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
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened probe input, disposed with the returned cursor.</param>
        /// <param name="inner">The opened build input, drained into the lookup and disposed before this method
        /// returns.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null
        /// left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <returns>A cursor over the joined rows, in probe input order, followed by the build rows under
        /// unmatched keys.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.hashEquiJoin_</c>. For a join that generates nulls on the left, what
        /// is left over at the end is each key no outer row carried, and every build row under it is returned
        /// together.
        ///
        /// <para><c>hashEquiJoin_</c>'s <c>enumerator()</c> drains the build side into the lookup before the
        /// first <c>moveNext</c>, so the drain happens at the open.</para>
        /// </remarks>
        static IClrCursor<TResult> HashEquiJoin<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            IClrCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight)
        {
            // a java.util.HashMap, as linq4j's toLookup builds: the unmatched right rows a RIGHT or FULL join
            // ends with come out in this map's order (JavaHashingTests covers its stability across processes)
            var lookup = new java.util.HashMap();

            try
            {
                while (inner.Read())
                {
                    var row = inner.Current;
                    var key = innerKeySelector(row);

                    // a null key is kept under null, as toLookup keeps it, and nothing probes it
                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                    Bucket<TInner>(lookup, wrapped).Add(row);
                }
            }
            finally
            {
                inner.Dispose();
            }

            // every build-side key, removed as outer rows carry it, as Calcite tracks it; a key nothing
            // probes (the null key included) stays, and its rows are returned against a null left
            var unmatched = generateNullsOnLeft ? new java.util.HashSet(lookup.keySet()) : null;

            return new HashEquiJoinCursor<TSource, TInner, TKey, TResult>(outer, outerKeySelector, resultSelector, comparer, generateNullsOnRight, lookup, unmatched);
        }

        /// <summary>
        /// <see cref="HashEquiJoin"/>, over opens that await.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the probe input.</param>
        /// <param name="inner">The awaiting open of the build input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null
        /// left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="cancellationToken">Passed to each advance of the build side's drain.</param>
        /// <returns>The open, completing with the probing cursor once the build input has been drained.</returns>
        static async ValueTask<IClrCursor<TResult>> HashEquiJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            ValueTask<IClrCursor<TInner>> inner,
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

            // a java.util.HashMap, as linq4j's toLookup builds: the unmatched right rows a RIGHT or FULL join
            // ends with come out in this map's order (JavaHashingTests covers its stability across processes)
            var lookup = new java.util.HashMap();

            try
            {
                while (await innerCursor.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = innerCursor.Current;
                    var key = innerKeySelector(row);

                    // a null key is kept under null, as toLookup keeps it, and nothing probes it
                    var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                    Bucket<TInner>(lookup, wrapped).Add(row);
                }
            }
            finally
            {
                await innerCursor.DisposeAsync().ConfigureAwait(false);
            }

            // every build-side key, removed as outer rows carry it, as Calcite tracks it; a key nothing
            // probes (the null key included) stays, and its rows are returned against a null left
            var unmatched = generateNullsOnLeft ? new java.util.HashSet(lookup.keySet()) : null;

            return new HashEquiJoinCursor<TSource, TInner, TKey, TResult>(outerCursor, outerKeySelector, resultSelector, comparer, generateNullsOnRight, lookup, unmatched);
        }

        /// <summary>
        /// The probe loop of <see cref="HashEquiJoin"/>, over the lookup the open built.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened probe input, disposed with this cursor.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="lookup">The build rows bucketed by wrapped key, the rows with a null key under
        /// null.</param>
        /// <param name="unmatched">The keys no probe row has carried yet, or null where the join does not generate
        /// nulls on the left.</param>
        /// <remarks>
        /// Three states: drawing an outer row, pairing it with its bucket, and, once the outer is exhausted
        /// and the join generates nulls on the left, walking the unmatched keys. Both advances step the same
        /// states and differ only in how they draw the outer row.
        /// </remarks>
        sealed class HashEquiJoinCursor<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, TKey> outerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnRight,
            java.util.HashMap lookup,
            java.util.HashSet? unmatched) : ClrCursor<TResult>
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
            /// <param name="row">The outer row just drawn.</param>
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
            /// <returns>True if a row was emitted; false once the outer row is finished, with the state set to
            /// draw the next.</returns>
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
            /// Moves on from the exhausted outer: to the unmatched keys where the join generates nulls on the
            /// left, and to the end otherwise.
            /// </summary>
            void Exhausted()
            {
                if (unmatched == null)
                {
                    state = Done;
                    return;
                }

                // walk the set and look each key back up, as linq4j does, rather than walking the map and
                // filtering by the set. A HashSet copied from a key set can iterate in a different order from
                // the map: HashSet(Collection) sizes its table as tableSizeFor(max((int) (n / 0.75f) + 1, 16)),
                // while a map grown by insertion holds the smallest power of two at or above 16 that leaves
                // n <= 0.75 * cap. The two differ where n = 0.75 * 2^k (12, 24, 48, ...).
                leftovers = unmatched.iterator();
                bucket = null;
                index = 0;
                state = Leftovers;
            }

            /// <summary>
            /// Emits the next build row under an unmatched key, against a null left.
            /// </summary>
            /// <returns>True if a row was emitted; false once every unmatched key has been walked.</returns>
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
        /// Joins two inputs on a key and a further predicate.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened probe input, disposed with the returned cursor.</param>
        /// <param name="inner">The opened build input, drained into the lookup and disposed before this method
        /// returns.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null
        /// left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="predicate">The part of the condition that is not an equality, run on every pair whose keys
        /// are equal.</param>
        /// <returns>A cursor over the joined rows, in probe input order, followed by the unmatched build rows in
        /// build input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.hashJoinWithPredicate_</c>. Unmatched build rows are tracked per row,
        /// not per key: a build row whose key matched but whose predicate never passed has matched nothing,
        /// even if another row under the same key did, and a RIGHT or FULL join returns it against a null
        /// left. The leftovers are returned in build input order.
        ///
        /// <para><c>hashJoinWithPredicate_</c>'s <c>enumerator()</c> reads the build side and builds the lookup
        /// and the leftover list before the first <c>moveNext</c>, so the build side is read at the open.</para>
        /// </remarks>
        static IClrCursor<TResult> HashJoinWithPredicate<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            IClrCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate)
        {
            // a join that generates nulls on the left needs the build rows again for its leftovers, and the
            // cursor can be read only once, so they are kept as they are read
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
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the probe input.</param>
        /// <param name="inner">The awaiting open of the build input, which is drained and disposed before the open
        /// completes.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnLeft">Whether an inner row with no match is returned against a null
        /// left.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="predicate">The part of the condition that is not an equality, run on every pair whose keys
        /// are equal.</param>
        /// <param name="cancellationToken">Passed to each advance of the build side's drain.</param>
        /// <returns>The open, completing with the probing cursor once the build input has been drained.</returns>
        static async ValueTask<IClrCursor<TResult>> HashJoinWithPredicateAsync<TSource, TInner, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            ValueTask<IClrCursor<TInner>> inner,
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

            // a join that generates nulls on the left needs the build rows again for its leftovers, and the
            // cursor can be read only once, so they are kept as they are read
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
        /// <typeparam name="TSource">The type of the outer (probe) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (build) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null key matches nothing.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened probe input, disposed with this cursor.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key, null where any key field is null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="predicate">The part of the condition that is not an equality, run on every pair whose keys
        /// are equal.</param>
        /// <param name="lookup">The build rows bucketed by wrapped key, the rows with a null key under
        /// null.</param>
        /// <param name="unmatched">The build rows no pair has accepted yet, in build input order, or null where
        /// the join does not generate nulls on the left.</param>
        /// <remarks>
        /// The states of <see cref="HashEquiJoinCursor{TSource, TInner, TKey, TResult}"/>, except that the
        /// bucket is filtered by the predicate before it is paired and the leftovers are a list of rows rather
        /// than a set of keys.
        /// </remarks>
        sealed class HashJoinWithPredicateCursor<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, TKey> outerKeySelector,
            Func<TSource?, TInner?, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate,
            java.util.HashMap lookup,
            List<TInner>? unmatched) : ClrCursor<TResult>
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
            /// <param name="row">The outer row just drawn.</param>
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
            /// <returns>True if a row was emitted; false once the outer row is finished, with the state set to
            /// draw the next.</returns>
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
            /// <returns>True if a row was emitted; false once every leftover has been returned.</returns>
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
        /// <typeparam name="TSource">The type of the rows of the first input, which are the rows
        /// returned.</typeparam>
        /// <typeparam name="TInner">The type of the rows of the second input.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null outer key matches nothing.</typeparam>
        /// <param name="outer">The opened first input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the second input synchronously.</param>
        /// <param name="innerAsync">Opens the second input with await.</param>
        /// <param name="outerKeySelector">Extracts a first-input row's key, null where any key field is
        /// null.</param>
        /// <param name="innerKeySelector">Extracts a second-input row's key.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="anti">Whether the rows without a match are the ones returned.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null when there is
        /// none.</param>
        /// <returns>A cursor over the first input's rows that pass, in input order; the second input is opened on
        /// its first advance that draws a row.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.semiJoin</c>, which dispatches: with no predicate to
        /// <c>semiEquiJoin_</c>, which holds the distinct inner keys, and with one to
        /// <c>semiJoinWithPredicate_</c>, which holds a lookup of inner rows because the predicate needs them.
        ///
        /// <para>The inner is acquired only when the first outer row is tested, as linq4j memoizes it, so an
        /// empty outer never opens the inner. That happens inside an advance, so the inner is given as opens
        /// of both kinds and the cursor calls the one matching the advance.</para>
        /// </remarks>
        public static IClrCursor<TSource> SemiJoin<TSource, TInner, TKey>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
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
        /// <see cref="SemiJoin"/>, over an outer open that awaits. Only the outer is acquired at this open.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows of the first input, which are the rows
        /// returned.</typeparam>
        /// <typeparam name="TInner">The type of the rows of the second input.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null outer key matches nothing.</typeparam>
        /// <param name="outer">The awaiting open of the first input.</param>
        /// <param name="inner">Opens the second input synchronously.</param>
        /// <param name="innerAsync">Opens the second input with await.</param>
        /// <param name="outerKeySelector">Extracts a first-input row's key, null where any key field is
        /// null.</param>
        /// <param name="innerKeySelector">Extracts a second-input row's key.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="anti">Whether the rows without a match are the ones returned.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null when there is
        /// none.</param>
        /// <param name="cancellationToken">Unused: the first input's open was started by the caller, and each
        /// advance of the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the filtering cursor once the first input is open.</returns>
        public static async ValueTask<IClrCursor<TSource>> SemiJoinAsync<TSource, TInner, TKey>(
            ValueTask<IClrCursor<TSource>> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
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
        /// <typeparam name="TSource">The type of the rows of the first input, which are the rows
        /// returned.</typeparam>
        /// <typeparam name="TInner">The type of the rows of the second input.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null outer key matches nothing.</typeparam>
        /// <param name="outer">The opened first input, disposed with this cursor.</param>
        /// <param name="inner">Opens the second input, called from <c>Read</c> on the first outer row.</param>
        /// <param name="innerAsync">Opens the second input with await, called from <c>ReadAsync</c> on the first
        /// outer row.</param>
        /// <param name="outerKeySelector">Extracts a first-input row's key, null where any key field is
        /// null.</param>
        /// <param name="innerKeySelector">Extracts a second-input row's key.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="anti">Whether the rows without a match are the ones returned.</param>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.semiEquiJoin_</c>, which holds
        /// <c>inner.select(innerKeySelector).distinct()</c> and tests <c>contains</c> for each outer row.
        ///
        /// <para>Two sets are kept. <c>distinct(comparer)</c> decides which keys are duplicates by the
        /// comparer, but the <c>contains</c> that follows is <c>EnumerableDefaults.contains</c> over the
        /// unwrapped keys, which uses <c>Objects.equals</c>. A comparer coarser than <c>equals</c> therefore
        /// collapses unequal keys and keeps only the first; the comparer-keyed set reproduces that, and the
        /// plain set answers the membership test.</para>
        ///
        /// <para>The key sets are built on the first outer row, where linq4j uses <c>Suppliers.memoize</c>; a
        /// null field serves the same purpose.</para>
        /// </remarks>
        sealed class SemiEquiJoinCursor<TSource, TInner, TKey>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti) : ClrCursor<TSource>
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
            /// <param name="innerRow">A row of the second input.</param>
            /// <param name="distinct">The keys seen so far, wrapped for the comparer.</param>
            void Add(TInner innerRow, java.util.HashSet distinct)
            {
                var innerKey = JavaValues.From(innerKeySelector(innerRow));

                if (distinct.add(JavaWrapped.Of(comparer, innerKey)))
                    keys!.add(innerKey);
            }

            /// <summary>
            /// Returns whether an outer row's key is one of the inner's.
            /// </summary>
            /// <param name="row">A row of the first input.</param>
            /// <returns>True if the row's key is not null and equals one of the inner keys.</returns>
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
        /// <typeparam name="TSource">The type of the rows of the first input, which are the rows
        /// returned.</typeparam>
        /// <typeparam name="TInner">The type of the rows of the second input.</typeparam>
        /// <typeparam name="TKey">The type of the join key; a null outer key matches nothing.</typeparam>
        /// <param name="outer">The opened first input, disposed with this cursor.</param>
        /// <param name="inner">Opens the second input, called from <c>Read</c> on the first outer row.</param>
        /// <param name="innerAsync">Opens the second input with await, called from <c>ReadAsync</c> on the first
        /// outer row.</param>
        /// <param name="outerKeySelector">Extracts a first-input row's key, null where any key field is
        /// null.</param>
        /// <param name="innerKeySelector">Extracts a second-input row's key.</param>
        /// <param name="comparer">Key equality, or null for the keys' own equality.</param>
        /// <param name="anti">Whether the rows without a match are the ones returned.</param>
        /// <param name="predicate">The part of the condition that is not an equality, run on every pair whose keys
        /// are equal.</param>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.semiJoinWithPredicate_</c>. It holds the inner rows, because the
        /// predicate is given a pair, in a lookup built as <c>toLookup</c> does, so the comparer governs the
        /// lookup. The lookup is built on the first outer row, as in <see cref="SemiEquiJoinCursor{TSource, TInner, TKey}"/>.
        /// </remarks>
        sealed class SemiJoinWithPredicateCursor<TSource, TInner, TKey>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool> predicate) : ClrCursor<TSource>
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
            /// <param name="innerRow">A row of the second input.</param>
            void Add(TInner innerRow)
            {
                var innerKey = JavaWrapped.Of(comparer, JavaValues.From(innerKeySelector(innerRow)));

                Bucket<TInner>(lookup!, innerKey).Add(innerRow);
            }

            /// <summary>
            /// Returns whether any inner row of an outer row's key satisfies the predicate with it.
            /// </summary>
            /// <param name="row">A row of the first input.</param>
            /// <returns>True if the row's key is not null and some inner row under it satisfies the
            /// predicate.</returns>
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
        /// <param name="joinType">The join type.</param>
        /// <returns>True for INNER, SEMI, ANTI and LEFT; false for RIGHT, FULL and anything else.</returns>
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
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, sorted on the key and disposed with the returned
        /// cursor.</param>
        /// <param name="inner">The opened inner input, sorted on the key and disposed with the returned
        /// cursor.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row; the inner row is the default value
        /// for an unmatched LEFT or ANTI row.</param>
        /// <param name="joinType">INNER, SEMI, ANTI or LEFT.</param>
        /// <param name="comparator">Orders two keys; null means they compare themselves.</param>
        /// <param name="comparer">Decides whether two keys of one input are the same; null means they do.</param>
        /// <returns>A cursor over the joined rows in key order, with both inputs already positioned on their first
        /// key run.</returns>
        /// <exception cref="java.lang.UnsupportedOperationException"><paramref name="joinType"/> is not one
        /// <see cref="IsMergeJoinSupported"/> accepts.</exception>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.mergeJoin</c> statement for statement, with the state of linq4j's
        /// <c>MergeJoinEnumerator</c> held in <see cref="MergeJoinCursor{TSource, TInner, TKey, TResult}"/>.
        /// Both inputs are walked once and only the rows of one key are held.
        ///
        /// <para>Two null keys must not compare equal. Calcite's generated comparator signals that case by
        /// throwing <c>BothValuesAreNullException</c>, which linq4j catches to advance the right side; this
        /// catches the same exception, by class name because the class is package private.</para>
        ///
        /// <para><c>MergeJoinEnumerator</c>'s constructor calls <c>start()</c>, which reads each input as far
        /// as its first key run; the open does the same.</para>
        /// </remarks>
        public static IClrCursor<TResult> MergeJoin<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            IClrCursor<TInner> inner,
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
        /// and completes before the open does.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the outer input, sorted on the key.</param>
        /// <param name="inner">The awaiting open of the inner input, sorted on the key.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row; the inner row is the default value
        /// for an unmatched LEFT or ANTI row.</param>
        /// <param name="joinType">INNER, SEMI, ANTI or LEFT.</param>
        /// <param name="comparator">Orders two keys; null means they compare themselves.</param>
        /// <param name="comparer">Decides whether two keys of one input are the same; null means they do.</param>
        /// <param name="cancellationToken">Passed to each advance of the initial positioning.</param>
        /// <returns>The open, completing with the merging cursor once both inputs are positioned.</returns>
        public static async ValueTask<IClrCursor<TResult>> MergeJoinAsync<TSource, TInner, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            ValueTask<IClrCursor<TInner>> inner,
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
        /// The cursor of <see cref="MergeJoin"/>, mirroring linq4j's <c>MergeJoinEnumerator</c>. Each method
        /// that steps an input is written twice over the one set of fields, once with <c>Read</c> and once
        /// with <c>ReadAsync</c>.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TKey">The type of the join key.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, disposed with this cursor.</param>
        /// <param name="inner">The opened inner input, disposed with this cursor.</param>
        /// <param name="outerKeySelector">Extracts an outer row's key.</param>
        /// <param name="innerKeySelector">Extracts an inner row's key.</param>
        /// <param name="predicate">The part of the condition that is not an equality, or null.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row; the inner row is the default value
        /// for an unmatched LEFT or ANTI row.</param>
        /// <param name="joinType">INNER, SEMI, ANTI or LEFT.</param>
        /// <param name="comparator">Orders two keys; null means they compare themselves.</param>
        /// <param name="comparer">Decides whether two keys of one input are the same; null means they do.</param>
        sealed class MergeJoinCursor<TSource, TInner, TKey, TResult>(
            IClrCursor<TSource> outer,
            IClrCursor<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, bool>? predicate,
            Func<TSource?, TInner?, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            java.util.Comparator? comparator,
            EqualityComparer? comparer) : ClrCursor<TResult>
        {

            readonly IEqualityComparer<TKey> equality = JavaEqualityComparer<TKey>.Of(comparer);

            readonly bool isLeft = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.LEFT);
            readonly bool isAnti = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            readonly bool isSemi = joinType.name() == nameof(org.apache.calcite.linq4j.JoinType.SEMI);

            readonly List<TSource> lefts = [];
            readonly List<TInner> rights = [];
            bool done;
            bool remainingLeft;
            IClrCursor<TResult>? results;
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
            /// <param name="cancellationToken">Passed to each advance of either input.</param>
            /// <returns>A task that completes once both inputs are positioned.</returns>
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

            // true when the left input advanced onto a row with a non-null key; a LEFT join accepts any row,
            // because every left row is a result. Inputs sort nulls last, so a null key ends the walk
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
                            // take the left as the bigger, so the right advances. Unreachable, because the null
                            // guard above returns first; kept because Calcite's advance has the same catch
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
                        // the rest of the condition is decided by a nested loop over the two runs
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
                            // take the left as the bigger, so the right advances. Unreachable, because the null
                            // guard above returns first; kept because Calcite's advance has the same catch
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
                        // the rest of the condition is decided by a nested loop over the two runs
                        results = Residual();
                    }

                    return true;
                }
            }

            /// <summary>
            /// Joins the two runs of one key under the predicate, with a nested loop over copies of them.
            /// </summary>
            /// <returns>A cursor over the pairs of the current runs that satisfy the predicate, per the join
            /// type.</returns>
            /// <remarks>
            /// Mirrors Calcite's <c>nestedLoopJoin(Linq4j.asEnumerable(lefts), Linq4j.asEnumerable(rights), …)</c>,
            /// whose enumerator is obtained on the spot. The opens passed stand for the right run, opened once per
            /// left row.
            /// </remarks>
            IClrCursor<TResult> Residual()
            {
                var rightRun = new List<TInner>(rights);

                return NestedLoopJoin(
                    new ListCursor<TSource>([.. lefts]),
                    () => new ListCursor<TInner>(rightRun),
                    token => new ValueTask<IClrCursor<TInner>>(new ListCursor<TInner>(rightRun)),
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
        /// <typeparam name="TSource">The type of the rows of the first list.</typeparam>
        /// <typeparam name="TInner">The type of the rows of the second list.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The first list, walked slowest.</param>
        /// <param name="inner">The second list, walked in full for each row of the first.</param>
        /// <param name="resultSelector">Combines a row of each list into the current row.</param>
        /// <remarks>
        /// The counterpart of <c>CartesianProductJoinEnumerator</c>, which extends linq4j's
        /// <c>CartesianProductEnumerator</c>: it advances the last list first and falls back to the one before
        /// it when that runs out. The lists may be the merge join's own buffers, which it clears per key run,
        /// so the pairings must be read to the end before the join advances; the merge join's read loop does
        /// that, and Calcite has the same coupling through <c>Linq4j.enumerator(lefts)</c>. There is nothing to
        /// await or dispose.
        /// </remarks>
        sealed class CartesianCursor<TSource, TInner, TResult>(IReadOnlyList<TSource> outer, IReadOnlyList<TInner> inner, Func<TSource, TInner, TResult> resultSelector) : ClrCursor<TResult>
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
        /// Thrown where a merge join compares two null keys, which it must not treat as equal.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.BothValuesAreNullException</c>, which is private. It is
        /// caught in the merge join's advance and never escapes.
        /// </remarks>
        sealed class BothValuesAreNullException : Exception
        {

        }

        /// <summary>
        /// Orders two keys with nulls last, throwing <see cref="BothValuesAreNullException"/> for two nulls.
        /// </summary>
        /// <typeparam name="TKey">The type of the keys, which must be comparable once converted to their Java
        /// values.</typeparam>
        /// <param name="a">The left key.</param>
        /// <param name="b">The right key.</param>
        /// <returns>Negative, zero or positive as <paramref name="a"/> sorts before, with or after <paramref
        /// name="b"/>, a null sorting last.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.compareNullsLastForMergeJoin</c>; used only where no comparator was
        /// given. Two nulls must not compare equal, and which side to advance is the caller's decision, so the
        /// case is thrown; Calcite's <c>advance</c> catches it and takes 1.
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
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The opened left input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the right side synchronously.</param>
        /// <param name="innerAsync">Opens the right side with await.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <returns>A cursor with one row per left row, in left input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.leftMarkNestedLoopJoin</c>, which is <c>leftMarkJoinInternal</c> with
        /// an inner that does not depend on the left row. The marker is resolved as in
        /// <see cref="CorrelateLeftMarkJoin{TSource, TInner, TResult}"/>.
        ///
        /// <para>The right side is opened afresh for every left row, inside the advance, so it is given as opens
        /// of both kinds.</para>
        /// </remarks>
        public static IClrCursor<TResult> LeftMarkNestedLoopJoin<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(innerAsync);

            return LeftMarkJoin<TSource, TInner, TResult>(outer, _ => inner(), (_, token) => innerAsync(token), predicate, resultSelector);
        }

        /// <summary>
        /// <see cref="LeftMarkNestedLoopJoin"/>, over an outer open that awaits. Only the outer is acquired at
        /// this open.
        /// </summary>
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The awaiting open of the left input.</param>
        /// <param name="inner">Opens the right side synchronously.</param>
        /// <param name="innerAsync">Opens the right side with await.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <param name="cancellationToken">Unused: the left input's open was started by the caller, and each
        /// advance of the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the marking cursor once the left input is open.</returns>
        public static async ValueTask<IClrCursor<TResult>> LeftMarkNestedLoopJoinAsync<TSource, TInner, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector,
            CancellationToken cancellationToken)
        {
            return LeftMarkNestedLoopJoin(await outer.ConfigureAwait(false), inner, innerAsync, predicate, resultSelector);
        }

        /// <summary>
        /// The walk shared by the nested loop mark joins.
        /// </summary>
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The opened left input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the right side of one left row synchronously, or returns null for none.</param>
        /// <param name="innerAsync">Opens the right side of one left row with await, or returns null for none.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        /// <returns>A cursor with one row per left row, in left input order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.leftMarkJoinInternal</c>, which opens and reads each right side at its
        /// left row's turn.
        /// </remarks>
        static IClrCursor<TResult> LeftMarkJoin<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
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
        /// <typeparam name="TSource">The type of the left rows.</typeparam>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="outer">The opened left input, disposed with this cursor.</param>
        /// <param name="inner">Opens the right side of a left row, called from <c>Read</c>; null is read as
        /// empty.</param>
        /// <param name="innerAsync">Opens the right side of a left row with await, called from <c>ReadAsync</c>;
        /// null is read as empty.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector">Combines a left row with its marker: true for a match, false for none,
        /// null for unknown.</param>
        sealed class LeftMarkJoinCursor<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<TSource, IClrCursor<TInner>?> inner,
            Func<TSource, CancellationToken, ValueTask<IClrCursor<TInner>?>> innerAsync,
            Func<TSource, TInner, java.lang.Boolean?> predicate,
            Func<TSource, java.lang.Boolean?, TResult> resultSelector) : ClrCursor<TResult>
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
            /// Folds one comparison into the marker, and returns whether it was a match, which ends the scan.
            /// </summary>
            /// <param name="left">The current left row.</param>
            /// <param name="right">A right row.</param>
            /// <returns>True if the pair matched, which ends the scan; false if it did not or its comparison was
            /// unknown.</returns>
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
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the inner synchronously.</param>
        /// <param name="innerAsync">Opens the inner with await.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join or an ANTI row.</param>
        /// <param name="predicate">The join condition, run on every pair.</param>
        /// <param name="joinType">The join type, which decides which walk runs and which unmatched rows are
        /// returned.</param>
        /// <returns>A cursor over the joined rows; for RIGHT and FULL the whole result has already been
        /// built.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.nestedLoopJoin</c>, which dispatches to one of two walks. A join that
        /// generates nulls on the left (RIGHT, FULL) goes to <see cref="NestedLoopJoinAsList"/>, which buffers
        /// the inner and builds the whole result at the open; every other join goes to
        /// <see cref="NestedLoopJoinOptimized"/>, which streams.
        ///
        /// <para>The inner is given as opens of both kinds because the streaming walk opens it once per outer
        /// row, inside whichever advance reaches that row; the buffering walk opens it once, at the open, with
        /// the open of its own kind.</para>
        /// </remarks>
        public static IClrCursor<TResult> NestedLoopJoin<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
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
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The awaiting open of the outer input.</param>
        /// <param name="inner">Opens the inner synchronously.</param>
        /// <param name="innerAsync">Opens the inner with await.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join or an ANTI row.</param>
        /// <param name="predicate">The join condition, run on every pair.</param>
        /// <param name="joinType">The join type, which decides which walk runs and which unmatched rows are
        /// returned.</param>
        /// <param name="cancellationToken">Passed to the inner's open and each advance of the buffering walk for
        /// RIGHT and FULL; otherwise unused.</param>
        /// <returns>The open, completing once the outer input is open and, for RIGHT and FULL, once the whole
        /// result has been built.</returns>
        public static async ValueTask<IClrCursor<TResult>> NestedLoopJoinAsync<TSource, TInner, TResult>(
            ValueTask<IClrCursor<TSource>> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
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
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, drained and disposed before this method returns.</param>
        /// <param name="inner">Opens the inner, once, before the outer is read.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join or an ANTI row.</param>
        /// <param name="predicate">The join condition, run on every pair.</param>
        /// <param name="joinType">The join type, which decides which walk runs and which unmatched rows are
        /// returned.</param>
        /// <returns>A cursor over the finished result: each outer row's pairings in outer order, then the
        /// unmatched inner rows.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.nestedLoopJoinAsList</c>, which runs the whole join when called and
        /// returns the filled list wrapped by <c>Linq4j.asEnumerable</c>; here the whole join runs at the open.
        /// The inner is read into a list once and walked for every outer row.
        ///
        /// <para>The right rows that never matched are held in Guava's <c>Sets.newIdentityHashSet()</c>, as
        /// Calcite holds them. That set deduplicates by reference, so two unmatched right rows that are the
        /// same object are emitted once rather than twice. This is a Calcite defect, reproduced.</para>
        /// </remarks>
        static IClrCursor<TResult> NestedLoopJoinAsList<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
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
        /// <see cref="NestedLoopJoinAsList"/>, awaiting each row it reads. The whole join runs before the open
        /// completes.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, drained and disposed before the open completes.</param>
        /// <param name="innerAsync">Opens the inner with await, once, before the outer is read.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join or an ANTI row.</param>
        /// <param name="predicate">The join condition, run on every pair.</param>
        /// <param name="joinType">The join type, which decides which walk runs and which unmatched rows are
        /// returned.</param>
        /// <param name="cancellationToken">Passed to the inner's open and to each advance of both drains.</param>
        /// <returns>The open, completing with a cursor over the finished result once the whole join has
        /// run.</returns>
        static async ValueTask<IClrCursor<TResult>> NestedLoopJoinAsListAsync<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
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
        /// Returns the set the unmatched right rows are held in, or null where the join does not generate nulls
        /// on the left.
        /// </summary>
        /// <typeparam name="TInner">The type of the right rows.</typeparam>
        /// <param name="generateNullsOnLeft">Whether the join returns unmatched right rows.</param>
        /// <param name="rightList">The buffered right rows, all of which start unmatched.</param>
        /// <returns>An identity set holding every right row, or null.</returns>
        static java.util.Set? RightUnmatched<TInner>(bool generateNullsOnLeft, List<TInner> rightList)
        {
            if (generateNullsOnLeft == false)
                return null;

            // Guava's identity set, as Calcite uses: the unmatched rows come out in its iteration order, which
            // follows System.identityHashCode
            var rightUnmatched = com.google.common.collect.Sets.newIdentityHashSet();

            foreach (var right in rightList)
                rightUnmatched.add(right);

            return rightUnmatched;
        }

        /// <summary>
        /// One outer row's turn of <see cref="NestedLoopJoinAsList"/>: adds its pairings to
        /// <paramref name="result"/>, or the row against a null right where it matched nothing and the join
        /// generates nulls on the right or is ANTI.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="left">The outer row.</param>
        /// <param name="rightList">The buffered inner rows, walked in order.</param>
        /// <param name="rightUnmatched">The inner rows not yet matched, from which a matched row is removed, or
        /// null.</param>
        /// <param name="name">The join type's name.</param>
        /// <param name="generateNullsOnRight">Whether an outer row with no match is returned against a null
        /// right.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which may be the default
        /// value.</param>
        /// <param name="predicate">The join condition.</param>
        /// <param name="result">The list the rows are added to.</param>
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
        /// Joins two inputs on a condition, streaming, without building the result as a list.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, disposed with the returned cursor.</param>
        /// <param name="inner">Opens the inner synchronously, once per outer row read by <c>Read</c>.</param>
        /// <param name="innerAsync">Opens the inner with await, once per outer row read by
        /// <c>ReadAsync</c>.</param>
        /// <param name="resultSelector">Combines an outer row and an inner row, either of which is the default
        /// value for an unmatched row of an outer join or an ANTI row.</param>
        /// <param name="predicate">The join condition, run on every pair.</param>
        /// <param name="joinType">The join type; RIGHT and FULL throw <see cref="ArgumentException"/>.</param>
        /// <returns>A streaming cursor over the joined rows, in outer order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.nestedLoopJoinOptimized</c> and its state machine: state 0 moves the
        /// outer, state 1 moves the inner. The inner is opened afresh for every outer row, inside the advance,
        /// and is not buffered.
        ///
        /// <para>A join type other than INNER, LEFT, SEMI or ANTI (for example ASOF or LEFT_MARK) returns no
        /// rows, as in Calcite, whose switch falls to a default that emits nothing. RIGHT and FULL throw
        /// <see cref="ArgumentException"/> at the open, as Calcite refuses them before returning its
        /// enumerable.</para>
        /// </remarks>
        static IClrCursor<TResult> NestedLoopJoinOptimized<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            var name = joinType.name();

            if (name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL))
                throw new ArgumentException($"JoinType {name} is unsupported");

            return new NestedLoopJoinOptimizedCursor<TSource, TInner, TResult>(outer, inner, innerAsync, resultSelector, predicate, name);
        }

        /// <summary>
        /// The state machine of <see cref="NestedLoopJoinOptimized"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of the outer (left) rows.</typeparam>
        /// <typeparam name="TInner">The type of the inner (right) rows.</typeparam>
        /// <typeparam name="TResult">The type of the joined rows.</typeparam>
        /// <param name="outer">The opened outer input, disposed with this cursor.</param>
        /// <param name="inner">Opens the inner for an outer row, called from <c>Read</c>.</param>
        /// <param name="innerAsync">Opens the inner for an outer row with await, called from
        /// <c>ReadAsync</c>.</param>
        /// <param name="resultSelector">Combines the outer row and an inner row, or the default value, into the
        /// current row.</param>
        /// <param name="predicate">The join condition, run on every pair.</param>
        /// <param name="name">The join type's name.</param>
        sealed class NestedLoopJoinOptimizedCursor<TSource, TInner, TResult>(
            IClrCursor<TSource> outer,
            Func<IClrCursor<TInner>> inner,
            Func<CancellationToken, ValueTask<IClrCursor<TInner>>> innerAsync,
            Func<TSource?, TInner?, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            string name) : ClrCursor<TResult>
        {

            IClrCursor<TInner>? innerCursor;
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
            /// Tests the inner row the advance moved onto and returns whether the pair is a result, setting the
            /// state the join type requires for the next advance.
            /// </summary>
            /// <param name="value">The inner row just read.</param>
            /// <returns>True if the pair produced the current row.</returns>
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
                        // Calcite's current() builds the row from the enumerator's fields, which for SEMI hold
                        // the matched inner row rather than null
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
            /// <returns>True if the outer row produced the current row against a null inner, which happens for
            /// LEFT and ANTI when nothing matched.</returns>
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
        /// Passes every row through and leaves the rows in a collection.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="collection">The collection the rows are left in; its contents are replaced when the input
        /// is exhausted.</param>
        /// <param name="input">The opened input, disposed with the returned cursor.</param>
        /// <returns>A cursor that returns the input's rows unchanged.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.lazyCollectionSpool</c>. Rows are buffered as they are read, and the
        /// advance that finds the input exhausted replaces the collection's contents with them, so the
        /// collection holds one round rather than every row seen. The next round of a recursive query therefore
        /// reads only the previous round's rows.
        /// </remarks>
        public static IClrCursor<TSource> LazyCollectionSpool<TSource>(java.util.Collection collection, IClrCursor<TSource> input)
        {
            ArgumentNullException.ThrowIfNull(collection);
            ArgumentNullException.ThrowIfNull(input);

            return new LazyCollectionSpoolCursor<TSource>(collection, input);
        }

        /// <summary>
        /// <see cref="LazyCollectionSpool{TSource}"/>, over an open that awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="collection">The collection the rows are left in; its contents are replaced when the input
        /// is exhausted.</param>
        /// <param name="input">The awaiting open of the input.</param>
        /// <param name="cancellationToken">Unused: the input's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the spooling cursor once the input is open.</returns>
        public static async ValueTask<IClrCursor<TSource>> LazyCollectionSpoolAsync<TSource>(java.util.Collection collection, ValueTask<IClrCursor<TSource>> input, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(collection);

            return new LazyCollectionSpoolCursor<TSource>(collection, await input.ConfigureAwait(false));
        }

        /// <summary>
        /// The cursor of <see cref="LazyCollectionSpool{TSource}"/>, mirroring linq4j's enumerator: each advance
        /// buffers the row it read, and an advance that finds the input exhausted flushes the buffer into the
        /// collection.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="collection">The collection the round's rows are flushed into, each converted to its Java
        /// value.</param>
        /// <param name="input">The opened input, disposed with this cursor.</param>
        sealed class LazyCollectionSpoolCursor<TSource>(java.util.Collection collection, IClrCursor<TSource> input) : ClrCursor<TSource>
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
                // the collection belongs to the table and may be read back by Calcite's Java code, so each row
                // is passed through JavaValues.From on its way in
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="seed">The seed, opened.</param>
        /// <param name="iteration">Opens the iterative part synchronously, once per round.</param>
        /// <param name="iterationAsync">Opens the iterative part with await, once per round.</param>
        /// <param name="iterationLimit">A negative value for no limit.</param>
        /// <param name="all">Whether a row already returned is returned again.</param>
        /// <param name="comparer">Row equality for the duplicate check when <paramref name="all"/> is false, or
        /// null for the rows' own equality.</param>
        /// <param name="cleanUp">Run once the cursor is disposed, or null.</param>
        /// <returns>A cursor over the seed's rows and then each round's; only the seed has been opened.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.repeatUnion</c>, used for WITH RECURSIVE. The iterative part is
        /// acquired afresh each round inside the advance, reading what the spool beneath it left, so it is given
        /// as opens of both kinds and the cursor calls the one matching the advance.
        ///
        /// <para>The enumerator is transcribed directly because its termination test is not the obvious one: it
        /// stops when <c>current</c> still holds the <c>DUMMY</c> sentinel after a round, not when the round
        /// produced nothing. See the comment at the seed/iteration boundary in the cursor.</para>
        ///
        /// <para>Calcite casts a private <c>DUMMY</c> object to the row type and compares by reference; no row
        /// can be that object, so a <see cref="bool"/> flag stands in for the comparison.</para>
        /// </remarks>
        public static IClrCursor<TSource> RepeatUnion<TSource>(
            IClrCursor<TSource> seed,
            Func<IClrCursor<TSource>> iteration,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> iterationAsync,
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
        /// <see cref="RepeatUnion{TSource}"/>, over a seed whose open awaits.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="seed">The awaiting open of the seed.</param>
        /// <param name="iteration">Opens the iterative part synchronously, once per round.</param>
        /// <param name="iterationAsync">Opens the iterative part with await, once per round.</param>
        /// <param name="iterationLimit">The largest number of rounds, or a negative value for no limit.</param>
        /// <param name="all">Whether a row already returned is returned again.</param>
        /// <param name="comparer">Row equality for the duplicate check when <paramref name="all"/> is false, or
        /// null for the rows' own equality.</param>
        /// <param name="cleanUp">Run once the cursor is disposed, or null.</param>
        /// <param name="cancellationToken">Unused: the seed's open was started by the caller, and each advance of
        /// the returned cursor takes its own token.</param>
        /// <returns>The open, completing with the recursive cursor once the seed is open.</returns>
        public static async ValueTask<IClrCursor<TSource>> RepeatUnionAsync<TSource>(
            ValueTask<IClrCursor<TSource>> seed,
            Func<IClrCursor<TSource>> iteration,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> iterationAsync,
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
        /// The cursor of <see cref="RepeatUnion{TSource}"/>, mirroring linq4j's repeat union enumerator, with
        /// each round opened by the open matching the advance.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="seed">The opened seed, disposed with this cursor.</param>
        /// <param name="iteration">Opens the iterative part synchronously, once per round.</param>
        /// <param name="iterationAsync">Opens the iterative part with await, once per round.</param>
        /// <param name="iterationLimit">The largest number of rounds, or a negative value for no limit.</param>
        /// <param name="all">Whether a row already returned is returned again.</param>
        /// <param name="comparer">Row equality for the duplicate check when <paramref name="all"/> is false, or
        /// null for the rows' own equality.</param>
        /// <param name="cleanUp">Run once the cursor is disposed, or null.</param>
        sealed class RepeatUnionCursor<TSource>(
            IClrCursor<TSource> seed,
            Func<IClrCursor<TSource>> iteration,
            Func<CancellationToken, ValueTask<IClrCursor<TSource>>> iterationAsync,
            int iterationLimit,
            bool all,
            EqualityComparer? comparer,
            Action? cleanUp) : ClrCursor<TSource>
        {

            TSource current = default!;

            // Calcite's `current == DUMMY`: false wherever Calcite assigns `current` a row, true wherever it
            // assigns the sentinel
            bool currentIsDummy = true;

            bool seedProcessed;
            int currentIteration;
            IClrCursor<TSource>? iterativeCursor;

            // Calcite's set of wrapped rows, consulted only when `all` is false
            readonly HashSet<TSource>? processed = all ? null : new HashSet<TSource>(JavaEqualityComparer<TSource>.Of(comparer));

            bool disposed;

            /// <inheritdoc />
            public override TSource Current => currentIsDummy ? throw new InvalidOperationException("The cursor is not positioned on a row.") : current;

            /// <summary>
            /// Calcite's <c>checkValue</c>: whether a row is one to return.
            /// </summary>
            /// <param name="value">A row from the seed or a round.</param>
            /// <returns>True if duplicates are kept or the row has not been returned before; the row is then
            /// recorded.</returns>
            bool CheckValue(TSource value)
            {
                return processed == null || processed.Add(value);
            }

            /// <inheritdoc />
            public override bool Read()
            {
                // advance the seed until it is exhausted
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

                // Calcite does not reset the sentinel between the seed and the first round, so a seed that
                // emitted a row leaves `current` holding it: the first iterative round that produces nothing
                // does not stop the sequence, and only a second empty round does. This reproduces that. It is
                // visible only when the step aggregates (COUNT(*) yields a row over no rows); a step that reads
                // the working table row by row gives an empty round either way.
                for (; ; )
                {
                    if (iterationLimit >= 0 && currentIteration == iterationLimit)
                    {
                        // max number of iterations reached: done
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
                        // current iteration returned no value: done
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
            /// Mirrors Calcite's <c>close()</c>: the clean-up first, then the two enumerators. A second call does
            /// nothing.
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">Opens the first source, which is acquired only once the second has been
        /// drained and disposed.</param>
        /// <param name="other">The second source, drained and disposed first.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <param name="all">Whether a row present more than once in each is returned more than once.</param>
        /// <returns>A cursor over the rows of the first source also in the second, in the result collection's
        /// iteration order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.intersect</c>, which runs <c>source1.into(set1)</c> in the method body
        /// and then reads <c>source0.enumerator()</c> against the set: both inputs are drained at the open, the
        /// second first. That is why the first source is taken as an open and the second as a cursor.
        ///
        /// <para>The collections are Calcite's, a <c>java.util.HashSet</c> or Guava's <c>HashMultiset</c>,
        /// because rows are returned in the result collection's iteration order.</para>
        /// </remarks>
        public static IClrCursor<TSource> Intersect<TSource>(Func<IClrCursor<TSource>> source, IClrCursor<TSource> other, EqualityComparer? comparer, bool all)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(other);

            // for ALL the collection counts occurrences, so a row is kept once per pairing
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">Opens the first source with await, once the second has been drained and
        /// disposed.</param>
        /// <param name="other">The awaiting open of the second source, drained and disposed first.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <param name="all">Whether a row present more than once in each is returned more than once.</param>
        /// <param name="cancellationToken">Passed to the first source's open and to each advance of both
        /// drains.</param>
        /// <returns>The open, completing with a cursor over the common rows once both sources have been
        /// drained.</returns>
        public static async ValueTask<IClrCursor<TSource>> IntersectAsync<TSource>(Func<CancellationToken, ValueTask<IClrCursor<TSource>>> source, ValueTask<IClrCursor<TSource>> other, EqualityComparer? comparer, bool all, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(source);

            // for ALL the collection counts occurrences, so a row is kept once per pairing
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
        /// <param name="all">Whether duplicates are kept.</param>
        /// <returns>A new Guava <c>HashMultiset</c> where <paramref name="all"/>, and a new
        /// <c>java.util.HashSet</c> otherwise.</returns>
        static java.util.Collection Collection(bool all)
        {
            return all ? com.google.common.collect.HashMultiset.create() : new java.util.HashSet();
        }

        /// <summary>
        /// Returns the rows of the first source that are not in the second.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The opened first source, drained and disposed before the second is opened.</param>
        /// <param name="other">Opens the second source, which is acquired only once the first has been
        /// drained and disposed.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <param name="all">Whether a row is removed once per appearance in the second rather than entirely.</param>
        /// <returns>A cursor over the rows of the first source left after removing the second's, in the
        /// collection's iteration order.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.except</c>, which runs <c>source0.into(collection)</c> in the method
        /// body and then removes each row of <c>source1</c>: both inputs are drained at the open. The collection
        /// is chosen as in <see cref="Intersect{TSource}"/>, and rows are returned in its iteration order.
        /// </remarks>
        public static IClrCursor<TSource> Except<TSource>(IClrCursor<TSource> source, Func<IClrCursor<TSource>> other, EqualityComparer? comparer, bool all)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="source">The awaiting open of the first source, drained and disposed before the second is
        /// opened.</param>
        /// <param name="other">Opens the second source with await, once the first has been drained and
        /// disposed.</param>
        /// <param name="comparer">Row equality, or null for the rows' own equality.</param>
        /// <param name="all">Whether a row is removed once per appearance in the second rather than
        /// entirely.</param>
        /// <param name="cancellationToken">Passed to the second source's open and to each advance of both
        /// drains.</param>
        /// <returns>The open, completing with a cursor over the remaining rows once both sources have been
        /// drained.</returns>
        public static async ValueTask<IClrCursor<TSource>> ExceptAsync<TSource>(ValueTask<IClrCursor<TSource>> source, Func<CancellationToken, ValueTask<IClrCursor<TSource>>> other, EqualityComparer? comparer, bool all, CancellationToken cancellationToken)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="sources">Opens each source, as a <c>Func&lt;IClrCursor&lt;TSource&gt;&gt;</c>.</param>
        /// <param name="sortKeySelector">Extracts the sort key the sources are ordered on.</param>
        /// <param name="sortComparator">Orders two sort keys, as the sources are ordered.</param>
        /// <param name="all">Whether a row that repeats is kept.</param>
        /// <param name="comparer">Decides whether two rows are the same, where duplicates are dropped.</param>
        /// <returns>A cursor over the merged rows in key order, every source already opened and positioned on its
        /// first row.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.mergeUnion</c> and its <c>MergeUnionEnumerator</c>: take the smallest
        /// row across the inputs, emit it, and advance that input alone.
        ///
        /// <para><c>MergeUnionEnumerator</c>'s constructor acquires every input's enumerator in order and then
        /// positions each on its first row, all inside <c>enumerator()</c>. Both happen at the open: the sources
        /// are taken as opens and run in turn, and then each cursor is advanced once.</para>
        ///
        /// <para>Dropping duplicates needs only the rows sharing the current key, because the inputs are sorted
        /// and a repeated row arrives before the key changes. As in Calcite, the set is cleared when the key
        /// changes.</para>
        /// </remarks>
        public static IClrCursor<TSource> MergeUnion<TSource, TKey>(
            java.util.List sources,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer)
        {
            ArgumentNullException.ThrowIfNull(sources);
            ArgumentNullException.ThrowIfNull(sortKeySelector);
            ArgumentNullException.ThrowIfNull(sortComparator);

            var inputs = new IClrCursor<TSource>[sources.size()];
            for (int i = 0; i < inputs.Length; i++)
                inputs[i] = ((Func<IClrCursor<TSource>>)sources.get(i))();

            var cursor = new MergeUnionCursor<TSource, TKey>(inputs, sortKeySelector, sortComparator, all, comparer);
            cursor.Init();

            return cursor;
        }

        /// <summary>
        /// <see cref="MergeUnion{TSource, TKey}"/>, over opens that await, each a
        /// <c>Func&lt;CancellationToken, ValueTask&lt;IClrCursor&lt;TSource&gt;&gt;&gt;</c>.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="sources">Opens each source with await, in turn.</param>
        /// <param name="sortKeySelector">Extracts the sort key the sources are ordered on.</param>
        /// <param name="sortComparator">Orders two sort keys, as the sources are ordered.</param>
        /// <param name="all">Whether a row that repeats is kept.</param>
        /// <param name="comparer">Decides whether two rows are the same, where duplicates are dropped.</param>
        /// <param name="cancellationToken">Passed to each source's open and to its first advance.</param>
        /// <returns>The open, completing with the merging cursor once every source is open and
        /// positioned.</returns>
        public static async ValueTask<IClrCursor<TSource>> MergeUnionAsync<TSource, TKey>(
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

            var inputs = new IClrCursor<TSource>[sources.size()];
            for (int i = 0; i < inputs.Length; i++)
                inputs[i] = await ((Func<CancellationToken, ValueTask<IClrCursor<TSource>>>)sources.get(i))(cancellationToken).ConfigureAwait(false);

            var cursor = new MergeUnionCursor<TSource, TKey>(inputs, sortKeySelector, sortComparator, all, comparer);
            await cursor.InitAsync(cancellationToken).ConfigureAwait(false);

            return cursor;
        }

        /// <summary>
        /// The cursor of <see cref="MergeUnion{TSource, TKey}"/>, mirroring linq4j's <c>MergeUnionEnumerator</c>.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="inputs">The opened sources, disposed with this cursor.</param>
        /// <param name="sortKeySelector">Extracts the sort key the sources are ordered on.</param>
        /// <param name="sortComparator">Orders two sort keys, as the sources are ordered.</param>
        /// <param name="all">Whether a row that repeats is kept.</param>
        /// <param name="comparer">Decides whether two rows are the same, where duplicates are dropped.</param>
        sealed class MergeUnionCursor<TSource, TKey>(
            IClrCursor<TSource>[] inputs,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer) : ClrCursor<TSource>
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
            /// <param name="cancellationToken">Passed to each input's first advance.</param>
            /// <returns>A task that completes once every input is positioned.</returns>
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
            /// <returns>The index of the unfinished input whose current row sorts first, the lowest index winning
            /// a tie.</returns>
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="source">Opens the source, which is not acquired at all for a fetch of no rows.</param>
        /// <param name="keySelector">Extracts the sort key from a row.</param>
        /// <param name="comparator">Comparison of two keys, or null to use the keys' natural order.</param>
        /// <param name="offset">The number of sorted rows to skip.</param>
        /// <param name="fetch">The largest number of rows to return; zero or less returns none without opening the
        /// source.</param>
        /// <returns>A cursor over the kept rows in key order; the source has been drained and disposed.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.orderBy</c> with a fetch and an offset, which keeps at most
        /// <c>offset + fetch</c> rows: a row whose key sorts at or after the last key held, once the map is
        /// full, is dropped without being stored, and otherwise adding a row evicts the last one held.
        ///
        /// <para>linq4j does all of this inside <c>enumerator()</c>, so it happens at the open. For a fetch of
        /// no rows linq4j returns an empty enumerator without calling <c>source.enumerator()</c>, which is why
        /// the source is taken as an open. Otherwise the source is opened, drained into the bounded map,
        /// disposed, and the map trimmed by the offset before the cursor is returned.</para>
        ///
        /// <para>The map is a <c>java.util.TreeMap</c>, as in linq4j. It takes the Java comparator directly,
        /// passes a null key to it (which is how <c>Functions.nullsComparator</c> expresses NULLS FIRST and
        /// NULLS LAST; a <c>SortedDictionary</c> rejects a null key before consulting its comparer), and
        /// provides the <c>lastKey</c> and <c>headMap</c> operations the algorithm uses.</para>
        ///
        /// <para>linq4j stores a one-row group as a <c>Collections.singletonList</c> and replaces it with an
        /// <c>ArrayList</c> when a second row arrives; here a group is always a <see cref="List{T}"/>, which
        /// affects allocation only.</para>
        /// </remarks>
        public static IClrCursor<TSource> OrderByWithFetchAndOffset<TSource, TKey>(Func<IClrCursor<TSource>> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, java.math.BigDecimal offset, java.math.BigDecimal fetch)
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
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <typeparam name="TKey">The type of the sort key.</typeparam>
        /// <param name="source">Opens the source with await; it is not opened at all for a fetch of no
        /// rows.</param>
        /// <param name="keySelector">Extracts the sort key from a row.</param>
        /// <param name="comparator">Comparison of two keys, or null to use the keys' natural order.</param>
        /// <param name="offset">The number of sorted rows to skip.</param>
        /// <param name="fetch">The largest number of rows to return; zero or less returns none without opening the
        /// source.</param>
        /// <param name="cancellationToken">Passed to the source's open and to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor over the kept rows once the source has been
        /// drained.</returns>
        public static async ValueTask<IClrCursor<TSource>> OrderByWithFetchAndOffsetAsync<TSource, TKey>(Func<CancellationToken, ValueTask<IClrCursor<TSource>>> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, java.math.BigDecimal offset, java.math.BigDecimal fetch, CancellationToken cancellationToken)
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
        /// with the per-row bound and the offset trim shared by both opens.
        /// </summary>
        /// <typeparam name="TSource">The type of the rows.</typeparam>
        /// <param name="comparator">Orders the keys, or null for the keys' natural order.</param>
        /// <param name="offset">The number of rows the trim skips.</param>
        /// <param name="fetch">The number of rows wanted after the offset.</param>
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
            /// <param name="key">The row's sort key, which may be null where the comparator orders nulls.</param>
            /// <param name="row">The row.</param>
            public void Add(object? key, TSource row)
            {
                if (needed.signum() >= 0 && size.compareTo(needed) >= 0)
                {
                    // the current row will never appear in the output, so just skip it
                    var lastKey = map.lastKey();
                    if (Compare(comparator, key, lastKey) >= 0)
                        return;

                    // remove the last entry from the tree map, to keep at most 'needed' rows
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
            /// <returns>The rows left after the offset, in key order and, within a key, in arrival
            /// order.</returns>
            public List<TSource> Trimmed()
            {
                if (actualOffset.compareTo(java.math.BigDecimal.ZERO) > 0)
                {
                    // find the key up to which entries are removed from the map
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
                            // entries may need to be removed from the list
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
        /// <param name="count">The evaluated FETCH or OFFSET expression.</param>
        /// <returns>The count rounded up to a whole number, and zero for a negative count.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableDefaults.rowsRequired</c>. A count is a <c>BigDecimal</c> because its expression
        /// need not be an integer; a fractional count is rounded up, and a negative one counts as zero.
        /// </remarks>
        static java.math.BigDecimal RowsRequired(java.math.BigDecimal count)
        {
            return count.max(java.math.BigDecimal.ZERO).setScale(0, java.math.RoundingMode.CEILING);
        }

        /// <summary>
        /// Compares two keys the way the map holding them does.
        /// </summary>
        /// <param name="comparator">The map's comparator, or null where it orders keys naturally.</param>
        /// <param name="x">The first key.</param>
        /// <param name="y">The second key.</param>
        /// <returns>Negative, zero or positive as <paramref name="x"/> sorts before, with or after <paramref
        /// name="y"/>.</returns>
        /// <remarks>
        /// Uses the comparator where there is one. Otherwise it orders by the keys themselves, as a
        /// <c>TreeMap</c> built without a comparator does, through <see cref="IComparable"/>:
        /// <c>java.lang.Comparable</c> is an IKVM ghost interface that a cast from C# cannot reach.
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the partition key.</typeparam>
        /// <typeparam name="TAccumulator">The type holding every aggregate's state and last result.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="partitionSelector">Key of the PARTITION BY clause, or null where there is none.</param>
        /// <param name="comparator">Orders the rows of one partition, and compares two of them for EXCLUDE and for RANK.</param>
        /// <param name="exclude">Which rows of the frame the aggregates do not see.</param>
        /// <param name="lowerBound">First index of the frame, before it is clamped to the partition.</param>
        /// <param name="upperBound">Last index of the frame, before it is clamped to the partition.</param>
        /// <param name="alwaysNonEmpty">Whether the bounds can be taken as they are, because the frame always holds the current row.</param>
        /// <param name="clampStart">Whether the lower bound has to be brought back to the first row of the partition.</param>
        /// <param name="clampEnd">Whether the upper bound has to be brought back to the last row of the partition.</param>
        /// <param name="lowerBoundCanChange">Whether the frame's start moves at all, which UNBOUNDED PRECEDING settles.</param>
        /// <param name="accumulatorInitializer">Creates the accumulator, once for the whole window.</param>
        /// <param name="reset">Returns the accumulator to its starting value, or null where no aggregate has one.</param>
        /// <param name="adder">Folds one row into the accumulator, or null where no aggregate reads the rows.</param>
        /// <param name="cachedResult">Computes the results that only change when the frame does, or null where every aggregate is recomputed per row.</param>
        /// <param name="uncachedResult">Computes the results that change on every row, or null where there are none.</param>
        /// <param name="selector">Builds the output row from the input row and the results.</param>
        /// <returns>A cursor over one output row per input row, partition by partition, each partition in the
        /// comparator's order.</returns>
        /// <remarks>
        /// The counterpart of the block <c>EnumerableWindow</c> generates. What each aggregate computes (the
        /// implementors' reset, add and result, and the two frame bounds) is Calcite's, translated by the node;
        /// this method is the loop that calls those pieces, which Calcite writes as generated Java source.
        ///
        /// <para>The accumulator carries each aggregate's state and last result, so a result that does not
        /// change while the frame is unchanged is computed once and reused. It is created once for the whole
        /// window, as Calcite declares its variables once.</para>
        ///
        /// <para>The generated block drains its source into the partition collection, appends each output row
        /// to an <c>ArrayList</c>, and evaluates to <c>Linq4j.asEnumerable(list)</c>. Likewise, the whole window
        /// is computed at the open and the returned cursor reads the finished list.</para>
        /// </remarks>
        public static IClrCursor<TResult> Window<TSource, TKey, TAccumulator, TResult>(
            IClrCursor<TSource> source,
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
        /// <see cref="Window{TSource, TKey, TAccumulator, TResult}"/>, over an open that awaits. The drain awaits
        /// each row, and the window is computed before the open completes.
        /// </summary>
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the partition key.</typeparam>
        /// <typeparam name="TAccumulator">The type holding every aggregate's state and last result.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="source">The awaiting open of the input, drained and disposed before the open
        /// completes.</param>
        /// <param name="partitionSelector">Key of the PARTITION BY clause, or null where there is none.</param>
        /// <param name="comparator">Orders the rows of one partition, and compares two of them for EXCLUDE and for
        /// RANK.</param>
        /// <param name="exclude">Which rows of the frame the aggregates do not see.</param>
        /// <param name="lowerBound">First index of the frame, before it is clamped to the partition.</param>
        /// <param name="upperBound">Last index of the frame, before it is clamped to the partition.</param>
        /// <param name="alwaysNonEmpty">Whether the bounds can be taken as they are, because the frame always
        /// holds the current row.</param>
        /// <param name="clampStart">Whether the lower bound has to be brought back to the first row of the
        /// partition.</param>
        /// <param name="clampEnd">Whether the upper bound has to be brought back to the last row of the
        /// partition.</param>
        /// <param name="lowerBoundCanChange">Whether the frame's start moves at all, which UNBOUNDED PRECEDING
        /// settles.</param>
        /// <param name="accumulatorInitializer">Creates the accumulator, once for the whole window.</param>
        /// <param name="reset">Returns the accumulator to its starting value, or null where no aggregate has
        /// one.</param>
        /// <param name="adder">Folds one row into the accumulator, or null where no aggregate reads the
        /// rows.</param>
        /// <param name="cachedResult">Computes the results that only change when the frame does, or null where
        /// every aggregate is recomputed per row.</param>
        /// <param name="uncachedResult">Computes the results that change on every row, or null where there are
        /// none.</param>
        /// <param name="selector">Builds the output row from the input row and the results.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>The open, completing with a cursor over the output rows once the whole window has been
        /// computed.</returns>
        public static async ValueTask<IClrCursor<TResult>> WindowAsync<TSource, TKey, TAccumulator, TResult>(
            ValueTask<IClrCursor<TSource>> source,
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
        /// <typeparam name="TAccumulator">The type holding every aggregate's state and last result.</typeparam>
        /// <typeparam name="TResult">The type of the output rows.</typeparam>
        /// <param name="collection">The collection the partitions were drained into, cleared before this method
        /// returns.</param>
        /// <param name="iterator">Iterates the partitions, each an array of rows in the comparator's
        /// order.</param>
        /// <param name="comparator">Orders the rows of one partition, and compares two of them for EXCLUDE and for
        /// RANK.</param>
        /// <param name="exclude">Which rows of the frame the aggregates do not see.</param>
        /// <param name="lowerBound">First index of the frame, before it is clamped to the partition.</param>
        /// <param name="upperBound">Last index of the frame, before it is clamped to the partition.</param>
        /// <param name="alwaysNonEmpty">Whether the bounds can be taken as they are, because the frame always
        /// holds the current row.</param>
        /// <param name="clampStart">Whether the lower bound has to be brought back to the first row of the
        /// partition.</param>
        /// <param name="clampEnd">Whether the upper bound has to be brought back to the last row of the
        /// partition.</param>
        /// <param name="lowerBoundCanChange">Whether the frame's start moves at all, which UNBOUNDED PRECEDING
        /// settles.</param>
        /// <param name="accumulatorInitializer">Creates the accumulator, once for the whole window.</param>
        /// <param name="reset">Returns the accumulator to its starting value, or null where no aggregate has
        /// one.</param>
        /// <param name="adder">Folds one row into the accumulator, or null where no aggregate reads the
        /// rows.</param>
        /// <param name="cachedResult">Computes the results that only change when the frame does, or null where
        /// every aggregate is recomputed per row.</param>
        /// <param name="uncachedResult">Computes the results that change on every row, or null where there are
        /// none.</param>
        /// <param name="selector">Builds the output row from the input row and the results.</param>
        /// <returns>One output row per input row, partition by partition.</returns>
        /// <remarks>
        /// The loop of the generated block, from the first <c>while</c> over the partition iterator to the
        /// <c>clear</c> of the partition collection. It does not read the input, so both opens share it once
        /// the drain has run.
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
            // with an exclusion other than NO OTHER, the same bounds do not mean the same rows from one current
            // row to the next, so every recomputed frame starts afresh
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

                    // with no cached result there is no frame to maintain; Calcite omits this block then
                    if (cachedResult != null)
                    {
                        var lowerChanged = lowerBoundCanChange && frame.Start != previousStart;

                        // Calcite's guard asks only whether the bounds moved, not about the exclusion, so an
                        // UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING frame is computed for row 0 and never again,
                        // and an EXCLUDE on it excludes nothing after row 0. This reproduces EnumerableWindow
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the partition key.</typeparam>
        /// <param name="source">The opened input, drained and disposed before this method returns.</param>
        /// <param name="partitionSelector">Key of the PARTITION BY clause, or null where there is none.</param>
        /// <param name="comparator">Orders the rows of each partition.</param>
        /// <returns>The list or <c>SortedMultiMap</c> holding the rows, and an iterator yielding each partition as
        /// a sorted array.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableWindow.getPartitionIterator</c>, which writes <c>source.into(tempList)</c> for a
        /// window with no PARTITION BY and a loop over the source into a <c>SortedMultiMap</c> otherwise; both
        /// read the source to its end and dispose it.
        ///
        /// <para>Calcite's runtime <c>SortedMultiMap</c> is used directly because it decides the order the
        /// partitions come out in (a hash map's), so a query with no ORDER BY returns rows in the same order as
        /// under <c>EnumerableConvention</c>. It also makes a null key a partition of its own, and its
        /// <c>arrays</c> sorts with the stable <c>Arrays.sort</c>, so rows the collation does not separate keep
        /// their arrival order.</para>
        /// </remarks>
        static (object Collection, java.util.Iterator Iterator) PartitionIterator<TSource, TKey>(IClrCursor<TSource> source, Func<TSource, TKey>? partitionSelector, java.util.Comparator comparator)
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
        /// <typeparam name="TSource">The type of the input rows.</typeparam>
        /// <typeparam name="TKey">The type of the partition key.</typeparam>
        /// <param name="source">The opened input, drained and disposed before the returned task completes.</param>
        /// <param name="partitionSelector">Key of the PARTITION BY clause, or null where there is none.</param>
        /// <param name="comparator">Orders the rows of each partition.</param>
        /// <param name="cancellationToken">Passed to each advance of the drain.</param>
        /// <returns>A task completing with the collection holding the rows and an iterator over the sorted
        /// partitions.</returns>
        static async ValueTask<(object Collection, java.util.Iterator Iterator)> PartitionIteratorAsync<TSource, TKey>(IClrCursor<TSource> source, Func<TSource, TKey>? partitionSelector, java.util.Comparator comparator, CancellationToken cancellationToken)
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
        /// Returns the initial capacity the generated block gives its output list.
        /// </summary>
        /// <param name="collection">The collection <see cref="PartitionIterator{TSource, TKey}"/>
        /// returned.</param>
        /// <returns>The number of partitions for a map, the number of rows for a list, and zero
        /// otherwise.</returns>
        /// <remarks>
        /// Mirrors <c>new ArrayList&lt;&gt;(collectionExpr.size())</c>. Calcite writes one <c>size</c> call and
        /// javac resolves it against the receiver: over a <c>SortedMultiMap</c> it is <c>HashMap.size</c> and
        /// counts partitions, and over the one-partition list it counts rows. This dispatches on the type to
        /// get the same result.
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
        /// Drops the buffered input, as the generated block does before returning the output list, so that
        /// the input can be collected.
        /// </summary>
        /// <param name="collection">The collection <see cref="PartitionIterator{TSource, TKey}"/>
        /// returned.</param>
        /// <remarks>
        /// Mirrors <c>collectionExpr.clear()</c>, which Calcite writes as <c>BuiltInMethod.MAP_CLEAR</c>; javac
        /// resolves it to <c>List.clear</c> when the receiver is the one-partition list, so this dispatches on
        /// the type.
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
        /// <param name="exclude">The window's exclusion, or null for none.</param>
        /// <param name="comparator">Orders the partition's rows; two rows it compares equal are peers.</param>
        /// <param name="rows">The partition's rows, sorted.</param>
        /// <param name="index">The row being evaluated.</param>
        /// <param name="position">The row that would be folded in.</param>
        /// <returns>True if the row at <paramref name="position"/> is excluded from the frame of the row at
        /// <paramref name="index"/>.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableWindow.buildExcludeGuard</c>. A peer is a row the comparator does not separate
        /// from the current one.
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

        /// <summary>
        /// Recognizes a pattern in each partition of the rows and emits what each match measures.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector">Selects the partition a row belongs to.</param>
        /// <param name="matcher">The automaton and one predicate per symbol, Calcite's own.</param>
        /// <param name="emitter">Turns one match into the rows it contributes.</param>
        /// <param name="history">How many rows back a predicate reads, which is how many the memory keeps.</param>
        /// <param name="future">How many rows forward a predicate reads.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>Enumerables.match</c>. Its anonymous <c>Enumerator</c> acquires the input in a
        /// field initializer, which runs at <c>enumerator()</c>; here the input arrives opened, which is the same
        /// moment. Each advance hands back a row the last match left queued, and otherwise reads one input row,
        /// feeds it to its partition and lets <c>matchOne</c> queue whatever that completes.
        ///
        /// <para>The matcher is Calcite's, reached as <c>Enumerables</c> reaches it from its own package:
        /// <c>matchOne</c> is protected, and the partition state and the match it hands back are package
        /// private types, so <see cref="MatcherMembers"/> calls each through a delegate.</para>
        /// </remarks>
        public static IClrCursor<TResult> Match<TSource, TKey, TResult>(
            IClrCursor<TSource> source,
            Func<TSource, TKey> keySelector,
            org.apache.calcite.runtime.Matcher matcher,
            org.apache.calcite.runtime.Enumerables.Emitter emitter,
            int history,
            int future)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(matcher);
            ArgumentNullException.ThrowIfNull(emitter);

            return new MatchCursor<TSource, TKey, TResult>(source, keySelector, matcher, emitter, history, future);
        }

        /// <summary>
        /// <see cref="Match{TSource, TKey, TResult}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<IClrCursor<TResult>> MatchAsync<TSource, TKey, TResult>(
            ValueTask<IClrCursor<TSource>> source,
            Func<TSource, TKey> keySelector,
            org.apache.calcite.runtime.Matcher matcher,
            org.apache.calcite.runtime.Enumerables.Emitter emitter,
            int history,
            int future,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ArgumentNullException.ThrowIfNull(matcher);
            ArgumentNullException.ThrowIfNull(emitter);

            return new MatchCursor<TSource, TKey, TResult>(await source.ConfigureAwait(false), keySelector, matcher, emitter, history, future);
        }

        /// <summary>
        /// The members of <c>Matcher</c> that <c>Enumerables.match</c> reaches from Calcite's own package and
        /// nothing outside it can call by name.
        /// </summary>
        static class MatcherMembers
        {

            static java.lang.reflect.Method Method(java.lang.Class declaring, string name, params java.lang.Class[] parameters)
            {
                var method = declaring.getDeclaredMethod(name, parameters);
                method.setAccessible(true);
                return method;
            }

            static java.lang.reflect.Field Field(java.lang.Class declaring, string name)
            {
                var field = declaring.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            }

            static java.lang.Class Nested(string name) =>
                ((java.lang.Class)typeof(org.apache.calcite.runtime.Matcher)).getDeclaredClasses().Single(c => c.getSimpleName() == name);

            static readonly java.lang.Class PartitionState = Nested("PartitionState");

            static readonly java.lang.Class PartialMatch = Nested("PartialMatch");

            /// <summary>
            /// <c>Matcher.createPartitionState(int, int)</c>, which is public and returns a package private type.
            /// </summary>
            public static readonly IKVM.Runtime.MH<object, int, int, object> CreatePartitionState =
                (IKVM.Runtime.MH<object, int, int, object>)JavaDelegates.FromMethod(Method((java.lang.Class)typeof(org.apache.calcite.runtime.Matcher), "createPartitionState", java.lang.Integer.TYPE, java.lang.Integer.TYPE));

            /// <summary>
            /// <c>PartitionState.getMemoryFactory()</c>.
            /// </summary>
            public static readonly IKVM.Runtime.MH<object, object> GetMemoryFactory =
                (IKVM.Runtime.MH<object, object>)JavaDelegates.FromMethod(Method(PartitionState, "getMemoryFactory"));

            /// <summary>
            /// <c>PartitionState.getRows()</c>.
            /// </summary>
            public static readonly IKVM.Runtime.MH<object, object> GetRows =
                (IKVM.Runtime.MH<object, object>)JavaDelegates.FromMethod(Method(PartitionState, "getRows"));

            /// <summary>
            /// <c>Matcher.matchOne(Memory, PartitionState, Consumer)</c>, which is protected.
            /// </summary>
            public static readonly IKVM.Runtime.MHV<object, object, object, object> MatchOne =
                (IKVM.Runtime.MHV<object, object, object, object>)JavaDelegates.FromMethod(Method((java.lang.Class)typeof(org.apache.calcite.runtime.Matcher), "matchOne", (java.lang.Class)typeof(org.apache.calcite.linq4j.MemoryFactory.Memory), PartitionState, (java.lang.Class)typeof(java.util.function.Consumer)));

            /// <summary>
            /// <c>PartialMatch.rows</c>.
            /// </summary>
            public static readonly IKVM.Runtime.MH<object, object> Rows =
                (IKVM.Runtime.MH<object, object>)JavaDelegates.FromGetter(Field(PartialMatch, "rows"));

            /// <summary>
            /// <c>PartialMatch.symbols</c>.
            /// </summary>
            public static readonly IKVM.Runtime.MH<object, object> Symbols =
                (IKVM.Runtime.MH<object, object>)JavaDelegates.FromGetter(Field(PartialMatch, "symbols"));

        }

        /// <summary>
        /// The cursor of <see cref="Match{TSource, TKey, TResult}"/>.
        /// </summary>
        sealed class MatchCursor<TSource, TKey, TResult> : ClrCursor<TResult>
        {

            readonly IClrCursor<TSource> source;
            readonly Func<TSource, TKey> keySelector;
            readonly org.apache.calcite.runtime.Matcher matcher;
            readonly org.apache.calcite.runtime.Enumerables.Emitter emitter;
            readonly int history;
            readonly int future;

            // the state of each partition, in a java.util.HashMap because the key is a Calcite value and its
            // equality is Java's
            readonly java.util.HashMap partitionStates = new();

            // the rows the matches completed so far produced and nothing has read yet, in the ArrayDeque
            // Calcite queues them in
            readonly java.util.ArrayDeque emitRows = new();

            readonly java.util.function.Consumer emitRowsAdd;
            readonly java.util.function.Consumer onMatch;

            int inputRow = -1;

            // Oracle numbers matches from 1
            int matchCounter = 1;

            TResult current = default!;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            public MatchCursor(
                IClrCursor<TSource> source,
                Func<TSource, TKey> keySelector,
                org.apache.calcite.runtime.Matcher matcher,
                org.apache.calcite.runtime.Enumerables.Emitter emitter,
                int history,
                int future)
            {
                this.source = source;
                this.keySelector = keySelector;
                this.matcher = matcher;
                this.emitter = emitter;
                this.history = history;
                this.future = future;

                emitRowsAdd = new DelegateConsumer(row => emitRows.add(row));
                onMatch = new DelegateConsumer(OnMatch);
            }

            /// <inheritdoc />
            public override TResult Current => current;

            /// <summary>
            /// What <c>Enumerables.match</c> passes <c>matchOne</c> as its consumer: each match is emitted, with
            /// the next number, into the queue. The row states are null, as they are there.
            /// </summary>
            /// <param name="match"></param>
            void OnMatch(object match)
            {
                emitter.emit((java.util.List)MatcherMembers.Rows(match), null, (java.util.List)MatcherMembers.Symbols(match), matchCounter++, emitRowsAdd);
            }

            /// <summary>
            /// Hands back a queued row, if there is one.
            /// </summary>
            /// <returns></returns>
            bool Poll()
            {
                var row = emitRows.pollFirst();
                if (row == null)
                    return false;

                current = JavaValues.As<TResult>(row);
                return true;
            }

            /// <summary>
            /// Feeds one input row to its partition, queueing whatever matches it completes.
            /// </summary>
            /// <param name="row"></param>
            void Feed(TSource row)
            {
                ++inputRow;

                var key = JavaValues.From(keySelector(row));

                // computeIfAbsent, which a map of non-null values answers the same way as a get and a put
                var partitionState = partitionStates.get(key);
                if (partitionState == null)
                {
                    partitionState = MatcherMembers.CreatePartitionState(matcher, history, future);
                    partitionStates.put(key, partitionState);
                }

                ((org.apache.calcite.linq4j.MemoryFactory)MatcherMembers.GetMemoryFactory(partitionState)).add(JavaValues.From(row));
                MatcherMembers.MatchOne(matcher, MatcherMembers.GetRows(partitionState), partitionState, onMatch);
            }

            /// <inheritdoc />
            public override bool Read()
            {
                for (; ; )
                {
                    if (Poll())
                        return true;

                    // no rows are ready to emit: read the next input row, and see whether it completes a match
                    if (source.Read() == false)
                        return false;

                    Feed(source.Current);
                }
            }

            /// <inheritdoc />
            public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                for (; ; )
                {
                    if (Poll())
                        return true;

                    if (await source.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                        return false;

                    Feed(source.Current);
                }
            }

            /// <inheritdoc />
            public override void Dispose() => source.Dispose();

            /// <inheritdoc />
            public override ValueTask DisposeAsync() => source.DisposeAsync();

        }

        /// <summary>
        /// A <see cref="java.util.function.Consumer"/> over a delegate: what Calcite writes as a method
        /// reference or a lambda where a match is handed on.
        /// </summary>
        sealed class DelegateConsumer(Action<object> action) : java.util.function.Consumer
        {

            /// <inheritdoc />
            public void accept(object t) => action(t);

            // C# does not inherit the defaults of an interface IKVM compiled, so the one there is is forwarded

            /// <inheritdoc />
            public java.util.function.Consumer andThen(java.util.function.Consumer after) => java.util.function.Consumer.__DefaultMethods.andThen(this, after);

        }


    }

}
