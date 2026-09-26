using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorDefaults
    {

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

    }

}
