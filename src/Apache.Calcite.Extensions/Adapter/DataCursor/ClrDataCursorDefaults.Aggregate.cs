using System;
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

    }

}
