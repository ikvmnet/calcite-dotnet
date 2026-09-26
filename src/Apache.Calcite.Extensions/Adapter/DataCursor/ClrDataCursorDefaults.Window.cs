using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorDefaults
    {

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
