using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorDefaults
    {

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

    }

}
