using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The joins: the hash join, the semi join, the mark joins, the merge join and the nested loop join.
    /// </summary>
    /// <remarks>
    /// Each acquires at the moment its linq4j original does. A hash join drains its build side inside
    /// <c>enumerator()</c>, so the open drains it and the cursor probes it with the other input, which
    /// arrives opened. A semi join and a nested loop join acquire their inner inside <c>moveNext</c> — the
    /// first memoized to the first outer row, the second once per outer row — so those take the inner as
    /// openers of both kinds and call the one matching the advance. A merge join positions both inputs
    /// inside <c>enumerator()</c>, and the open does. <c>nestedLoopJoinAsList</c> builds the whole result
    /// where it is called, and here the call is the open.
    /// </remarks>
    static partial class ClrDataCursorDefaults
    {

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

    }

}
