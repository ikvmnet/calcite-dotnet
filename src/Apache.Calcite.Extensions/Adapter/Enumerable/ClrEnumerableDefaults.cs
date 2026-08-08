using System;
using System.Collections.Generic;
using System.Linq;

using org.apache.calcite.linq4j.function;

using Apache.Calcite.Extensions.Runtime;

using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The sequence operators a plan of the <see cref="ClrEnumerableConvention"/> calling convention is built
    /// from.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>EnumerableDefaults</c>, taking the same arguments under .NET names. Most
    /// of these are what <see cref="Enumerable"/> already does and say so; the ones that are not are the ones
    /// SQL needs and .NET has no operator for.
    /// </remarks>
    static class ClrEnumerableDefaults
    {

        /// <summary>
        /// Returns the first field of each row.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// A one column result is the value, not a one element row. Calcite ends a plan the same way, with
        /// <c>Enumerables.slice0</c>.
        /// </remarks>
        public static IEnumerable<TRow> Slice0<TRow>(IEnumerable<object[]> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            // the row came from a table and its fields are still Java's, so the field taken out of it is
            // converted rather than cast. Calcite's slice0 is Enumerable<E[]> to Enumerable<E> and needs
            // neither: a linq4j Enumerable erases its element type, so nothing downstream can tell what came
            // out, and a CLR sequence says so in its own type.
            foreach (var row in source)
                yield return JavaValues.As<TRow>(row[0]);
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
        /// advances the input until the condition holds, and <c>current</c> projects. Generated Java source is
        /// the only place Calcite can put a custom enumerator; here it is an ordinary iterator, so the two
        /// steps are still one pass over the input.
        /// </remarks>
        public static IEnumerable<TResult> Calc<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            foreach (var row in source)
                if (predicate == null || predicate(row))
                    yield return selector(row);
        }

        /// <summary>
        /// Returns the rows that satisfy a condition.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Where<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            return source.Where(predicate);
        }

        /// <summary>
        /// Projects each row into a new form.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IEnumerable<TResult> Select<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, TResult> selector)
        {
            return source.Select(selector);
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
        /// The comparator is Java's, because that is what <c>PhysType.generateCollationKey</c> yields and by
        /// two different routes: a method call returning one when there is a single collation, and an
        /// anonymous class when there are several. Taking the interface is what lets both arrive here.
        /// </remarks>
        public static IEnumerable<TSource> OrderBy<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator)
        {
            return comparator == null
                ? source.OrderBy(keySelector)
                : source.OrderBy(keySelector, Comparer<TKey>.Create((x, y) => comparator.compare(x, y)));
        }

        /// <summary>
        /// Skips a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Skip<TSource>(IEnumerable<TSource> source, int count)
        {
            return source.Skip(count);
        }

        /// <summary>
        /// Takes a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Take<TSource>(IEnumerable<TSource> source, int count)
        {
            return source.Take(count);
        }

        /// <summary>
        /// Returns the rows of both sequences, keeping duplicates.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Concat<TSource>(IEnumerable<TSource> source, IEnumerable<TSource> other)
        {
            return source.Concat(other);
        }

        /// <summary>
        /// Returns the distinct rows of both sequences.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Union<TSource>(IEnumerable<TSource> source, IEnumerable<TSource> other, EqualityComparer? comparer)
        {
            // a java.util.HashSet, and not because the CLR has nothing to hold rows in: what a set operator
            // yields a row in is the order of the collection it held them in, and Calcite's is this one. See
            // JavaHashingTests for why that order is the same in every process.
            var set = new java.util.HashSet();
            foreach (var row in source)
                set.add(JavaWrapped.Of(comparer, JavaValues.From(row)));
            foreach (var row in other)
                set.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            return Unwrap<TSource>(set);
        }

        /// <summary>
        /// Returns the values of a Java collection, in its order, each unwrapped and converted back.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="collection"></param>
        /// <returns></returns>
        static IEnumerable<TSource> Unwrap<TSource>(java.lang.Iterable collection)
        {
            for (var i = collection.iterator(); i.hasNext();)
                yield return JavaValues.As<TSource>(JavaWrapped.Unwrap(i.next()));
        }

        /// <summary>
        /// Returns the rows in both sequences.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <param name="all">Whether a row present more than once in each is returned more than once.</param>
        /// <returns></returns>
        public static IEnumerable<TSource> Intersect<TSource>(IEnumerable<TSource> source, IEnumerable<TSource> other, EqualityComparer? comparer, bool all)
        {
            // ALL keeps a row once per pairing, so the collection counts rather than merely holding
            var set1 = Collection(all);
            foreach (var row in other)
                set1.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            var result = Collection(all);
            foreach (var row in source)
                if (set1.remove(JavaWrapped.Of(comparer, JavaValues.From(row))))
                    result.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            return Unwrap<TSource>(result);
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
        /// Returns each row as many times as it appears in both sequences.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        static IEnumerable<TSource> IntersectAll<TSource>(IEnumerable<TSource> source, IEnumerable<TSource> other, IEqualityComparer<TSource> comparer)
        {
            var counts = Count(other, comparer);

            foreach (var row in source)
            {
                if (counts.TryGetValue(row, out var remaining) == false || remaining == 0)
                    continue;

                counts[row] = remaining - 1;
                yield return row;
            }
        }

        /// <summary>
        /// Returns the rows of the first sequence that are not in the second.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <param name="all">Whether a row is removed once per appearance in the second rather than entirely.</param>
        /// <returns></returns>
        public static IEnumerable<TSource> Except<TSource>(IEnumerable<TSource> source, IEnumerable<TSource> other, EqualityComparer? comparer, bool all)
        {
            var collection = Collection(all);
            foreach (var row in source)
                collection.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            foreach (var row in other)
                collection.remove(JavaWrapped.Of(comparer, JavaValues.From(row)));

            return Unwrap<TSource>(collection);
        }

        /// <summary>
        /// Returns each row of the first sequence except as many times as it appears in the second.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        static IEnumerable<TSource> ExceptAll<TSource>(IEnumerable<TSource> source, IEnumerable<TSource> other, IEqualityComparer<TSource> comparer)
        {
            var counts = Count(other, comparer);

            foreach (var row in source)
            {
                if (counts.TryGetValue(row, out var remaining) && remaining > 0)
                {
                    counts[row] = remaining - 1;
                    continue;
                }

                yield return row;
            }
        }

        /// <summary>
        /// Counts how many times each row appears.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        static Dictionary<TSource, int> Count<TSource>(IEnumerable<TSource> source, IEqualityComparer<TSource> comparer)
        {
            var counts = new Dictionary<TSource, int>(comparer);

            foreach (var row in source)
                counts[row] = counts.TryGetValue(row, out var count) ? count + 1 : 1;

            return counts;
        }

        /// <summary>
        /// Returns the distinct rows of a sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Distinct<TSource>(IEnumerable<TSource> source, EqualityComparer? comparer)
        {
            var set = new java.util.HashSet();
            foreach (var row in source)
                set.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            return Unwrap<TSource>(set);
        }

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
        /// </remarks>
        public static IEnumerable<TResult> LeftMarkHashJoin<TSource, TInner, TKey, TNsKey, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeyNullAwareSelector,
            Func<TInner, TKey> innerKeyNullAwareSelector,
            Func<TSource, TNsKey>? outerNullSafeKeySelector,
            Func<TInner, TNsKey>? innerNullSafeKeySelector,
            bool atMostOneNotNullSafeKey,
            Func<TSource, java.lang.Boolean, TResult> resultSelector,
            EqualityComparer? comparer,
            EqualityComparer? nullSafeComparer,
            Func<TSource, TInner, java.lang.Boolean>? nonEquiPredicate,
            Func<TSource, TInner, java.lang.Boolean> equiPredicate)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);

            // the build side: one bucket per key, plus the set of null-safe keys seen. A null key goes into
            // the map under null rather than being dropped, because the rows behind it are what say UNKNOWN
            var lookup = new java.util.HashMap();
            var nullSafeKeys = new java.util.HashSet();

            foreach (var row in inner)
            {
                var key = innerKeyNullAwareSelector(row);
                var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));

                if (lookup.get(wrapped) is not List<TInner> bucket)
                    lookup.put(wrapped, bucket = []);

                bucket.Add(row);

                if (innerNullSafeKeySelector != null)
                    nullSafeKeys.add(JavaWrapped.Of(nullSafeComparer, JavaValues.From(innerNullSafeKeySelector(row))));
            }

            var buildSideIsEmpty = lookup.isEmpty();

            foreach (var row in outer)
            {
                var marker = java.lang.Boolean.FALSE;

                if (outerNullSafeKeySelector != null
                    && nullSafeKeys.contains(JavaWrapped.Of(nullSafeComparer, JavaValues.From(outerNullSafeKeySelector(row)))) == false)
                {
                    // a null-safe key matching nothing settles it: two rows that disagree there are not equal,
                    // whatever the rest of the condition says
                    yield return resultSelector(row, marker);
                    continue;
                }

                var key = outerKeyNullAwareSelector(row);

                if (key == null)
                {
                    if (atMostOneNotNullSafeKey)
                    {
                        // the one EQUALS key is null on this row, so every comparison it makes is unknown
                        marker = null!;
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
                                    marker = null!;
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
                                marker = null!;
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
                        marker = null!;
                    }
                    else
                    {
                        foreach (var other in nulls)
                        {
                            if (equiPredicate(row, other) == null)
                            {
                                marker = null!;
                                break;
                            }
                        }
                    }
                }

                // an empty build side is FALSE and never UNKNOWN: there was nothing to be unknown about
                if (marker == null && buildSideIsEmpty)
                    marker = java.lang.Boolean.FALSE;

                yield return resultSelector(row, marker);
            }
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
        /// Joins two sequences on a key.
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
        public static IEnumerable<TResult> HashJoin<TSource, TInner, TKey, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool>? predicate)
        {
            return predicate == null
                ? HashEquiJoin(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight)
                : HashJoinWithPredicate(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
        }

        /// <summary>
        /// Joins two sequences on a key alone.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.hashEquiJoin_</c>. What is left over at the end is a
        /// <em>key</em> no outer row carried, and every build row under it comes out together.
        /// </remarks>
        static IEnumerable<TResult> HashEquiJoin<TSource, TInner, TKey, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight)
        {
            // the lookup is a java.util.HashMap, as linq4j's toLookup builds one: a right or a full join ends
            // with the rows of the right input that matched nothing, and the order those come out in is this
            // map's. See JavaHashingTests for why that order is the same in every process.
            var lookup = new java.util.HashMap();

            foreach (var row in inner)
            {
                var key = innerKeySelector(row);

                // a null key is kept under a null key, as toLookup keeps one, and nothing probes it
                var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                if (lookup.get(wrapped) is not List<TInner> bucket)
                    lookup.put(wrapped, bucket = []);

                bucket.Add(row);
            }

            // every key the build side has, less the ones an outer row carries. Calcite keeps it this way
            // round, and it matters where the lookup holds a key nothing probes: that key's rows are what a
            // right or a full join owes against a null left.
            var unmatched = generateNullsOnLeft ? new java.util.HashSet(lookup.keySet()) : null;

            foreach (var row in outer)
            {
                var key = outerKeySelector(row);
                var any = false;

                if (key != null)
                {
                    var wrapped = JavaWrapped.Of(comparer, JavaValues.From(key));
                    unmatched?.remove(wrapped);

                    if (lookup.get(wrapped) is List<TInner> bucket)
                    {
                        foreach (var other in bucket)
                        {
                            any = true;
                            yield return resultSelector(row, other);
                        }
                    }
                }

                if (any == false && generateNullsOnRight)
                    yield return resultSelector(row, default!);
            }

            if (unmatched == null)
                yield break;

            for (var i = lookup.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                if (unmatched.contains(entry.getKey()) == false)
                    continue;

                foreach (var other in (List<TInner>)entry.getValue())
                    yield return resultSelector(default!, other);
            }
        }

        /// <summary>
        /// Joins two sequences on a key and something else besides.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.hashJoinWithPredicate_</c>. What is left over at the end
        /// is a <em>row</em> the predicate rejected or the key never reached, and the leftovers come out in
        /// the build input's own order rather than the lookup's.
        ///
        /// <para>Per row and not per key, which is the whole difference. A build row whose key matched but
        /// whose predicate did not has matched nothing, and a right join owes it a row against a null left.
        /// Tracking the key instead lost it, because some other row under that key had passed.</para>
        /// </remarks>
        static IEnumerable<TResult> HashJoinWithPredicate<TSource, TInner, TKey, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate)
        {
            // read once, because a right or a full join walks it twice
            IEnumerable<TInner> innerToLookUp = generateNullsOnLeft ? new List<TInner>(inner) : inner;

            var lookup = new java.util.HashMap();

            foreach (var row in innerToLookUp)
            {
                var key = innerKeySelector(row);

                var wrapped = key == null ? null : JavaWrapped.Of(comparer, JavaValues.From(key));
                if (lookup.get(wrapped) is not List<TInner> bucket)
                    lookup.put(wrapped, bucket = []);

                bucket.Add(row);
            }

            var unmatched = generateNullsOnLeft ? new List<TInner>(innerToLookUp) : null;

            foreach (var row in outer)
            {
                var key = outerKeySelector(row);
                var any = false;

                if (key != null && lookup.get(JavaWrapped.Of(comparer, JavaValues.From(key))) is List<TInner> bucket)
                {
                    var accepted = new List<TInner>();
                    foreach (var other in bucket)
                        if (predicate(row, other))
                            accepted.Add(other);

                    unmatched?.RemoveAll(accepted.Contains);

                    foreach (var other in accepted)
                    {
                        any = true;
                        yield return resultSelector(row, other);
                    }
                }

                if (any == false && generateNullsOnRight)
                    yield return resultSelector(row, default!);
            }

            if (unmatched == null)
                yield break;

            foreach (var other in unmatched)
                yield return resultSelector(default!, other);
        }

        /// <summary>
        /// Returns the rows of the first sequence that have, or have not, a match in the second.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="outerKeySelector"></param>
        /// <param name="innerKeySelector"></param>
        /// <param name="comparer"></param>
        /// <param name="anti">Whether the rows without a match are the ones returned.</param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> SemiJoin<TSource, TInner, TKey>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool>? predicate)
        {
            var equality = JavaEqualityComparer<TKey>.Of(comparer);
            var lookup = new Dictionary<TKey, List<TInner>>(equality);

            foreach (var row in inner)
            {
                var key = innerKeySelector(row);
                if (key == null)
                    continue;

                if (lookup.TryGetValue(key, out var bucket) == false)
                    lookup[key] = bucket = [];

                bucket.Add(row);
            }

            foreach (var row in outer)
            {
                var key = outerKeySelector(row);
                var any = false;

                if (key != null && lookup.TryGetValue(key, out var bucket))
                    foreach (var other in bucket)
                        if (predicate == null || predicate(row, other))
                        {
                            any = true;
                            break;
                        }

                if (any != anti)
                    yield return row;
            }
        }

        /// <summary>
        /// Joins each row of the first sequence to the one row of the second that has the same key and the
        /// nearest timestamp satisfying the match condition.
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
        /// </remarks>
        public static IEnumerable<TResult> AsofJoin<TSource, TInner, TKey, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> matchComparator,
            java.util.Comparator timestampComparator,
            bool emitNullsOnRight)
        {
            var leftIndex = new java.util.HashMap();
            var rightIndex = new java.util.HashMap();
            var outerWithNullKeys = new List<TSource>();

            foreach (var row in outer)
            {
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

            foreach (var row in inner)
            {
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

            for (var i = leftIndex.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                var left = (List<TSource>)entry.getValue();
                var best = (List<TInner>)rightIndex.get(entry.getKey());

                for (int j = 0; j < left.Count; j++)
                {
                    if (best[j] == null && emitNullsOnRight == false)
                        continue;

                    yield return resultSelector(left[j], best[j]);
                }
            }

            foreach (var row in outerWithNullKeys)
                yield return resultSelector(row, default!);
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
        /// which is the whole point of it against <see cref="GroupBy"/>, and the groups come out in the
        /// order the input was sorted in rather than a map's.
        /// </remarks>
        public static IEnumerable<TResult> SortedGroupBy<TSource, TKey, TResult>(
            IEnumerable<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            java.util.Comparator comparator)
        {
            object? accumulator = null;
            object? previousKey = null;
            var any = false;

            foreach (var row in source)
            {
                var key = JavaValues.From(keySelector(row));

                if (any && comparator.compare(previousKey, key) != 0)
                {
                    yield return JavaValues.As<TResult>(resultSelector.apply(previousKey, accumulator));
                    accumulator = null;
                }

                accumulator ??= accumulatorInitializer.apply();
                accumulator = accumulatorAdder.apply(accumulator, row);
                previousKey = key;
                any = true;
            }

            if (any)
                yield return JavaValues.As<TResult>(resultSelector.apply(previousKey, accumulator));
        }

        /// <summary>
        /// Returns the rows of sequences that are each already sorted on the key, in that order.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="sources"></param>
        /// <param name="sortKeySelector"></param>
        /// <param name="sortComparator"></param>
        /// <param name="all">Whether a row that repeats is kept.</param>
        /// <param name="comparer">Decides whether two rows are the same, where duplicates are dropped.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.mergeUnion</c> and its <c>MergeUnionEnumerator</c>: take
        /// the smallest row across the inputs, emit it, and advance that input alone.
        ///
        /// <para>Dropping duplicates does not need every row emitted so far, only the ones sharing the
        /// current key: the inputs are sorted, so a row that repeats one already emitted arrives before the
        /// key changes. That is Calcite's reasoning and its set is cleared the same way.</para>
        /// </remarks>
        public static IEnumerable<TSource> MergeUnion<TSource, TKey>(
            java.util.List sources,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer)
        {
            var inputs = new IEnumerator<TSource>[sources.size()];
            for (int i = 0; i < inputs.Length; i++)
                inputs[i] = ((IEnumerable<TSource>)sources.get(i)).GetEnumerator();

            var current = new TSource[inputs.Length];
            var finished = new bool[inputs.Length];
            var active = inputs.Length;

            // only where duplicates are dropped, and only ever holding the rows of one key
            var processed = all ? null : new java.util.HashSet();
            object? keyInProcessed = null;

            void Move(int i)
            {
                if (inputs[i].MoveNext() == false)
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

            try
            {
                for (int i = 0; i < inputs.Length; i++)
                    Move(i);

                while (active > 0)
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

                    var value = current[candidate];
                    var emit = NotDuplicated(value);
                    Move(candidate);

                    if (emit)
                        yield return value;
                }
            }
            finally
            {
                foreach (var input in inputs)
                    input.Dispose();
            }
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
        /// Joins two sequences that are already sorted on the key, ascending with nulls last.
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
        /// The counterpart of <c>EnumerableDefaults.mergeJoin</c>, statement for statement, as an iterator
        /// rather than the enumerator with a state machine that Java needs. Both inputs are walked once and
        /// only the rows of one key are held.
        ///
        /// <para>Two nulls must not compare equal, or a join of two null keys would return rows SQL says it
        /// does not. Calcite signals that out of its comparator by throwing, and catches it to advance the
        /// right side; the generated comparator this is called with is that comparator, so the same throw is
        /// caught here — by name, since the exception class is package private.</para>
        /// </remarks>
        public static IEnumerable<TResult> MergeJoin<TSource, TInner, TKey, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, bool>? predicate,
            Func<TSource, TInner, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            java.util.Comparator? comparator,
            EqualityComparer? comparer)
        {
            if (IsMergeJoinSupported(joinType) == false)
                throw new java.lang.UnsupportedOperationException($"MergeJoin unsupported for join type {joinType}");

            var name = joinType.name();
            var isLeft = name == nameof(org.apache.calcite.linq4j.JoinType.LEFT);
            var isAnti = name == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            var isSemi = name == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            var isLeftOrAnti = isLeft || isAnti;
            var equality = JavaEqualityComparer<TKey>.Of(comparer);

            var lefts = new List<TSource>();
            var rights = new List<TInner>();
            var done = false;
            var remainingLeft = false;
            IEnumerable<TResult>? results = null;

            using var leftEnumerator = outer.GetEnumerator();
            using var rightEnumerator = inner.GetEnumerator();

            // the left enumerator advanced, and onto a row whose key is not null — a LEFT join reads its
            // left input to the end whatever the keys are, because every row of it is a result
            bool LeftMoveNext() => leftEnumerator.MoveNext() && (isLeft || outerKeySelector(leftEnumerator.Current) != null);
            bool RightMoveNext() => rightEnumerator.MoveNext() && innerKeySelector(rightEnumerator.Current) != null;

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

                while (leftEnumerator.MoveNext())
                {
                    left = leftEnumerator.Current;
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

                while (rightEnumerator.MoveNext())
                {
                    right = rightEnumerator.Current;
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
                    var left = leftEnumerator.Current;
                    var leftKey = outerKeySelector(left);
                    var right = rightEnumerator.Current;
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

                        var c = Compare(leftKey, rightKey);
                        if (c == 0)
                            break;

                        if (c < 0)
                        {
                            if (isLeftOrAnti)
                            {
                                // this row, and every other with the same key, is a result on its own
                                if (AdvanceLeft(left, leftKey) == false)
                                    done = true;

                                results = Cartesian(lefts, [default(TInner)!], resultSelector);
                                return true;
                            }

                            if (leftEnumerator.MoveNext() == false)
                            {
                                done = true;
                                return false;
                            }

                            left = leftEnumerator.Current;
                            leftKey = outerKeySelector(left);
                        }
                        else
                        {
                            if (rightEnumerator.MoveNext() == false)
                            {
                                if (isLeftOrAnti)
                                {
                                    remainingLeft = true;
                                    return true;
                                }

                                done = true;
                                return false;
                            }

                            right = rightEnumerator.Current;
                            rightKey = innerKeySelector(right);
                        }
                    }

                    if (AdvanceLeft(left, leftKey) == false)
                        done = true;

                    if (AdvanceRight(right, rightKey) == false)
                    {
                        if (done == false && isLeftOrAnti)
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
                            ? Cartesian(lefts, [rights[0]], resultSelector)
                            : Cartesian(lefts, rights, resultSelector);
                    }
                    else
                    {
                        // the rest of the condition still has to hold, and a nested loop over the two runs is
                        // what decides it
                        results = NestedLoopJoin([.. lefts], [.. rights], resultSelector, predicate, joinType);
                    }

                    return true;
                }
            }

            if (isLeftOrAnti)
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

            while (true)
            {
                if (results != null)
                {
                    foreach (var row in results)
                        yield return row;

                    results = null;
                }

                if (remainingLeft)
                {
                    yield return resultSelector(leftEnumerator.Current, default!);

                    if (LeftMoveNext() == false)
                    {
                        remainingLeft = false;
                        done = true;
                    }

                    continue;
                }

                if (done)
                    yield break;

                if (Advance() == false)
                    yield break;
            }
        }

        /// <summary>
        /// Returns every pairing of two sequences, in order.
        /// </summary>
        static IEnumerable<TResult> Cartesian<TSource, TInner, TResult>(IReadOnlyList<TSource> outer, IReadOnlyList<TInner> inner, Func<TSource, TInner, TResult> resultSelector)
        {
            var rows = new List<TResult>(outer.Count * inner.Count);
            foreach (var left in outer)
                foreach (var right in inner)
                    rows.Add(resultSelector(left, right));

            return rows;
        }

        /// <summary>
        /// Orders two keys with nulls last, refusing to call two nulls equal.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.compareNullsLastForMergeJoin</c>, reached only where no
        /// comparator was given. Two nulls are not equal, and the caller takes the left as the bigger.
        /// </remarks>
        static int CompareNullsLastForMergeJoin<TKey>(TKey a, TKey b)
        {
            if (a == null && b == null)
                return 1;

            if (a == null)
                return 1;
            if (b == null)
                return -1;

            // IKVM maps java.lang.Comparable onto IComparable, so this is the key's own compareTo
            return ((IComparable)JavaValues.From(a)).CompareTo(JavaValues.From(b));
        }

        /// <summary>
        /// Joins by running the right input once per batch of left rows, rather than once per row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="joinType"></param>
        /// <param name="outer"></param>
        /// <param name="inner">Yields the right rows for a batch of left rows.</param>
        /// <param name="resultSelector"></param>
        /// <param name="predicate"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.correlateBatchJoin</c>. The right input is a filter over
        /// a disjunction of the batch's conditions, so one pass of it serves every row of the batch.
        ///
        /// <para>It is read the way Calcite reads it: the batch's <em>first</em> left row pulls from it and
        /// caches each row as it goes, and every left row after that reads the cache. So the right input is
        /// never enumerated further than the first left row needed — which is what a LIMIT above the join
        /// asks for, and is the only thing this shape buys over materialising it up front. The one place the
        /// first row would stop early is a semi or anti join finding its match, and there it finishes reading
        /// first, because the rest of the batch reads what it cached. Calcite does exactly that, for exactly
        /// that reason.</para>
        /// </remarks>
        public static IEnumerable<TResult> CorrelateBatchJoin<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            IEnumerable<TSource> outer,
            Func<java.util.List, IEnumerable<TInner>> inner,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            int batchSize)
        {
            var name = joinType.name();
            var isSemi = name == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            var isAnti = name == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            var isLeft = name == nameof(org.apache.calcite.linq4j.JoinType.LEFT);

            var batch = new List<TSource>(batchSize);
            var rows = new List<TInner>();

            using var enumerator = outer.GetEnumerator();

            // the right rows of the batch being read, held across the batch's first left row because that is
            // the only one that pulls from it
            IEnumerator<TInner>? source = null;

            try
            {
                while (true)
                {
                    batch.Clear();
                    while (batch.Count < batchSize && enumerator.MoveNext())
                        batch.Add(enumerator.Current);

                    if (batch.Count == 0)
                        yield break;

                    // a short batch is padded by repeating its first row, as Calcite pads it: the condition
                    // is a disjunction, so a row that repeats adds nothing to it
                    var padded = new java.util.ArrayList(batchSize);
                    for (int i = 0; i < batchSize; i++)
                        padded.add(JavaValues.From(batch[i < batch.Count ? i : 0]));

                    rows.Clear();
                    source?.Dispose();
                    source = (inner(padded) is IEnumerable<TInner> sequence ? sequence : []).GetEnumerator();

                    for (int i = 0; i < batch.Count; i++)
                    {
                        var left = batch[i];
                        var any = false;

                        for (int j = 0; ; j++)
                        {
                            TInner right;

                            if (i == 0)
                            {
                                if (source.MoveNext() == false)
                                    break;

                                right = source.Current;
                                rows.Add(right);
                            }
                            else
                            {
                                if (j == rows.Count)
                                    break;

                                right = rows[j];
                            }

                            if (predicate(left, right) == false)
                                continue;

                            any = true;

                            // an anti join wants the rows that match nothing, and a semi join wants each
                            // left row once, so both stop at the first match — but the first left row of a
                            // batch reads the rest of the right input before it stops, because every row
                            // after it reads what this one cached
                            if (isSemi || isAnti)
                            {
                                if (i == 0)
                                    while (source.MoveNext())
                                        rows.Add(source.Current);

                                if (isAnti == false)
                                    yield return resultSelector(left, right);

                                break;
                            }

                            yield return resultSelector(left, right);
                        }

                        if (any == false && (isLeft || isAnti))
                            yield return resultSelector(left, default!);
                    }
                }
            }
            finally
            {
                source?.Dispose();
            }
        }

        /// <summary>
        /// Returns every left row with a marker saying whether the right side had a match.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
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
        /// </remarks>
        public static IEnumerable<TResult> LeftMarkNestedLoopJoin<TSource, TInner, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TInner, java.lang.Boolean> predicate,
            Func<TSource, java.lang.Boolean, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(inner);

            return LeftMarkJoin(outer, _ => inner, predicate, resultSelector);
        }

        /// <summary>
        /// Returns every left row with a marker saying whether its own right side had a match.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">Yields the right rows for one left row.</param>
        /// <param name="predicate">Three-valued: null where the comparison is unknown.</param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.correlateLeftMarkJoin</c>: the correlated half of a mark
        /// join, and the same walk as <see cref="LeftMarkNestedLoopJoin"/>. Calcite writes both against one
        /// <c>leftMarkJoinInternal</c>, and so does this.
        /// </remarks>
        public static IEnumerable<TResult> CorrelateLeftMarkJoin<TSource, TInner, TResult>(
            IEnumerable<TSource> outer,
            Func<TSource, IEnumerable<TInner>> inner,
            Func<TSource, TInner, java.lang.Boolean> predicate,
            Func<TSource, java.lang.Boolean, TResult> resultSelector)
        {
            return LeftMarkJoin(outer, inner, predicate, resultSelector);
        }

        /// <summary>
        /// The walk both mark joins over a nested loop make.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner"></param>
        /// <param name="predicate"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.leftMarkJoinInternal</c>.
        /// </remarks>
        static IEnumerable<TResult> LeftMarkJoin<TSource, TInner, TResult>(
            IEnumerable<TSource> outer,
            Func<TSource, IEnumerable<TInner>> inner,
            Func<TSource, TInner, java.lang.Boolean> predicate,
            Func<TSource, java.lang.Boolean, TResult> resultSelector)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(resultSelector);

            foreach (var left in outer)
            {
                var marker = java.lang.Boolean.FALSE;
                var rows = inner(left);

                if (rows != null)
                {
                    foreach (var right in rows)
                    {
                        var matched = predicate(left, right);

                        if (matched == null)
                            marker = null!;
                        else if (matched.booleanValue())
                        {
                            marker = java.lang.Boolean.TRUE;
                            break;
                        }
                    }
                }

                yield return resultSelector(left, marker);
            }
        }

        /// <summary>
        /// Joins two sequences on a condition, comparing every pair.
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
        /// The counterpart of <c>EnumerableDefaults.nestedLoopJoin</c>. The inner sequence is read once and
        /// kept, because it is walked again for every outer row.
        /// </remarks>
        public static IEnumerable<TResult> NestedLoopJoin<TSource, TInner, TResult>(
            IEnumerable<TSource> outer,
            IEnumerable<TInner> inner,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            var rows = inner as IReadOnlyList<TInner> ?? [.. inner];
            var name = joinType.name();
            var nullsOnRight = name is nameof(org.apache.calcite.linq4j.JoinType.LEFT) or nameof(org.apache.calcite.linq4j.JoinType.FULL);
            var nullsOnLeft = name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL);
            var semi = name == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            var anti = name == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            var matched = nullsOnLeft ? new bool[rows.Count] : null;

            foreach (var row in outer)
            {
                var any = false;

                for (int i = 0; i < rows.Count; i++)
                {
                    if (predicate(row, rows[i]) == false)
                        continue;

                    any = true;
                    if (matched != null)
                        matched[i] = true;

                    if (semi || anti)
                        break;

                    yield return resultSelector(row, rows[i]);
                }

                if (semi && any)
                    yield return resultSelector(row, default!);
                else if (anti && any == false)
                    yield return resultSelector(row, default!);
                else if (any == false && nullsOnRight)
                    yield return resultSelector(row, default!);
            }

            if (matched == null)
                yield break;

            for (int i = 0; i < rows.Count; i++)
                if (matched[i] == false)
                    yield return resultSelector(default!, rows[i]);
        }

        /// <summary>
        /// Joins each row of a sequence to the rows a function of it yields.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TInner"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="outer"></param>
        /// <param name="inner">The sequence for one outer row, which is what makes the join correlated.</param>
        /// <param name="resultSelector"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.correlateJoin</c>.
        /// </remarks>
        public static IEnumerable<TResult> CorrelateJoin<TSource, TInner, TResult>(
            IEnumerable<TSource> outer,
            Func<TSource, IEnumerable<TInner>> inner,
            Func<TSource, TInner, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType)
        {
            var name = joinType.name();
            var semi = name == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            var anti = name == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            var nullsOnRight = name == nameof(org.apache.calcite.linq4j.JoinType.LEFT);

            foreach (var row in outer)
            {
                var any = false;

                foreach (var other in inner(row))
                {
                    any = true;

                    if (semi || anti)
                        break;

                    yield return resultSelector(row, other);
                }

                if (semi && any)
                    yield return resultSelector(row, default!);
                else if (anti && any == false)
                    yield return resultSelector(row, default!);
                else if (any == false && nullsOnRight)
                    yield return resultSelector(row, default!);
            }
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
        /// </remarks>
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TResult>(
            IEnumerable<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer)
        {
            // a java.util.HashMap, because the order the groups come out in is the map's and Calcite's is
            // this one. Holding the insertion order instead gave a different answer to the same GROUP BY.
            var accumulators = new java.util.HashMap();

            foreach (var row in source)
            {
                var key = JavaWrapped.Of(comparer, JavaValues.From(keySelector(row)));
                var accumulator = accumulators.get(key) ?? accumulatorInitializer.apply();

                accumulators.put(key, accumulatorAdder.apply(accumulator, row));
            }

            for (var i = accumulators.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                yield return JavaValues.As<TResult>(resultSelector.apply(JavaWrapped.Unwrap(entry.getKey()), entry.getValue()));
            }
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
        /// </remarks>
        public static IEnumerable<TResult> GroupByMultiple<TSource, TKey, TResult>(
            IEnumerable<TSource> source,
            Func<TSource, TKey>[] keySelectors,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer)
        {
            // a java.util.HashMap, for the reason GroupBy gives: the order the groups come out in is the
            // map's, and Calcite's map is this one
            var accumulators = new java.util.HashMap();

            foreach (var row in source)
            {
                foreach (var keySelector in keySelectors)
                {
                    var key = JavaWrapped.Of(comparer, JavaValues.From(keySelector(row)));
                    var accumulator = accumulators.get(key) ?? accumulatorInitializer.apply();

                    accumulators.put(key, accumulatorAdder.apply(accumulator, row));
                }
            }

            for (var i = accumulators.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                yield return JavaValues.As<TResult>(resultSelector.apply(JavaWrapped.Unwrap(entry.getKey()), entry.getValue()));
            }
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
        /// and no GROUP BY becomes.
        /// </remarks>
        public static TResult Aggregate<TSource, TResult>(IEnumerable<TSource> source, object seed, Function2 accumulatorAdder, Function1 resultSelector)
        {
            var accumulator = seed;

            foreach (var row in source)
                accumulator = accumulatorAdder.apply(accumulator, row);

            return JavaValues.As<TResult>(resultSelector.apply(accumulator));
        }

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
        /// </remarks>
        public static IEnumerable<TResult> Window<TSource, TKey, TAccumulator, TResult>(
            IEnumerable<TSource> source,
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

            // an exclusion that is not "no other" makes every frame a fresh one, because the same bounds do not
            // mean the same rows once the current row's peers are taken out of them
            var excluding = exclude == null || exclude.name() != nameof(org.apache.calcite.rex.RexWindowExclusion.EXCLUDE_NO_OTHER);

            var frame = new WindowFrame();
            var accumulator = accumulatorInitializer();

            foreach (var rows in Partitions(source, partitionSelector, comparator))
            {
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

                    yield return selector(frame, accumulator);
                }
            }
        }

        /// <summary>
        /// Returns the rows of each partition, in the window's order.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="partitionSelector"></param>
        /// <param name="comparator"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>SortedMultiMap</c> itself, rather than a dictionary standing in for it. It is a runtime class of
        /// Calcite's and not a generated tree, and it is what decides the order the partitions come out in —
        /// a hash map's, which nothing else reproduces. Being feature compatible with
        /// <c>EnumerableConvention</c> means a query with no ORDER BY gives the rows in the same order, so the
        /// map is the one Calcite uses. It also settles the two questions underneath: a null key is a
        /// partition of its own, and <c>arrays</c> sorts with <c>Arrays.sort</c>, which is stable, so rows the
        /// collation does not separate stay in the order they arrived.
        /// </remarks>
        static IEnumerable<object[]> Partitions<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey>? partitionSelector, java.util.Comparator comparator)
        {
            java.util.Iterator iterator;

            if (partitionSelector == null)
            {
                // one partition, which is yielded even when it is empty
                var all = new java.util.ArrayList();
                foreach (var row in source)
                    all.add(row);

                iterator = org.apache.calcite.runtime.SortedMultiMap.singletonArrayIterator(comparator, all);
            }
            else
            {
                var multiMap = new org.apache.calcite.runtime.SortedMultiMap();
                foreach (var row in source)
                    multiMap.putMulti(partitionSelector(row), row);

                iterator = multiMap.arrays(comparator);
            }

            while (iterator.hasNext())
                yield return (object[])iterator.next();
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

        /// <summary>
        /// Returns each row of each sequence a function yields.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector">Yields a linq4j sequence for one row, which is what Calcite builds here.</param>
        /// <returns></returns>
        public static IEnumerable<TResult> SelectMany<TSource, TResult>(IEnumerable<TSource> source, Function1 selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            foreach (var row in source)
                foreach (var item in JavaSequences.FromJava<TResult>((org.apache.calcite.linq4j.Enumerable)selector.apply(row!)))
                    yield return item;
        }

        /// <summary>
        /// Orders rows by a key, then skips and takes.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
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
        /// <para>This sorted the whole input and then skipped and took, which answers the same rows in the
        /// same order and holds all million. The differential tests could not see it, because what they
        /// compare is the answer.</para>
        ///
        /// <para>A <c>SortedDictionary</c> where linq4j has a <c>TreeMap</c>, and the same reason its own
        /// comment gives for choosing one over a heap: it behaves like the plain <c>OrderBy</c> and does
        /// better when there are few distinct keys. The list per key starts as a single element, as
        /// linq4j's does.</para>
        /// </remarks>
        public static IEnumerable<TSource> OrderByWithFetchAndOffset<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, int offset, int fetch)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);

            if (fetch == 0)
                return [];

            return Ordered(source, keySelector, comparator, offset, fetch);
        }


        /// <summary>
        /// Reads the input, keeping only the rows that can reach the output, and yields them in order.
        /// </summary>
        /// <remarks>
        /// linq4j's <c>orderBy(source, keySelector, comparator, offset, fetch)</c>, transcribed. A
        /// <c>java.util.TreeMap</c> because that is what linq4j uses, and the reason its own comment gives:
        /// it behaves like the plain <c>orderBy</c> and does better than a heap where there are few distinct
        /// keys.
        ///
        /// <para>Using Calcite's own structure rather than a <c>SortedDictionary</c> settles three things at
        /// once. It takes the <c>java.util.Comparator</c> this method is handed, unwrapped. It accepts a
        /// null key and routes it through that comparator — which is the whole job of
        /// <c>Functions.nullsComparator</c>, and is how NULLS FIRST and NULLS LAST are expressed;
        /// <c>SortedDictionary</c> rejects a null key before it ever consults the comparer. And
        /// <c>lastKey</c> and <c>headMap</c> are the operations the algorithm is written in terms of, in
        /// O(log n), where <c>SortedDictionary</c> offers neither.</para>
        ///
        /// <para>One deliberate difference: linq4j stores a one-row group as a <c>Collections.singletonList</c>
        /// and swaps in an <c>ArrayList</c> when a second row arrives. Ours is always a
        /// <see cref="List{T}"/>. That is an allocation difference, not a logical one.</para>
        /// </remarks>
        static IEnumerable<TSource> Ordered<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, int offset, int fetch)
        {
            var map = comparator == null ? new java.util.TreeMap() : new java.util.TreeMap(comparator);
            var size = 0L;
            var needed = fetch + (long)offset;

            // read the input into a tree map
            foreach (var row in source)
            {
                var key = (object?)keySelector(row);

                if (needed >= 0 && size >= needed)
                {
                    // the current row will never appear in the output, so just skip it
                    var lastKey = map.lastKey();
                    if (Compare(comparator, key, lastKey) >= 0)
                        continue;

                    // remove last entry from tree map, so that we keep at most 'needed' rows
                    var last = (List<TSource>)map.get(lastKey);
                    if (last.Count == 1)
                        map.remove(lastKey);
                    else
                        last.RemoveAt(last.Count - 1);

                    size--;
                }

                // add the current element to the map
                if (map.get(key) is List<TSource> held)
                    held.Add(row);
                else
                    map.put(key, new List<TSource> { row });

                size++;
            }

            // skip the first 'offset' rows by deleting them from the map
            if (offset > 0)
            {
                // search the key up to (but excluding) which we have to remove entries from the map
                var skipped = 0;
                var found = false;
                object? until = null;

                for (var i = map.entrySet().iterator(); i.hasNext();)
                {
                    var entry = (java.util.Map.Entry)i.next();
                    var rows = (List<TSource>)entry.getValue();
                    skipped += rows.Count;

                    if (skipped > offset)
                    {
                        // we might need to remove entries from the list
                        var keep = skipped - offset;
                        if (keep < rows.Count)
                            rows.RemoveRange(0, rows.Count - keep);

                        until = entry.getKey();
                        found = true;
                        break;
                    }
                }

                // the offset is bigger than the number of rows in the map
                if (found == false)
                    yield break;

                map.headMap(until, false).clear();
            }

            for (var i = map.values().iterator(); i.hasNext();)
                foreach (var row in (List<TSource>)i.next())
                    yield return row;
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

        /// <summary>
        /// Reads a Java list as a .NET sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// An adapter boundary, so it converts rather than casts, for the reason
        /// <see cref="JavaSequences.FromJava"/> gives.
        /// </remarks>
        public static IEnumerable<TSource> FromJavaList<TSource>(java.util.List source)
        {
            ArgumentNullException.ThrowIfNull(source);

            for (var i = source.iterator(); i.hasNext();)
                yield return JavaValues.As<TSource>(i.next());
        }

        /// <summary>
        /// Reads every row into a list.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// A java.util.List, because this is a value in a row and the reader of that row is Calcite's.
        /// </remarks>
        public static java.util.List ToJavaList<TSource>(IEnumerable<TSource> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            var list = new java.util.ArrayList();
            foreach (var row in source)
                list.add(row);

            return list;
        }

        /// <summary>
        /// Reads every row into a map, keeping the order the keys were seen in.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="valueSelector"></param>
        /// <returns></returns>
        public static java.util.Map ToJavaMap<TSource>(IEnumerable<TSource> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector)
        {
            ArgumentNullException.ThrowIfNull(source);

            var map = new java.util.LinkedHashMap();
            foreach (var row in source)
                map.put(keySelector(row), valueSelector(row));

            return map;
        }

        /// <summary>
        /// Passes every row through, and leaves them in a collection behind it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="collection"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.lazyCollectionSpool</c>. Rows are buffered while they are
        /// yielded and the collection is replaced once the input is exhausted, so it holds one round rather
        /// than everything seen so far. That is what makes the next round of a recursive query read a delta.
        /// </remarks>
        public static IEnumerable<TSource> LazyCollectionSpool<TSource>(java.util.Collection collection, IEnumerable<TSource> input)
        {
            ArgumentNullException.ThrowIfNull(collection);
            ArgumentNullException.ThrowIfNull(input);

            var buffer = new List<TSource>();

            foreach (var row in input)
            {
                buffer.Add(row);
                yield return row;
            }

            // the collection belongs to the table, and what reads it back is Java — the interpreter, for a
            // transient table neither convention's scan will touch. So this is a boundary and it converts
            collection.clear();
            foreach (var row in buffer)
                collection.add(JavaValues.From(row));
        }

        /// <summary>
        /// Returns the seed, then the iterative part over and over until it yields nothing.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="seed"></param>
        /// <param name="iteration"></param>
        /// <param name="iterationLimit">A negative value for no limit.</param>
        /// <param name="all">Whether a row already returned is returned again.</param>
        /// <param name="comparer"></param>
        /// <param name="cleanUp">Run once the sequence is finished with, or null.</param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.repeatUnion</c>, which is what WITH RECURSIVE becomes. The
        /// iterative part is enumerated afresh each round, reading what the spool beneath it left behind.
        /// </remarks>
        public static IEnumerable<TSource> RepeatUnion<TSource>(IEnumerable<TSource> seed, IEnumerable<TSource> iteration, int iterationLimit, bool all, EqualityComparer? comparer, Action? cleanUp)
        {
            ArgumentNullException.ThrowIfNull(seed);
            ArgumentNullException.ThrowIfNull(iteration);

            try
            {
                var processed = all ? null : new HashSet<TSource>(JavaEqualityComparer<TSource>.Of(comparer));

                foreach (var row in seed)
                    if (processed == null || processed.Add(row))
                        yield return row;

                for (int i = 0; iterationLimit < 0 || i < iterationLimit; i++)
                {
                    // a round that returned no row this sequence had not already returned is the end of it,
                    // and not a round that read no rows. Calcite's own enumerator says the same thing by
                    // setting `current` only where `checkValue` passed and stopping where it is still DUMMY.
                    // Counting what was read instead is an infinite loop for every UNION rather than UNION
                    // ALL: the iterative part re-reads the whole spool each round and so never runs dry.
                    var any = false;

                    foreach (var row in iteration)
                    {
                        if (processed == null || processed.Add(row))
                        {
                            any = true;
                            yield return row;
                        }
                    }

                    if (any == false)
                        yield break;
                }
            }
            finally
            {
                cleanUp?.Invoke();
            }
        }

        /// <summary>
        /// Returns the rows of an array.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a VALUES clause becomes, which Calcite spells <c>Linq4j.asEnumerable</c>.
        /// </remarks>
        public static IEnumerable<TSource> AsEnumerable<TSource>(TSource[] source)
        {
            return source;
        }

        /// <summary>
        /// Returns a sequence of one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="element"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Singleton<TSource>(TSource element)
        {
            yield return element;
        }

        /// <summary>
        /// Returns an empty sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <returns></returns>
        public static IEnumerable<TSource> Empty<TSource>()
        {
            return [];
        }

    }

}
