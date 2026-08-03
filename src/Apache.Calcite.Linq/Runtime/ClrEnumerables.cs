using System;
using System.Collections.Generic;
using System.Linq;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Linq.Runtime
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
    public static class ClrEnumerables
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
        public static IEnumerable<object> Slice0(IEnumerable<object[]> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            foreach (var row in source)
                yield return row[0];
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
            return source.Union(other, JavaEqualityComparer<TSource>.Of(comparer));
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
            if (all == false)
                return source.Intersect(other, JavaEqualityComparer<TSource>.Of(comparer));

            return IntersectAll(source, other, JavaEqualityComparer<TSource>.Of(comparer));
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
            if (all == false)
                return source.Except(other, JavaEqualityComparer<TSource>.Of(comparer));

            return ExceptAll(source, other, JavaEqualityComparer<TSource>.Of(comparer));
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
            return source.Distinct(JavaEqualityComparer<TSource>.Of(comparer));
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
            var equality = JavaEqualityComparer<TKey>.Of(comparer);
            var lookup = new Dictionary<TKey, List<TInner>>(equality);
            var matched = generateNullsOnLeft ? new HashSet<TKey>(equality) : null;

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
                {
                    foreach (var other in bucket)
                    {
                        if (predicate != null && predicate(row, other) == false)
                            continue;

                        any = true;
                        matched?.Add(key);
                        yield return resultSelector(row, other);
                    }
                }

                if (any == false && generateNullsOnRight)
                    yield return resultSelector(row, default!);
            }

            if (matched == null)
                yield break;

            foreach (var pair in lookup)
                if (matched.Contains(pair.Key) == false)
                    foreach (var other in pair.Value)
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
            var accumulators = new Dictionary<TKey, object>(JavaEqualityComparer<TKey>.Of(comparer));
            var order = new List<TKey>();

            foreach (var row in source)
            {
                var key = keySelector(row);

                if (accumulators.TryGetValue(key, out var accumulator) == false)
                {
                    accumulator = accumulatorInitializer.apply();
                    order.Add(key);
                }

                accumulators[key] = accumulatorAdder.apply(accumulator, row);
            }

            foreach (var key in order)
                yield return (TResult)resultSelector.apply(key, accumulators[key]);
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

            return (TResult)resultSelector.apply(accumulator);
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
        /// The counterpart of <c>EnumerableDefaults.orderBy</c> with a fetch and an offset, which is a sort
        /// carrying a limit rather than a sort followed by one.
        /// </remarks>
        public static IEnumerable<TSource> OrderByWithFetchAndOffset<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, int offset, int fetch)
        {
            var ordered = OrderBy(source, keySelector, comparator);

            if (offset > 0)
                ordered = ordered.Skip(offset);

            if (fetch != int.MaxValue)
                ordered = ordered.Take(fetch);

            return ordered;
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
