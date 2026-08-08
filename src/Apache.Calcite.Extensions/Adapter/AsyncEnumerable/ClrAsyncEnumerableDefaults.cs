using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite.linq4j.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// The sequence operators a plan of the <see cref="ClrAsyncEnumerableConvention"/> calling convention is
    /// built from.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableDefaults"/>, operator for operator, over
    /// <see cref="IAsyncEnumerable{T}"/>. Each body is that one with <c>foreach</c> made
    /// <c>await foreach</c>; nothing about what an operator means changes, and the row-level delegates each
    /// takes are the same synchronous <see cref="Func{T, TResult}"/>, because they are the per-row work
    /// Calcite's generators write and there is nothing in one to await.
    ///
    /// <para><b>Cancellation is carried by the language rather than by the plan.</b> Every operator is an
    /// <c>async IAsyncEnumerable</c> iterator declaring
    /// <c>[EnumeratorCancellation] CancellationToken</c> and consuming its input through
    /// <c>WithCancellation</c>, so a token entering at the root's
    /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> reaches the leaf without appearing anywhere in
    /// the expression tree. The tree passes nothing.</para>
    ///
    /// <para><b>Several of these are iterators where their counterparts delegate to
    /// <see cref="System.Linq.Enumerable"/>.</b> <c>System.Linq.AsyncEnumerable</c> arrived in .NET 10 and
    /// this targets net8.0 — measured, absent — so <see cref="Where"/>, <see cref="Select"/>,
    /// <see cref="Skip"/> and <see cref="Take"/> are written out. They are the only operators here that are
    /// not a transcription of a body in the synchronous file.</para>
    /// </remarks>
    static class ClrAsyncEnumerableDefaults
    {

        /// <summary>
        /// Returns the first field of each row.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.Slice0"/>. A one column result is the value, not a one element
        /// row.
        /// </remarks>
        public static async IAsyncEnumerable<TRow> Slice0<TRow>(IAsyncEnumerable<object[]> source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            // the row came from a table and its fields are still Java's, so the field taken out of it is
            // converted rather than cast
            await foreach (var row in source.WithCancellation(cancellationToken))
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
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.Calc"/>, which is what Calcite generates an anonymous
        /// <c>Enumerator</c> for. Both halves are still one pass over the input.
        /// </remarks>
        public static async IAsyncEnumerable<TResult> Calc<TSource, TResult>(IAsyncEnumerable<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            await foreach (var row in source.WithCancellation(cancellationToken))
                if (predicate == null || predicate(row))
                    yield return selector(row);
        }

        /// <summary>
        /// Returns the rows that satisfy a condition.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="predicate"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// Written out rather than delegated, for the reason the class remarks give.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> Where<TSource>(IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(predicate);

            await foreach (var row in source.WithCancellation(cancellationToken))
                if (predicate(row))
                    yield return row;
        }

        /// <summary>
        /// Projects each row into a new form.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// Written out rather than delegated, for the reason the class remarks give.
        /// </remarks>
        public static async IAsyncEnumerable<TResult> Select<TSource, TResult>(IAsyncEnumerable<TSource> source, Func<TSource, TResult> selector, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            await foreach (var row in source.WithCancellation(cancellationToken))
                yield return selector(row);
        }

        /// <summary>
        /// Orders rows by a key.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="comparator">Comparison of two keys, or null to compare them naturally.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.OrderBy"/>. A sort has to read its whole input before it can
        /// yield anything, so this drains the source and then hands the buffer to the synchronous operator:
        /// the awaiting is done by the time any comparing starts, and the comparison itself is the same code
        /// the other convention runs. Buffering is what a sort does in both.
        ///
        /// <para>The comparator is Java's, because that is what <c>PhysType.generateCollationKey</c>
        /// yields.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TSource> OrderBy<TSource, TKey>(IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);

            foreach (var row in ClrEnumerableDefaults.OrderBy(await Buffer(source, cancellationToken).ConfigureAwait(false), keySelector, comparator))
                yield return row;
        }

        /// <summary>
        /// Skips a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// Written out rather than delegated, for the reason the class remarks give.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> Skip<TSource>(IAsyncEnumerable<TSource> source, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            var skipped = 0;

            await foreach (var row in source.WithCancellation(cancellationToken))
                if (skipped++ >= count)
                    yield return row;
        }

        /// <summary>
        /// Takes a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.take</c> is <c>takeWhile</c> over <c>n &lt; count</c>, and
        /// <c>TakeWhileLongEnumerator.moveNext</c> is <c>enumerator.moveNext() &amp;&amp; predicate(current,
        /// ++n)</c> -- it has to draw a row before it can test it. Two consequences, and both are visible.
        ///
        /// <para>A satisfied fetch has drawn one row more than it returned. Over a table that is a row the
        /// scan actually read.</para>
        ///
        /// <para>A count of zero still opens the input and still draws that row. Opening is not free: it is
        /// what runs a lazy collection spool's write-back and a repeat union's clean-up. Returning an empty
        /// sequence without touching the input skips both.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TSource> Take<TSource>(IAsyncEnumerable<TSource> source, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            await using var enumerator = source.GetAsyncEnumerator(cancellationToken);

            var n = -1;

            while (await enumerator.MoveNextAsync().ConfigureAwait(false) && ++n < count)
                yield return enumerator.Current;
        }

        /// <summary>
        /// Orders rows by a key, skipping and taking as it goes.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="comparator"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.OrderByWithFetchAndOffset"/>: a sort carrying a limit rather than
        /// a sort followed by one.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> OrderByWithFetchAndOffset<TSource, TKey>(IAsyncEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator, int offset, int fetch, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(keySelector);

            foreach (var row in ClrEnumerableDefaults.OrderByWithFetchAndOffset(await Buffer(source, cancellationToken).ConfigureAwait(false), keySelector, comparator, offset, fetch))
                yield return row;
        }

        /// <summary>
        /// Returns the rows of an array.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.AsEnumerable"/>, which is what a VALUES clause becomes. The rows
        /// are a constant of the plan, so nothing here suspends; the sequence is asynchronous because
        /// everything the convention composes is, not because there is anything to wait for.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> AsEnumerable<TSource>(TSource[] source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            foreach (var row in source)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
            }

            await System.Threading.Tasks.Task.CompletedTask;
        }

        /// <summary>
        /// Returns a sequence of one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="element"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.Singleton"/>.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> Singleton<TSource>(TSource element, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return element;

            await System.Threading.Tasks.Task.CompletedTask;
        }

        /// <summary>
        /// Returns an empty sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.Empty"/>.
        /// </remarks>
#pragma warning disable CS1998 // async method lacks await: an empty iterator has nothing to await
        public static async IAsyncEnumerable<TSource> Empty<TSource>([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield break;
        }
#pragma warning restore CS1998

        /// <summary>
        /// Reads a Java list as a .NET sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.FromJavaList"/>. An adapter boundary, so it converts rather than
        /// casts. The list is a value already in hand — a field of a row — so nothing suspends.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> FromJavaList<TSource>(java.util.List source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            for (var i = source.iterator(); i.hasNext();)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return JavaValues.As<TSource>(i.next());
            }

            await System.Threading.Tasks.Task.CompletedTask;
        }


        /// <summary>
        /// Returns the rows of both sequences, keeping duplicates.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <returns></returns>
        public static async IAsyncEnumerable<TSource> Concat<TSource>(IAsyncEnumerable<TSource> source, IAsyncEnumerable<TSource> other,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            await foreach (var row in source.WithCancellation(cancellationToken))
                yield return row;

            await foreach (var row in other.WithCancellation(cancellationToken))
                yield return row;
        }


        /// <summary>
        /// Returns the distinct rows of both sequences.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static async IAsyncEnumerable<TSource> Union<TSource>(IAsyncEnumerable<TSource> source, IAsyncEnumerable<TSource> other, EqualityComparer? comparer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            // a java.util.HashSet, and not because the CLR has nothing to hold rows in: what a set operator
            // yields a row in is the order of the collection it held them in, and Calcite's is this one. See
            // JavaHashingTests for why that order is the same in every process.
            var set = new java.util.HashSet();
            await foreach (var row in (source).WithCancellation(cancellationToken))
                set.add(JavaWrapped.Of(comparer, JavaValues.From(row)));
            await foreach (var row in (other).WithCancellation(cancellationToken))
                set.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            foreach (var row in Unwrap<TSource>(set))
                yield return row;
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
        public static async IAsyncEnumerable<TSource> Intersect<TSource>(IAsyncEnumerable<TSource> source, IAsyncEnumerable<TSource> other, EqualityComparer? comparer, bool all,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            // ALL keeps a row once per pairing, so the collection counts rather than merely holding
            var set1 = Collection(all);
            await foreach (var row in (other).WithCancellation(cancellationToken))
                set1.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            var result = Collection(all);
            await foreach (var row in (source).WithCancellation(cancellationToken))
                if (set1.remove(JavaWrapped.Of(comparer, JavaValues.From(row))))
                    result.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            foreach (var row in Unwrap<TSource>(result))
                yield return row;
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
        /// Returns the rows of the first sequence that are not in the second.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="other"></param>
        /// <param name="comparer"></param>
        /// <param name="all">Whether a row is removed once per appearance in the second rather than entirely.</param>
        /// <returns></returns>
        public static async IAsyncEnumerable<TSource> Except<TSource>(IAsyncEnumerable<TSource> source, IAsyncEnumerable<TSource> other, EqualityComparer? comparer, bool all,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            var collection = Collection(all);
            await foreach (var row in (source).WithCancellation(cancellationToken))
                collection.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            await foreach (var row in (other).WithCancellation(cancellationToken))
                collection.remove(JavaWrapped.Of(comparer, JavaValues.From(row)));

            foreach (var row in Unwrap<TSource>(collection))
                yield return row;
        }


        /// <summary>
        /// Returns the distinct rows of a sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="comparer"></param>
        /// <returns></returns>
        public static async IAsyncEnumerable<TSource> Distinct<TSource>(IAsyncEnumerable<TSource> source, EqualityComparer? comparer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            var set = new java.util.HashSet();
            await foreach (var row in (source).WithCancellation(cancellationToken))
                set.add(JavaWrapped.Of(comparer, JavaValues.From(row)));

            foreach (var row in Unwrap<TSource>(set))
                yield return row;
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
        public static async IAsyncEnumerable<TResult> LeftMarkHashJoin<TSource, TInner, TKey, TNsKey, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeyNullAwareSelector,
            Func<TInner, TKey> innerKeyNullAwareSelector,
            Func<TSource, TNsKey>? outerNullSafeKeySelector,
            Func<TInner, TNsKey>? innerNullSafeKeySelector,
            bool atMostOneNotNullSafeKey,
            Func<TSource, java.lang.Boolean, TResult> resultSelector,
            EqualityComparer? comparer,
            EqualityComparer? nullSafeComparer,
            Func<TSource, TInner, java.lang.Boolean>? nonEquiPredicate,
            Func<TSource, TInner, java.lang.Boolean> equiPredicate,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);

            // the build side: one bucket per key, plus the set of null-safe keys seen. A null key goes into
            // the map under null rather than being dropped, because the rows behind it are what say UNKNOWN
            var lookup = new java.util.HashMap();
            var nullSafeKeys = new java.util.HashSet();

            await foreach (var row in (inner).WithCancellation(cancellationToken))
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

            await foreach (var row in (outer).WithCancellation(cancellationToken))
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
                        foreach (var bucket in (Buckets<TInner>(lookup)))
                        {
                            foreach (var other in (bucket))
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
                        foreach (var other in (matches))
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
                        foreach (var other in (nulls))
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
        public static async IAsyncEnumerable<TResult> HashJoin<TSource, TInner, TKey, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool>? predicate,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            var rows = predicate == null
                ? HashEquiJoin(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, cancellationToken)
                : HashJoinWithPredicate(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate, cancellationToken);

            await foreach (var row in rows.WithCancellation(cancellationToken))
                yield return row;
        }


        /// <summary>
        /// Joins two sequences on a key alone.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableDefaults.hashEquiJoin_</c>. What is left over at the end is a
        /// <em>key</em> no outer row carried, and every build row under it comes out together.
        /// </remarks>
        static async IAsyncEnumerable<TResult> HashEquiJoin<TSource, TInner, TKey, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // the lookup is a java.util.HashMap, as linq4j's toLookup builds one: a right or a full join ends
            // with the rows of the right input that matched nothing, and the order those come out in is this
            // map's. See JavaHashingTests for why that order is the same in every process.
            var lookup = new java.util.HashMap();

            await foreach (var row in inner.WithCancellation(cancellationToken))
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

            await foreach (var row in outer.WithCancellation(cancellationToken))
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

            // the set is walked and each key looked back up, which is what linq4j does and is not the same as
            // walking the map and filtering by the set. A HashSet copied from a key set does not have the
            // map's iteration order: HashSet(Collection) sizes its table as
            // tableSizeFor(max((int) (n / 0.75f) + 1, 16)), while a map grown by insertion holds the smallest
            // power of two at or above 16 that still leaves n <= 0.75 * cap. The two disagree exactly where
            // n = 0.75 * 2^k — 12, 24, 48 — and these rows have no ORDER BY over them.
            for (var i = unmatched.iterator(); i.hasNext();)
            {
                var key = i.next();

                foreach (var other in (List<TInner>)lookup.get(key))
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
        static async IAsyncEnumerable<TResult> HashJoinWithPredicate<TSource, TInner, TKey, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            EqualityComparer? comparer,
            bool generateNullsOnLeft,
            bool generateNullsOnRight,
            Func<TSource, TInner, bool> predicate,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // read once, because a right or a full join walks it twice
            IReadOnlyList<TInner> innerToLookUp = await Buffer(inner, cancellationToken).ConfigureAwait(false);

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

            await foreach (var row in outer.WithCancellation(cancellationToken))
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
        /// <remarks>
        /// <c>EnumerableDefaults.semiJoin</c>, which is a dispatch and not an implementation: with no
        /// predicate it is <c>semiEquiJoin_</c>, which holds the distinct inner <em>keys</em>, and with one it
        /// is <c>semiJoinWithPredicate_</c>, which holds a lookup of inner <em>rows</em> because the predicate
        /// has to see them. Those are the two methods below, and they are not the same algorithm.
        /// </remarks>
        public static IAsyncEnumerable<TSource> SemiJoin<TSource, TInner, TKey>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool>? predicate,
            CancellationToken cancellationToken = default)
        {
            return predicate == null
                ? SemiEquiJoin(outer, inner, outerKeySelector, innerKeySelector, comparer, anti, cancellationToken)
                : SemiJoinWithPredicate(outer, inner, outerKeySelector, innerKeySelector, comparer, anti, predicate, cancellationToken);
        }

        /// <summary>
        /// Returns the rows of the first sequence whose key is, or is not, one of the second's.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.SemiEquiJoin"/> over asynchronous sequences:
        /// <c>EnumerableDefaults.semiEquiJoin_</c>, the distinct keys rather than the rows, the two sets that
        /// keep the comparer off the membership test, and CALCITE-2909's memoization.
        /// </remarks>
        static async IAsyncEnumerable<TSource> SemiEquiJoin<TSource, TInner, TKey>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            java.util.HashSet? keys = null;

            await foreach (var row in outer.WithCancellation(cancellationToken))
            {
                if (keys == null)
                {
                    var distinct = new java.util.HashSet();
                    keys = new java.util.HashSet();

                    await foreach (var innerRow in inner.WithCancellation(cancellationToken))
                    {
                        var innerKey = JavaValues.From(innerKeySelector(innerRow));

                        if (distinct.add(JavaWrapped.Of(comparer, innerKey)))
                            keys.add(innerKey);
                    }
                }

                var key = outerKeySelector(row);
                var found = key != null && keys.contains(JavaValues.From(key));

                if (found != anti)
                    yield return row;
            }
        }

        /// <summary>
        /// Returns the rows of the first sequence that have, or have not, a match in the second under a
        /// condition the key does not express.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.SemiJoinWithPredicate"/> over asynchronous sequences:
        /// <c>EnumerableDefaults.semiJoinWithPredicate_</c>, which holds the rows and does let the comparer
        /// reach the lookup.
        /// </remarks>
        static async IAsyncEnumerable<TSource> SemiJoinWithPredicate<TSource, TInner, TKey>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            EqualityComparer? comparer,
            bool anti,
            Func<TSource, TInner, bool> predicate,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            java.util.HashMap? lookup = null;

            await foreach (var row in outer.WithCancellation(cancellationToken))
            {
                if (lookup == null)
                {
                    lookup = new java.util.HashMap();

                    await foreach (var innerRow in inner.WithCancellation(cancellationToken))
                    {
                        var innerKey = JavaWrapped.Of(comparer, JavaValues.From(innerKeySelector(innerRow)));

                        if (lookup.get(innerKey) is not List<TInner> bucket)
                            lookup.put(innerKey, bucket = []);

                        bucket.Add(innerRow);
                    }
                }

                var key = outerKeySelector(row);
                var found = false;

                if (key != null && lookup.get(JavaWrapped.Of(comparer, JavaValues.From(key))) is List<TInner> matches)
                {
                    foreach (var other in matches)
                    {
                        if (predicate(row, other))
                        {
                            found = true;
                            break;
                        }
                    }
                }

                if (found != anti)
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
        ///
        /// <para><b>Forced by the CLR:</b> Calcite builds all three indexes where <c>asofJoin</c> is called
        /// and only then returns the enumerable that walks them. A method returning an
        /// <see cref="IAsyncEnumerable{T}"/> cannot await before it returns, so both scans happen on the first
        /// <c>MoveNextAsync</c>. Every index is still complete before the first row is yielded. Same
        /// constraint as <see cref="NestedLoopJoinAsList"/>.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TResult> AsofJoin<TSource, TInner, TKey, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> matchComparator,
            java.util.Comparator timestampComparator,
            bool emitNullsOnRight,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            var leftIndex = new java.util.HashMap();
            var rightIndex = new java.util.HashMap();
            var outerWithNullKeys = new List<TSource>();

            await foreach (var row in (outer).WithCancellation(cancellationToken))
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

            await foreach (var row in (inner).WithCancellation(cancellationToken))
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

            foreach (var row in (outerWithNullKeys))
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
        public static async IAsyncEnumerable<TResult> SortedGroupBy<TSource, TKey, TResult>(
            IAsyncEnumerable<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            java.util.Comparator comparator,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            object? accumulator = null;
            object? previousKey = null;
            var any = false;

            await foreach (var row in (source).WithCancellation(cancellationToken))
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
        public static async IAsyncEnumerable<TSource> MergeUnion<TSource, TKey>(
            java.util.List sources,
            Func<TSource, TKey> sortKeySelector,
            java.util.Comparator sortComparator,
            bool all,
            EqualityComparer? comparer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            var inputs = new IAsyncEnumerator<TSource>[sources.size()];
            for (int i = 0; i < inputs.Length; i++)
                inputs[i] = ((IAsyncEnumerable<TSource>)sources.get(i)).GetAsyncEnumerator(cancellationToken);

            var current = new TSource[inputs.Length];
            var finished = new bool[inputs.Length];
            var active = inputs.Length;

            // only where duplicates are dropped, and only ever holding the rows of one key
            var processed = all ? null : new java.util.HashSet();
            object? keyInProcessed = null;

            async System.Threading.Tasks.ValueTask Move(int i)
            {
                if (await inputs[i].MoveNextAsync().ConfigureAwait(false) == false)
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
                    await Move(i);

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
                    await Move(candidate);

                    if (emit)
                        yield return value;
                }
            }
            finally
            {
                foreach (var input in inputs)
                    await input.DisposeAsync().ConfigureAwait(false);
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
        public static async IAsyncEnumerable<TResult> MergeJoin<TSource, TInner, TKey, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TKey> outerKeySelector,
            Func<TInner, TKey> innerKeySelector,
            Func<TSource, TInner, bool>? predicate,
            Func<TSource, TInner, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            java.util.Comparator? comparator,
            EqualityComparer? comparer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
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

            await using var leftEnumerator = outer.GetAsyncEnumerator(cancellationToken);
            await using var rightEnumerator = inner.GetAsyncEnumerator(cancellationToken);

            // the left enumerator advanced, and onto a row whose key is not null — a LEFT join reads its
            // left input to the end whatever the keys are, because every row of it is a result
            async ValueTask<bool> LeftMoveNext() => await leftEnumerator.MoveNextAsync().ConfigureAwait(false) && (isLeft || outerKeySelector(leftEnumerator.Current) != null);
            async ValueTask<bool> RightMoveNext() => await rightEnumerator.MoveNextAsync().ConfigureAwait(false) && innerKeySelector(rightEnumerator.Current) != null;

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
            async ValueTask<bool> AdvanceLeft(TSource left, TKey leftKey)
            {
                lefts.Clear();
                lefts.Add(left);

                while (await leftEnumerator.MoveNextAsync().ConfigureAwait(false))
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

            async ValueTask<bool> AdvanceRight(TInner right, TKey rightKey)
            {
                rights.Clear();
                rights.Add(right);

                while (await rightEnumerator.MoveNextAsync().ConfigureAwait(false))
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
            async ValueTask<bool> Advance()
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
                                if (await AdvanceLeft(left, leftKey) == false)
                                    done = true;

                                results = Cartesian(lefts, [default(TInner)!], resultSelector);
                                return true;
                            }

                            if (await leftEnumerator.MoveNextAsync().ConfigureAwait(false) == false)
                            {
                                done = true;
                                return false;
                            }

                            left = leftEnumerator.Current;
                            leftKey = outerKeySelector(left);
                        }
                        else
                        {
                            if (await rightEnumerator.MoveNextAsync().ConfigureAwait(false) == false)
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

                    if (await AdvanceLeft(left, leftKey) == false)
                        done = true;

                    if (await AdvanceRight(right, rightKey) == false)
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
                        results = ClrEnumerableDefaults.NestedLoopJoin([.. lefts], [.. rights], resultSelector, predicate, joinType);
                    }

                    return true;
                }
            }

            if (isLeftOrAnti)
            {
                if (await LeftMoveNext() == false)
                    done = true;
                else if (await RightMoveNext() == false)
                    remainingLeft = true;
                else if (await Advance() == false)
                    done = true;
            }
            else if (await LeftMoveNext() == false || await RightMoveNext() == false || await Advance() == false)
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

                    if (await LeftMoveNext() == false)
                    {
                        remainingLeft = false;
                        done = true;
                    }

                    continue;
                }

                if (done)
                    yield break;

                if (await Advance() == false)
                    yield break;
            }
        }


        /// <summary>
        /// Returns every pairing of two sequences, in order.
        /// </summary>
        /// <remarks>
        /// <c>CartesianProductJoinEnumerator</c>, which extends linq4j's <c>CartesianProductEnumerator</c> and
        /// holds nothing: it advances the last enumerator first and only falls back to the one before it when
        /// that runs out, which is this nesting. Building the pairings into a list instead made a merge join
        /// pay for the whole of a key run before it could yield the first row of it, and multiplied the two
        /// counts as <see cref="int"/> to size the list -- an overflow at about 2^31 pairs, on the one path
        /// where a run of equal keys on both sides is exactly what produces them.
        ///
        /// <para>Being lazy couples this to the caller: the two lists are the merge join's own buffers, reused
        /// and cleared per key run, so the pairings must be drained before the join advances. They are -- the
        /// driving loop yields all of <c>results</c> before it calls <c>Advance</c>. Calcite has the same
        /// coupling, <c>Linq4j.enumerator(lefts)</c> being a live view of the list it goes on clearing.</para>
        /// </remarks>
        static IEnumerable<TResult> Cartesian<TSource, TInner, TResult>(IReadOnlyList<TSource> outer, IReadOnlyList<TInner> inner, Func<TSource, TInner, TResult> resultSelector)
        {
            foreach (var left in outer)
                foreach (var right in inner)
                    yield return resultSelector(left, right);
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
        public static async IAsyncEnumerable<TResult> CorrelateBatchJoin<TSource, TInner, TResult>(
            org.apache.calcite.linq4j.JoinType joinType,
            IAsyncEnumerable<TSource> outer,
            Func<java.util.List, IAsyncEnumerable<TInner>> inner,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            int batchSize,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            var name = joinType.name();
            var isSemi = name == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
            var isAnti = name == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
            var isLeft = name == nameof(org.apache.calcite.linq4j.JoinType.LEFT);

            var batch = new List<TSource>(batchSize);
            var rows = new List<TInner>();

            await using var enumerator = outer.GetAsyncEnumerator(cancellationToken);

            // the right rows of the batch being read, held across the batch's first left row because that is
            // the only one that pulls from it
            IAsyncEnumerator<TInner>? source = null;

            try
            {
                while (true)
                {
                    batch.Clear();
                    while (batch.Count < batchSize && await enumerator.MoveNextAsync().ConfigureAwait(false))
                        batch.Add(enumerator.Current);

                    if (batch.Count == 0)
                        yield break;

                    // a short batch is padded by repeating its first row, as Calcite pads it: the condition
                    // is a disjunction, so a row that repeats adds nothing to it
                    var padded = new java.util.ArrayList(batchSize);
                    for (int i = 0; i < batchSize; i++)
                        padded.add(JavaValues.From(batch[i < batch.Count ? i : 0]));

                    rows.Clear();
                    if (source != null)
                        await source.DisposeAsync().ConfigureAwait(false);
                    source = (inner(padded) is IAsyncEnumerable<TInner> sequence ? sequence : Empty<TInner>()).GetAsyncEnumerator(cancellationToken);

                    for (int i = 0; i < batch.Count; i++)
                    {
                        var left = batch[i];
                        var any = false;

                        for (int j = 0; ; j++)
                        {
                            TInner right;

                            if (i == 0)
                            {
                                if (await source.MoveNextAsync().ConfigureAwait(false) == false)
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
                                    while (await source.MoveNextAsync().ConfigureAwait(false))
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
                if (source != null)
                        await source.DisposeAsync().ConfigureAwait(false);
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
        public static async IAsyncEnumerable<TResult> LeftMarkNestedLoopJoin<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TInner, java.lang.Boolean> predicate,
            Func<TSource, java.lang.Boolean, TResult> resultSelector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            ArgumentNullException.ThrowIfNull(inner);

            await foreach (var row in LeftMarkJoin(outer, _ => inner, predicate, resultSelector, cancellationToken).WithCancellation(cancellationToken))
                yield return row;
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
        public static async IAsyncEnumerable<TResult> CorrelateLeftMarkJoin<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            Func<TSource, IAsyncEnumerable<TInner>> inner,
            Func<TSource, TInner, java.lang.Boolean> predicate,
            Func<TSource, java.lang.Boolean, TResult> resultSelector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            await foreach (var row in LeftMarkJoin(outer, inner, predicate, resultSelector, cancellationToken).WithCancellation(cancellationToken))
                yield return row;
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
        static async IAsyncEnumerable<TResult> LeftMarkJoin<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            Func<TSource, IAsyncEnumerable<TInner>> inner,
            Func<TSource, TInner, java.lang.Boolean> predicate,
            Func<TSource, java.lang.Boolean, TResult> resultSelector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(outer);
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(resultSelector);

            await foreach (var left in outer.WithCancellation(cancellationToken))
            {
                var marker = java.lang.Boolean.FALSE;
                var rows = inner(left);

                if (rows != null)
                {
                    await foreach (var right in rows.WithCancellation(cancellationToken))
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
        /// <see cref="ClrEnumerableDefaults.NestedLoopJoin"/> over asynchronous sequences, and the same
        /// dispatch <c>EnumerableDefaults.nestedLoopJoin</c> makes: a join type that generates nulls on the
        /// left -- RIGHT and FULL -- goes to <see cref="NestedLoopJoinAsList"/>, which reads the inner into
        /// a list; everything else goes to <see cref="NestedLoopJoinOptimized"/>, which buffers nothing and
        /// re-enumerates the inner for every outer row. The asymmetry is Calcite's.
        ///
        /// <para>The token is an ordinary parameter here rather than an <c>[EnumeratorCancellation]</c> one,
        /// because this method is not the iterator -- the two bodies are, and each carries the attribute. It
        /// is declared at all so that the operator ends in a <see cref="CancellationToken"/>, which is what
        /// <see cref="ClrAsyncBuiltInMethod.Call"/> requires of everything a plan calls.</para>
        /// </remarks>
        public static IAsyncEnumerable<TResult> NestedLoopJoin<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType,
            CancellationToken cancellationToken = default)
        {
            if (joinType.generatesNullsOnLeft() == false)
                return NestedLoopJoinOptimized(outer, inner, resultSelector, predicate, joinType, cancellationToken);

            return NestedLoopJoinAsList(outer, inner, resultSelector, predicate, joinType, cancellationToken);
        }

        /// <summary>
        /// Joins two sequences on a condition, building the whole result before yielding any of it.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.nestedLoopJoinAsList</c>. The inner is read into a list once, and the
        /// unmatched right rows are held in a set keyed on identity -- <c>Sets.newIdentityHashSet()</c> --
        /// so two unmatched rows that are the same object emit once, as they do in Calcite.
        ///
        /// <para><b>Forced by the CLR:</b> Calcite runs this whole join at <em>call</em> time and returns
        /// <c>Linq4j.asEnumerable(result)</c>. A method returning an <see cref="IAsyncEnumerable{T}"/>
        /// cannot await before it returns, so the work happens on the first <c>MoveNextAsync</c> instead.
        /// Every row is still computed before the first one is yielded, which is the property the ordering
        /// depends on.</para>
        ///
        /// <para><b>Also forced:</b> the unmatched rows come out in insertion order. Calcite walks an
        /// <c>IdentityHashMap</c>, whose buckets are keyed on <c>System.identityHashCode</c>; there is no
        /// CLR counterpart to that number, so the order cannot be reproduced. The set of rows, deduplication
        /// included, is Calcite's.</para>
        /// </remarks>
        static async IAsyncEnumerable<TResult> NestedLoopJoinAsList<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var name = joinType.name();
            var generateNullsOnLeft = joinType.generatesNullsOnLeft();
            var generateNullsOnRight = joinType.generatesNullsOnRight();
            var result = new List<TResult>();
            var rightList = await Buffer(inner, cancellationToken).ConfigureAwait(false);
            HashSet<TInner>? rightUnmatched;

            if (generateNullsOnLeft)
            {
                rightUnmatched = new HashSet<TInner>(ClrEnumerableDefaults.IdentityComparer<TInner>.Instance);

                foreach (var right in rightList)
                    rightUnmatched.Add(right);
            }
            else
            {
                rightUnmatched = null;
            }

            await foreach (var left in outer.WithCancellation(cancellationToken))
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
                            rightUnmatched?.Remove(right);

                            // a semi join emits the matched right row, not a null one, and then stops
                            result.Add(resultSelector(left, right));

                            if (name == nameof(org.apache.calcite.linq4j.JoinType.SEMI))
                                break;
                        }
                    }
                }

                if (leftMatchCount == 0 && (generateNullsOnRight || name == nameof(org.apache.calcite.linq4j.JoinType.ANTI)))
                    result.Add(resultSelector(left, default!));
            }

            if (rightUnmatched != null)
                foreach (var right in rightUnmatched)
                    result.Add(resultSelector(default!, right));

            foreach (var row in result)
                yield return row;
        }

        /// <summary>
        /// Joins two sequences on a condition without building the result as a list first.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.nestedLoopJoinOptimized</c>, and the same state machine: state 0 moves the
        /// outer, state 1 moves the inner. The inner is enumerated afresh for every outer row and never read
        /// into a list.
        ///
        /// <para>A join type that is none of the six the switch names -- ASOF, LEFT_ASOF, LEFT_MARK -- falls
        /// to its default, which returns no pair and no unmatched row either, so the whole join is empty.
        /// That is what Calcite does, and it is not treated as INNER.</para>
        ///
        /// <para>The RIGHT/FULL refusal happens when the caller calls rather than when it enumerates, as
        /// Calcite's does, which is why this method is not itself the iterator.</para>
        /// </remarks>
        static IAsyncEnumerable<TResult> NestedLoopJoinOptimized<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            IAsyncEnumerable<TInner> inner,
            Func<TSource, TInner, TResult> resultSelector,
            Func<TSource, TInner, bool> predicate,
            org.apache.calcite.linq4j.JoinType joinType,
            CancellationToken cancellationToken = default)
        {
            var name = joinType.name();

            if (name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL))
                throw new ArgumentException($"JoinType {name} is unsupported");

            return Walk(cancellationToken);

            async IAsyncEnumerable<TResult> Walk([EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                var outerEnumerator = outer.GetAsyncEnumerator(cancellationToken);
                IAsyncEnumerator<TInner>? innerEnumerator = null;
                var outerMatch = false;
                var outerValue = default(TSource)!;
                var innerValue = default(TInner)!;
                var state = 0;

                try
                {
                    while (true)
                    {
                        switch (state)
                        {
                            case 0:
                                if (await outerEnumerator.MoveNextAsync().ConfigureAwait(false) == false)
                                    yield break;

                                outerValue = outerEnumerator.Current;
                                if (innerEnumerator != null)
                                    await innerEnumerator.DisposeAsync().ConfigureAwait(false);
                                innerEnumerator = inner.GetAsyncEnumerator(cancellationToken);
                                outerMatch = false;
                                state = 1;
                                continue;
                            case 1:
                                if (await innerEnumerator!.MoveNextAsync().ConfigureAwait(false))
                                {
                                    innerValue = innerEnumerator.Current;

                                    if (predicate(outerValue, innerValue))
                                    {
                                        outerMatch = true;

                                        switch (name)
                                        {
                                            case nameof(org.apache.calcite.linq4j.JoinType.ANTI):
                                                state = 0;
                                                continue;
                                            case nameof(org.apache.calcite.linq4j.JoinType.SEMI):
                                                state = 0;
                                                // the matched inner row, not a null one -- the fields are
                                                // what Calcite's current() would have read
                                                yield return resultSelector(outerValue, innerValue);
                                                continue;
                                            case nameof(org.apache.calcite.linq4j.JoinType.INNER):
                                            case nameof(org.apache.calcite.linq4j.JoinType.LEFT):
                                                yield return resultSelector(outerValue, innerValue);
                                                continue;
                                            default:
                                                break;
                                        }
                                    }
                                }
                                else
                                {
                                    state = 0;
                                    innerValue = default!;

                                    if (outerMatch == false &&
                                        name is nameof(org.apache.calcite.linq4j.JoinType.LEFT) or nameof(org.apache.calcite.linq4j.JoinType.ANTI))
                                    {
                                        yield return resultSelector(outerValue, innerValue);
                                        continue;
                                    }
                                }

                                break;
                            default:
                                break;
                        }
                    }
                }
                finally
                {
                    await outerEnumerator.DisposeAsync().ConfigureAwait(false);
                    if (innerEnumerator != null)
                        await innerEnumerator.DisposeAsync().ConfigureAwait(false);
                }
            }
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
        ///
        /// <para>Two things it does before any row moves. RIGHT and FULL are refused -- a correlated join has
        /// no right side to drive -- and the refusal happens where the enumerable is built rather than where
        /// it is enumerated, which is why this method is not itself the iterator. And a correlated function
        /// that answers null is read as an empty sequence rather than dereferenced; Calcite writes
        /// <c>Linq4j.emptyEnumerable()</c> for it, and <c>LeftMarkJoin</c> next door already guarded it.</para>
        ///
        /// <para>The null right row a SEMI join emits is Calcite's too, and for a subtler reason than it
        /// looks: its enumerator returns without assigning <c>innerValue</c>, so <c>current()</c> reads
        /// whatever was there. For a join that is SEMI throughout, that is null every time.</para>
        /// </remarks>
        public static IAsyncEnumerable<TResult> CorrelateJoin<TSource, TInner, TResult>(
            IAsyncEnumerable<TSource> outer,
            Func<TSource, IAsyncEnumerable<TInner>> inner,
            Func<TSource, TInner, TResult> resultSelector,
            org.apache.calcite.linq4j.JoinType joinType,
            CancellationToken cancellationToken = default)
        {
            var name = joinType.name();

            if (name is nameof(org.apache.calcite.linq4j.JoinType.RIGHT) or nameof(org.apache.calcite.linq4j.JoinType.FULL))
                throw new ArgumentException($"JoinType {name} is not valid for correlation");

            return Walk(cancellationToken);

            async IAsyncEnumerable<TResult> Walk([EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                var semi = name == nameof(org.apache.calcite.linq4j.JoinType.SEMI);
                var anti = name == nameof(org.apache.calcite.linq4j.JoinType.ANTI);
                var nullsOnRight = name == nameof(org.apache.calcite.linq4j.JoinType.LEFT);

                await foreach (var row in outer.WithCancellation(cancellationToken))
                {
                    var any = false;

                    await foreach (var other in (inner(row) ?? Empty<TInner>(cancellationToken)).WithCancellation(cancellationToken))
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
        /// <para><b>Forced by the CLR:</b> <c>groupBy_</c> drains the input where it is called and returns a
        /// <c>LookupResultEnumerable</c> over a finished map. A method returning an
        /// <see cref="IAsyncEnumerable{T}"/> cannot await before it returns, so the fold happens on the first
        /// <c>MoveNextAsync</c> instead, and a second pass folds again where Calcite replays. Every group is
        /// still complete before the first one is yielded, which is the property the order rests on. Same
        /// constraint as <see cref="NestedLoopJoinAsList"/>.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TResult> GroupBy<TSource, TKey, TResult>(
            IAsyncEnumerable<TSource> source,
            Func<TSource, TKey> keySelector,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            // a java.util.HashMap, because the order the groups come out in is the map's and Calcite's is
            // this one. Holding the insertion order instead gave a different answer to the same GROUP BY.
            var accumulators = new java.util.HashMap();

            await foreach (var row in (source).WithCancellation(cancellationToken))
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
        /// <para><b>Forced by the CLR:</b> <c>groupBy_</c> drains the input where it is called and returns a
        /// <c>LookupResultEnumerable</c> over a finished map. A method returning an
        /// <see cref="IAsyncEnumerable{T}"/> cannot await before it returns, so the fold happens on the first
        /// <c>MoveNextAsync</c> instead, and a second pass folds again where Calcite replays. Every group is
        /// still complete before the first one is yielded, which is the property the order rests on. Same
        /// constraint as <see cref="NestedLoopJoinAsList"/>.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TResult> GroupByMultiple<TSource, TKey, TResult>(
            IAsyncEnumerable<TSource> source,
            Func<TSource, TKey>[] keySelectors,
            Function0 accumulatorInitializer,
            Function2 accumulatorAdder,
            Function2 resultSelector,
            EqualityComparer? comparer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            // a java.util.HashMap, for the reason GroupBy gives: the order the groups come out in is the
            // map's, and Calcite's map is this one
            var accumulators = new java.util.HashMap();

            await foreach (var row in (source).WithCancellation(cancellationToken))
            {
                foreach (var keySelector in (keySelectors))
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
        public static async IAsyncEnumerable<TResult> Window<TSource, TKey, TAccumulator, TResult>(
            IAsyncEnumerable<TSource> source,
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
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
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

            foreach (var rows in Partitions(await Buffer(source, cancellationToken).ConfigureAwait(false), partitionSelector, comparator))
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
        public static async IAsyncEnumerable<TResult> SelectMany<TSource, TResult>(IAsyncEnumerable<TSource> source, Function1 selector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            // the inner sequence is linq4j's, produced for one row by a generator of Calcite's, and it is
            // iterated synchronously on purpose: it is a value already in hand rather than a source, so
            // nothing about reading it can block. Awaiting it would mean an adapter over a Java sequence,
            // which is the async-over-sync this convention exists to refuse.
            await foreach (var row in source.WithCancellation(cancellationToken))
                foreach (var item in JavaSequences.FromJava<TResult>((org.apache.calcite.linq4j.Enumerable)selector.apply(row!)))
                    yield return item;
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
        public static async IAsyncEnumerable<TSource> LazyCollectionSpool<TSource>(java.util.Collection collection, IAsyncEnumerable<TSource> input,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
            ArgumentNullException.ThrowIfNull(collection);
            ArgumentNullException.ThrowIfNull(input);

            var buffer = new List<TSource>();

            await foreach (var row in (input).WithCancellation(cancellationToken))
            {
                buffer.Add(row);
                yield return row;
            }

            // the collection belongs to the table, and what reads it back is Java — the interpreter, for a
            // transient table neither convention's scan will touch. So this is a boundary and it converts
            collection.clear();
            foreach (var row in (buffer))
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
        public static async IAsyncEnumerable<TSource> RepeatUnion<TSource>(IAsyncEnumerable<TSource> seed, IAsyncEnumerable<TSource> iteration, int iterationLimit, bool all, EqualityComparer? comparer, Action? cleanUp,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(seed);
            ArgumentNullException.ThrowIfNull(iteration);

            var processed = all ? null : new HashSet<TSource>(JavaEqualityComparer<TSource>.Of(comparer));

            // Calcite's `current == DUMMY`. Set false wherever `checkValue` passed and Calcite assigned
            // `current`, and back to true wherever Calcite put the sentinel back.
            var currentIsDummy = true;

            // the seed enumerator is held for the whole life of the sequence, as Calcite holds it -- it is
            // closed in `close()` and not when the seed runs out
            var seedEnumerator = seed.GetAsyncEnumerator(cancellationToken);
            IAsyncEnumerator<TSource>? iterativeEnumerator = null;

            try
            {
                while (await seedEnumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    var value = seedEnumerator.Current;

                    if (processed == null || processed.Add(value))
                    {
                        currentIsDummy = false;
                        yield return value;
                    }
                }

                // The sentinel is NOT put back here, and that is Calcite's, not an oversight of the
                // transcription. A seed that emitted a row leaves `current` holding that row, so the first
                // iterative round to produce nothing does not stop the sequence -- it goes round once more,
                // and only the second empty round stops it. A recursive query whose step reads the working
                // table row by row cannot tell, because an empty table gives an empty round either way; one
                // whose step aggregates -- COUNT(*) yields a row over no rows -- can, and does.
                for (var currentIteration = 0; ; currentIteration++)
                {
                    if (iterationLimit >= 0 && currentIteration == iterationLimit)
                        yield break;

                    iterativeEnumerator = iteration.GetAsyncEnumerator(cancellationToken);

                    while (await iterativeEnumerator.MoveNextAsync().ConfigureAwait(false))
                    {
                        var value = iterativeEnumerator.Current;

                        if (processed == null || processed.Add(value))
                        {
                            currentIsDummy = false;
                            yield return value;
                        }
                    }

                    if (currentIsDummy)
                        yield break;

                    currentIsDummy = true;
                    await iterativeEnumerator.DisposeAsync().ConfigureAwait(false);
                    iterativeEnumerator = null;
                }
            }
            finally
            {
                // Calcite's `close()` in its order: the clean-up first, then the two enumerators. An await
                // foreach would have disposed them first, which is the other way round.
                cleanUp?.Invoke();
                await seedEnumerator.DisposeAsync().ConfigureAwait(false);
                if (iterativeEnumerator != null)
                    await iterativeEnumerator.DisposeAsync().ConfigureAwait(false);
            }
        }


        /// <summary>
        /// Folds a whole sequence and yields the one row the fold produces.
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
        /// <c>Singleton(Aggregate(source, …))</c> as one operator, which is a divergence from the
        /// synchronous convention and is forced. There <c>Aggregate</c> returns the row and
        /// <c>Singleton</c> wraps it, composed as two nested calls in the tree; here the fold has to be
        /// awaited and <b>an expression tree cannot await</b>, so the composition cannot be written as a
        /// tree and is written as an operator instead.
        ///
        /// <para>It is also lazier than the pair it replaces. The synchronous <c>Aggregate</c> runs when
        /// <c>Singleton</c> is called, which is when the compiled plan is invoked rather than when it is
        /// enumerated; this folds on the first <c>MoveNextAsync</c>, which is where the work belongs.</para>
        ///
        /// <para>The fold itself does not buffer, as the synchronous one does not.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TResult> SingletonAggregate<TSource, TResult>(
            IAsyncEnumerable<TSource> source,
            object seed,
            Function2 accumulatorAdder,
            Function1 resultSelector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            var accumulator = seed;

            await foreach (var row in source.WithCancellation(cancellationToken))
                accumulator = accumulatorAdder.apply(accumulator, row);

            yield return JavaValues.As<TResult>(resultSelector.apply(accumulator));
        }

        /// <summary>
        /// Reads a whole sequence into a Java list and yields the one row holding it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(ToJavaList(source))</c> as one operator, for the reason
        /// <see cref="SingletonAggregate"/> gives. A <c>java.util.List</c>, because this is a value in a row
        /// and the reader of that row is Calcite's.
        /// </remarks>
        public static async IAsyncEnumerable<java.util.List> SingletonJavaList<TSource>(
            IAsyncEnumerable<TSource> source,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            var list = new java.util.ArrayList();

            await foreach (var row in source.WithCancellation(cancellationToken))
                list.add(row);

            yield return list;
        }

        /// <summary>
        /// Reads a whole sequence into a Java map and yields the one row holding it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="valueSelector"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(ToJavaMap(source, …))</c> as one operator, for the reason
        /// <see cref="SingletonAggregate"/> gives. A <c>LinkedHashMap</c>, so that the order the rows
        /// arrived in is the order the map keeps.
        /// </remarks>
        public static async IAsyncEnumerable<java.util.Map> SingletonJavaMap<TSource>(
            IAsyncEnumerable<TSource> source,
            Func<TSource, object> keySelector,
            Func<TSource, object> valueSelector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            var map = new java.util.LinkedHashMap();

            await foreach (var row in source.WithCancellation(cancellationToken))
                map.put(keySelector(row), valueSelector(row));

            yield return map;
        }

        /// <summary>
        /// Reads a whole sequence into a Java list.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ClrEnumerableDefaults.ToJavaList"/>. Not called from a plan -- an expression tree
        /// cannot await it -- and kept for <see cref="ClrAsyncEnumerableCombine"/>, which needs the lists
        /// before it can combine them and does its awaiting inside an operator of its own.
        /// </remarks>
        public static async System.Threading.Tasks.ValueTask<java.util.List> ToJavaList<TSource>(
            IAsyncEnumerable<TSource> source,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            var list = new java.util.ArrayList();

            await foreach (var row in source.WithCancellation(cancellationToken))
                list.add(row);

            return list;
        }


        /// <summary>
        /// Reads every input into a Java list, combines them, and yields the rows that come back.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="sources"></param>
        /// <param name="combine">What to do with the lists once they are all read, which is
        /// <c>SqlFunctions.combineQueryResults</c>.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// The third of the fused operators, for the reason <see cref="SingletonAggregate"/> gives: the
        /// synchronous node reads each input into a list inside the tree and passes the lists to the combine,
        /// and here each read has to be awaited.
        ///
        /// <para>The combining itself is not this operator's business and arrives as a delegate, so that
        /// which function Calcite combines with stays the node's decision, as it is in the other
        /// convention.</para>
        /// </remarks>
        public static async IAsyncEnumerable<TResult> CombineQueryResults<TResult>(
            IAsyncEnumerable<java.util.Map>[] sources,
            Func<java.util.List[], java.util.List> combine,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(sources);
            ArgumentNullException.ThrowIfNull(combine);

            var lists = new java.util.List[sources.Length];
            for (int i = 0; i < sources.Length; i++)
                lists[i] = await ToJavaList(sources[i], cancellationToken).ConfigureAwait(false);

            foreach (var row in ClrEnumerableDefaults.FromJavaList<TResult>(combine(lists)))
                yield return row;
        }

        /// <summary>
        /// Reads a whole sequence into a list.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// Not an operator and not in <see cref="ClrAsyncBuiltInMethod"/>: no plan calls it. It is what an
        /// operator that cannot yield until it has read everything uses to do its awaiting in one place, so
        /// that the part of it which is not about waiting can be the synchronous convention's own code rather
        /// than a second copy of it.
        /// </remarks>
        static async System.Threading.Tasks.ValueTask<List<TSource>> Buffer<TSource>(IAsyncEnumerable<TSource> source, CancellationToken cancellationToken)
        {
            var buffer = new List<TSource>();

            await foreach (var row in source.WithCancellation(cancellationToken))
                buffer.Add(row);

            return buffer;
        }

    }

}
