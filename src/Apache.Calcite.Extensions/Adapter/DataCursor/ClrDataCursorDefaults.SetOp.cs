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

    }

}
