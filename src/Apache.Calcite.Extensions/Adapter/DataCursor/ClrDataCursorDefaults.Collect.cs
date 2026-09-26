using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The operators a collect, an uncollect and a combine are built from.
    /// </summary>
    static partial class ClrDataCursorDefaults
    {

        /// <summary>
        /// Reads every row into a Java list, closing the cursor once it is read.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.toList</c>, which is <c>source.into(new ArrayList())</c>: a drain in the
        /// method body, so it drains here, where the tree is evaluated. A <c>java.util.List</c>, because
        /// this is a value in a row and the reader of that row is Calcite's.
        /// </remarks>
        public static java.util.List ToJavaList<TSource>(ClrDataCursor<TSource> source)
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
        /// Reads every row into a Java map, keeping the order the keys were seen in, and closes the cursor
        /// once it is read.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="valueSelector"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.toMap</c>, which drains inside a <c>try</c> over the enumerator into a
        /// <c>LinkedHashMap</c>, so that the order the rows arrived in is the order the map keeps.
        /// </remarks>
        public static java.util.Map ToJavaMap<TSource>(ClrDataCursor<TSource> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector)
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
        /// Returns a cursor of one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="element"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Linq4j.singletonEnumerable</c>, whose enumerator is over the one element already in hand.
        /// </remarks>
        public static ClrDataCursor<TSource> Singleton<TSource>(TSource element)
        {
            return new ListCursor<TSource>([element]);
        }

        /// <summary>
        /// Returns each row of each sequence a function yields.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector">Yields a linq4j sequence for one row, which is what Calcite builds here.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.selectMany</c>, whose enumerator acquires the source in a field initializer
        /// at <c>enumerator()</c> — the source arrives opened here, which is the same moment — and builds and
        /// acquires each row's sequence at its turn, inside <c>moveNext</c>.
        /// </remarks>
        public static ClrDataCursor<TResult> SelectMany<TSource, TResult>(ClrDataCursor<TSource> source, Function1 selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectManyCursor<TSource, TResult>(source, selector);
        }

        /// <summary>
        /// The cursor of <see cref="SelectMany{TSource, TResult}"/>: linq4j's <c>selectMany</c> enumerator,
        /// with the row's sequence read through <see cref="JavaCursors.FromJava{TSource}"/>.
        /// </summary>
        /// <remarks>
        /// The inner sequence is linq4j's, produced for one row by a generator of Calcite's, and it is
        /// pulled whichever advance reaches it: it is a value already in hand rather than a source, so
        /// nothing about reading it can suspend, and the cursor it is read through completes its
        /// <see cref="ClrDataCursor.ReadAsync"/> synchronously for that reason. A null is where linq4j
        /// holds <c>Linq4j.emptyEnumerator()</c> before the first row and between one row's sequence and
        /// the next.
        /// </remarks>
        sealed class SelectManyCursor<TSource, TResult>(ClrDataCursor<TSource> source, Function1 selector) : ClrDataCursor<TResult>
        {

            ClrDataCursor<TResult>? result;

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
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Linq4j.asEnumerable(List)</c>, read through the crossing every linq4j sequence is read
        /// through, so each value is converted rather than cast for the reason
        /// <see cref="JavaSequences.FromJava{TSource}"/> gives.
        /// </remarks>
        public static ClrDataCursor<TSource> FromJavaList<TSource>(java.util.List source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return JavaCursors.FromJava<TSource>(org.apache.calcite.linq4j.Linq4j.asEnumerable(source));
        }

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="SelectMany{TSource, TResult}"/>, over an open that awaits.
        /// </summary>
        public static async ValueTask<ClrDataCursor<TResult>> SelectManyAsync<TSource, TResult>(ValueTask<ClrDataCursor<TSource>> source, Function1 selector, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(selector);

            return new SelectManyCursor<TSource, TResult>(await source.ConfigureAwait(false), selector);
        }

        /// <summary>
        /// Reads a whole cursor into a Java list and hands back the one row holding it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(ToJavaList(source))</c> as one operator, because the drain has to be awaited and an
        /// expression tree cannot await: the composition the synchronous body writes as two nested calls
        /// cannot be written as a tree here, so it is written as an operator.
        ///
        /// <para>The drain is at the open, as the synchronous pair's is, which is what an awaiting open can
        /// do and an <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> could not: its
        /// <c>GetAsyncEnumerator</c> cannot await, so the sequence convention's operator had to fold on the
        /// first advance and keep the row for every later enumeration. Nothing here is deferred and nothing
        /// is kept — the open awaits the drain and hands back a cursor over the one row.</para>
        /// </remarks>
        public static async ValueTask<ClrDataCursor<java.util.List>> SingletonJavaListAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, CancellationToken cancellationToken)
        {
            return Singleton(await ToJavaListAsync(source, cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Reads a whole cursor into a Java map and hands back the one row holding it.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="valueSelector"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Singleton(ToJavaMap(source, …))</c> as one operator, for the reason
        /// <see cref="SingletonJavaListAsync{TSource}"/> gives, and draining at the open as that does. A
        /// <c>LinkedHashMap</c>, so that the order the rows arrived in is the order the map keeps.
        /// </remarks>
        public static async ValueTask<ClrDataCursor<java.util.Map>> SingletonJavaMapAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, Func<TSource, object> keySelector, Func<TSource, object> valueSelector, CancellationToken cancellationToken)
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
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ToJavaList{TSource}"/>, over an open that awaits. Not named in
        /// <see cref="ClrDataCursorBuiltInMethod"/>: no plan calls it, because an expression tree cannot
        /// await what it returns. It is what the operators that have to read everything before they can
        /// hand back a row drain with.
        /// </remarks>
        public static async ValueTask<java.util.List> ToJavaListAsync<TSource>(ValueTask<ClrDataCursor<TSource>> source, CancellationToken cancellationToken)
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
        /// Opens and reads each input into a Java list, one after another, combines the lists, and hands
        /// back a cursor over the rows that come back.
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="sources">Opens each input, called at its turn.</param>
        /// <param name="combine">What to do with the lists once they are all read, which is
        /// <c>SqlFunctions.combineQueryResults</c>.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// The synchronous node reads each input into a list inside the tree and passes the lists to the
        /// combine; here each read has to be awaited and an expression tree cannot await, so the reading
        /// moves into this operator, for the reason <see cref="SingletonJavaListAsync{TSource}"/> gives.
        ///
        /// <para><b>The inputs arrive as opens rather than opened, because an awaiting open is eager where
        /// the tree it stands in is not.</b> Calcite's generated <c>bind</c> reads <c>list0</c> to completion
        /// before <c>child1</c> is touched, and the synchronous body does the same by construction: each
        /// <c>ToJavaList</c> in the array initializer runs to completion before the next element's open is
        /// evaluated. An array of awaiting opens would have started every input before the first was
        /// drained, so each is opened here, after the one before it has been read and closed.</para>
        ///
        /// <para>The combining itself is not this operator's business and arrives as a delegate, so that
        /// which function Calcite combines with stays the node's decision, as it is in the synchronous
        /// body.</para>
        /// </remarks>
        public static async ValueTask<ClrDataCursor<TResult>> CombineQueryResultsAsync<TResult>(
            Func<CancellationToken, ValueTask<ClrDataCursor<java.util.Map>>>[] sources,
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
        /// <see cref="FromJavaList{TSource}"/>, as an open that awaits. The list is a value already in
        /// hand, so there is nothing to await and it completes at once.
        /// </summary>
        public static ValueTask<ClrDataCursor<TSource>> FromJavaListAsync<TSource>(java.util.List source, CancellationToken cancellationToken)
        {
            return new ValueTask<ClrDataCursor<TSource>>(FromJavaList<TSource>(source));
        }

    }

}
