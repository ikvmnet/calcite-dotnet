using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;

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
        /// Written out rather than delegated, for the reason the class remarks give. It stops reading the
        /// input once it has what it needs, which is what makes a fetch over a table a fetch rather than a
        /// scan.
        /// </remarks>
        public static async IAsyncEnumerable<TSource> Take<TSource>(IAsyncEnumerable<TSource> source, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            if (count <= 0)
                yield break;

            var taken = 0;

            await foreach (var row in source.WithCancellation(cancellationToken))
            {
                yield return row;

                if (++taken >= count)
                    yield break;
            }
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
        public static async IAsyncEnumerable<TSource> AsAsyncEnumerable<TSource>(TSource[] source, [EnumeratorCancellation] CancellationToken cancellationToken = default)
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
