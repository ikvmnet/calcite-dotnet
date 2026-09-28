using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Reads an <see cref="IEnumerable{T}"/> as an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// The default awaiting half of a table that only produces rows synchronously:
    /// <see cref="Schema.IClrScannableTable.ScanAsync"/> uses it.
    /// </remarks>
    static class ClrSequences
    {

        /// <summary>
        /// Reads a synchronous sequence as an asynchronous one.
        /// </summary>
        /// <typeparam name="TSource">The element type.</typeparam>
        /// <param name="source">The sequence to read.</param>
        /// <param name="cancellationToken">Not used; the token given to <c>GetAsyncEnumerator</c> is the one
        /// checked.</param>
        /// <returns>An asynchronous sequence over the same elements, which never suspends.</returns>
        /// <remarks>
        /// The token is checked before each element is returned, since nothing is awaited.
        /// </remarks>
        public static IAsyncEnumerable<TSource> ToAsyncEnumerable<TSource>(IEnumerable<TSource> source, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            // the source enumerator is obtained in GetAsyncEnumerator, as linq4j obtains a source when its
            // own enumerator is obtained, rather than deferred to the first MoveNextAsync
            return new ClrAsyncEnumerable<TSource>(token =>
            {
                var e = source.GetEnumerator();
                return new AcquiredAsyncEnumerator<TSource>(ToAsyncEnumerableRows(e, token), new SynchronousDisposal(e));
            });
        }

        /// <summary>
        /// The row loop of <see cref="ToAsyncEnumerable{TSource}"/>, over an already obtained enumerator.
        /// </summary>
        /// <remarks>
        /// The trailing <c>await</c> is there because an async iterator must contain one.
        /// </remarks>
        static async IAsyncEnumerator<TSource> ToAsyncEnumerableRows<TSource>(IEnumerator<TSource> source, CancellationToken cancellationToken)
        {
            while (source.MoveNext())
            {
                cancellationToken.ThrowIfCancellationRequested();

                yield return source.Current;
            }

            await Task.CompletedTask;
        }

        /// <summary>
        /// Adapts an <see cref="IDisposable"/> to <see cref="IAsyncDisposable"/>, disposing it synchronously.
        /// </summary>
        internal sealed class SynchronousDisposal(IDisposable disposable) : IAsyncDisposable
        {

            /// <inheritdoc />
            public ValueTask DisposeAsync()
            {
                disposable.Dispose();
                return default;
            }

        }

    }

}
