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
    /// What a table of this project's SPI answers its awaiting half with where it has only pulled rows —
    /// <see cref="Schema.IClrScannableTable.ScanAsync"/> and <see cref="Schema.IClrQueryableTable"/>'s
    /// awaiting expression by default. The rows are not touched. It costs a state machine and no thread,
    /// and nothing it produces ever suspends.
    ///
    /// <para><b>Internal, and it stays internal.</b> It is what this convention's own plans are built from,
    /// not a utility for an adapter. A table outside this assembly that has to bridge its own two halves
    /// writes that itself, in whatever its target framework gives it; there is nothing here it needs, and
    /// exposing the plan's own operators would invite an adapter to build against them.</para>
    /// </remarks>
    static class ClrSequences
    {

        /// <summary>
        /// Reads a synchronous sequence as an asynchronous one.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// Nothing here suspends, because the source is pulled. Producing an asynchronous sequence that always
        /// completes synchronously costs a state machine and no thread; a plan reading a pulled table this way
        /// is simply not asynchronous over that part of itself.
        ///
        /// <para>The token is checked per row rather than awaited on, which is the only way an operator that
        /// never suspends can honour one.</para>
        ///
        /// <para>The trailing <c>await</c> is what makes the compiler accept an async iterator that has
        /// nothing to await.</para>
        /// </remarks>
        public static IAsyncEnumerable<TSource> ToAsyncEnumerable<TSource>(IEnumerable<TSource> source, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);

            // the pulled rows are acquired at their GetEnumerator, so this acquires them at
            // GetAsyncEnumerator, where an awaited sequence's acquisition arrives. The token that matters
            // is GetAsyncEnumerator's.
            return new ClrAsyncEnumerable<TSource>(token =>
            {
                var e = source.GetEnumerator();
                return new AcquiredAsyncEnumerator<TSource>(ToAsyncEnumerableRows(e, token), new SynchronousDisposal(e));
            });
        }

        /// <summary>
        /// The row loop of <see cref="ToAsyncEnumerable{TSource}"/>, over an enumerator the factory
        /// acquired.
        /// </summary>
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
        /// Disposes the acquired synchronous enumerator from an asynchronous disposal, which completes
        /// synchronously.
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
