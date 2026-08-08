using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a plan of the <c>ClrAsyncEnumerableConvention</c> calling convention.
    /// </summary>
    internal sealed class CalciteAsyncEnumerableResult : CalciteResult
    {

        readonly IAsyncEnumerator<object>? _enumerator;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator">The plan's enumerator, already given the caller's cancellation token,
        /// or <see langword="null"/> where there is nothing to read.</param>
        /// <param name="recordsAffected"></param>
        public CalciteAsyncEnumerableResult(ClrSignature signature, IAsyncEnumerator<object>? enumerator, long recordsAffected = -1) :
            base(signature, recordsAffected)
        {
            _enumerator = enumerator;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Blocks on <see cref="ReadAsync"/>, because <c>DbDataReader.Read</c> has to be answerable: a
        /// consumer that knows nothing but the ADO.NET interface calls it, and a provider whose reader
        /// throws there is not one.
        ///
        /// <para>This is not the sync-over-async the convention refuses. That rule is about what a plan
        /// does inside itself, where a converter would insert blocking nobody chose; a caller reaching for
        /// <c>Read</c> on a reader they asked to be asynchronous is choosing it in the open. The usual
        /// caution applies -- a synchronization context can deadlock on it -- and <see cref="ReadAsync"/>
        /// is how to avoid that.</para>
        /// </remarks>
        public override bool Read()
        {
            return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Reads the next row.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>Whether there was a row.</returns>
        /// <remarks>
        /// <b>The token that reaches the leaf is the one given to <c>ExecuteReaderAsync</c>, not this one.</b>
        /// An <see cref="IAsyncEnumerable{T}"/> takes its token at
        /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, which happened once when this was made;
        /// <c>DbDataReader.ReadAsync</c> offers a token per call and there is nowhere to put a later one. So
        /// a token passed only here stops the reader between rows — which is what the check below does — but
        /// cannot interrupt a table already waiting on I/O.
        ///
        /// <para>Nothing can be done about that without giving every operator a token the plan threads,
        /// which is the design this convention deliberately does not have: a token enters at the enumerator
        /// and the language carries it the rest of the way.</para>
        /// </remarks>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var moved = _enumerator is not null && await _enumerator.MoveNextAsync().ConfigureAwait(false);

            return Accept(moved ? _enumerator!.Current : null, moved);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A synchronous disposal of an asynchronous plan has nowhere to await, and blocking here is the
        /// deadlock this convention exists to avoid. So it completes the disposal only where the plan
        /// finished it synchronously, and a caller reading asynchronously has
        /// <see cref="CalciteResult.DisposeAsync"/>.
        /// </remarks>
        protected override void Release()
        {
            if (_enumerator is null)
                return;

            var pending = _enumerator.DisposeAsync();
            if (pending.IsCompleted)
                pending.GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        protected override async ValueTask ReleaseAsync()
        {
            if (_enumerator is not null)
                await _enumerator.DisposeAsync().ConfigureAwait(false);
        }

    }

}
