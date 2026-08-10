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
        public CalciteAsyncEnumerableResult(PreparedPlan signature, IAsyncEnumerator<object>? enumerator, long recordsAffected = -1) :
            base(signature, recordsAffected)
        {
            _enumerator = enumerator;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Blocks on <see cref="ReadAsync"/>, because <c>DbDataReader.Read</c> has to be answerable: a
        /// consumer that knows nothing but the ADO.NET interface calls it, and a provider whose reader
        /// throws there is not one. In the default mode this is the synchronous surface over every query the
        /// connection plans, so it blocks only where the plan genuinely suspends — a synchronous source
        /// beneath the converters answers with a completed task and the wait never happens.
        ///
        /// <para><b>The synchronization context is suppressed around the whole call, not around the
        /// wait.</b> The operators of the asynchronous convention await without <c>ConfigureAwait(false)</c>,
        /// and a continuation captures the context at the moment of suspension — which is inside
        /// <c>MoveNextAsync</c>'s synchronous phase, on this thread, <em>before</em> any wait begins.
        /// Measured: suppressing only around the wait deadlocks under a context that cannot pump, because
        /// the capture has already happened; suppressing before the call completes, because there is nothing
        /// to capture and the continuation goes to the thread pool. Where there is no context, which is
        /// every thread a query is normally read on, this costs one read of
        /// <see cref="SynchronizationContext.Current"/>.</para>
        /// </remarks>
        public override bool Read()
        {
            var context = SynchronizationContext.Current;
            if (context is null)
                return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
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

            if (_enumerator is null || await _enumerator.MoveNextAsync().ConfigureAwait(false) == false)
                return Accept(null, false);

            return Accept(_enumerator.Current, true);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Blocks until the plan's disposal completes, the way <see cref="Read"/> blocks for a row. In the
        /// default mode a synchronous consumer's <c>using</c> lands here over every query, and abandoning a
        /// disposal that did not finish synchronously would leak whatever the leaf holds open. The context
        /// is suppressed before <c>DisposeAsync</c> is called, for the reason <see cref="Read"/> gives: an
        /// iterator's <c>finally</c> can suspend too, and its continuation captures the context at
        /// suspension, inside the call.
        /// </remarks>
        protected override void Release()
        {
            if (_enumerator is null)
                return;

            var context = SynchronizationContext.Current;
            if (context is null)
            {
                Wait(_enumerator.DisposeAsync());
                return;
            }

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                Wait(_enumerator.DisposeAsync());
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }

            static void Wait(ValueTask pending)
            {
                if (pending.IsCompleted)
                    pending.GetAwaiter().GetResult();
                else
                    pending.AsTask().GetAwaiter().GetResult();
            }
        }

        /// <inheritdoc />
        protected override async ValueTask ReleaseAsync()
        {
            if (_enumerator is not null)
                await _enumerator.DisposeAsync().ConfigureAwait(false);
        }

    }

}
