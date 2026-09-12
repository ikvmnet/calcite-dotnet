using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a plan compiled to an <c>IAsyncEnumerable</c>.
    /// </summary>
    internal sealed class CalciteAsyncEnumerableResult : CalciteResult
    {

        readonly IAsyncEnumerator<object>? _enumerator;
        readonly StatementCancellation? _cancellation;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator">The plan's enumerator, already given the statement's cancellation token,
        /// or <see langword="null"/> where there is nothing to read.</param>
        /// <param name="recordsAffected"></param>
        /// <param name="cancellation">The statement's cancellation, which this owns and disposes: it lives
        /// as long as the rows do.</param>
        public CalciteAsyncEnumerableResult(IClrPrepare.Signature signature, IAsyncEnumerator<object>? enumerator, long recordsAffected = -1, StatementCancellation? cancellation = null) :
            base(signature, recordsAffected)
        {
            _enumerator = enumerator;
            _cancellation = cancellation;
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
        /// <b>The token the leaf is enumerating under is the statement's, fixed at
        /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/> when this was made.</b>
        /// <c>DbDataReader.ReadAsync</c> offers a token per call and <c>MoveNextAsync</c> takes none, so a
        /// token given here cannot be handed to the sequence; what it gets instead is a registration against
        /// the statement's cancellation, for the duration of the call. Cancelling it therefore cancels the
        /// statement rather than the row — there is no cancelling one <c>MoveNextAsync</c> out of a
        /// sequence — and the reader is dead afterwards rather than resumable.
        ///
        /// <para><b>Every call takes its own token, and the registration lasts only that call.</b> So a
        /// reader can be read repeatedly under a different token each time, and a token cancelled after its
        /// read has returned reaches nothing — the registration is already gone. What one read's token
        /// cannot do is leave the reader usable after cancelling it: the statement is what gets cancelled,
        /// because that is the only thing there is to cancel.</para>
        ///
        /// <para>That is what <c>SqlDataReader.ReadAsync</c> does, read rather than remembered: it registers
        /// the token it is given against <c>SqlCommand.Cancel</c>, scoped to the call with a disposable
        /// holder. <b>It registers before it checks whether the token is already cancelled</b>, and says
        /// why — "to catch any already expired tokens to be able to trigger cancellation event" — so a read
        /// asked for under a dead token kills the statement rather than being quietly declined. The order
        /// here is the same, and it is the order that makes a token cancelled a moment before the call and a
        /// moment after it do the same thing.</para>
        /// </remarks>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            // registered before the check, as SqlDataReader.ReadAsync registers before its own
            using var registration = _cancellation?.Register(cancellationToken) ?? default;

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
            try
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
            }
            finally
            {
                _cancellation?.Dispose();
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
            try
            {
                if (_enumerator is not null)
                    await _enumerator.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _cancellation?.Dispose();
            }
        }

    }

}
