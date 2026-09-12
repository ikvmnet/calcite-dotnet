using System;
using System.Threading;

using java.util.concurrent.atomic;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The cancellation of one executing statement, in both of the forms a plan can read.
    /// </summary>
    /// <remarks>
    /// A statement's plan may hold nodes of two calling conventions, and they do not cancel the same way. A
    /// node of <c>ClrEnumerableConvention</c> reads a <see cref="CancellationToken"/> — the one its sequence
    /// was given at <c>GetAsyncEnumerator</c>, which the operators carry down to the leaf. A node of
    /// Calcite's <c>EnumerableConvention</c> reads <c>DataContext.Variable.CANCEL_FLAG</c>, an
    /// <see cref="AtomicBoolean"/> in the <c>DataContext</c>, which is what a table polls: in Calcite
    /// itself <c>ListTransientTable</c> does, and so do the CSV, file and Kafka adapters' tables. Nothing
    /// polls it for them — no operator of <c>EnumerableDefaults</c> and no generated block — so the flag
    /// reaches exactly as far as the tables that read it, and a token reaches exactly as far as the
    /// operators that carry it.
    ///
    /// <para><b>One source drives both.</b> The <see cref="DataContext"/> is the whole plan's, whichever
    /// convention a node is in and on both sides of a converter, so the flag needs no help from the
    /// converter to cross: setting it once reaches every Calcite node below every converter. What was
    /// missing was anything to set it on the reader path, where the flag was made, put in the context and
    /// then dropped.</para>
    ///
    /// <para>Cancelling is one-way and cancels the statement, not one read. That is what the shape allows —
    /// <c>MoveNextAsync</c> takes no token, so there is no cancelling a single row without cancelling the
    /// sequence — and it is what the driver this one is modelled on does:
    /// <c>SqlDataReader.ReadAsync</c> registers the token it is given against <c>SqlCommand.Cancel</c>, for
    /// the duration of the call, and a cancelled read leaves the reader dead rather than resumable.</para>
    /// </remarks>
    internal sealed class StatementCancellation : IDisposable
    {

        readonly CancellationTokenSource _source;
        readonly AtomicBoolean _cancelFlag;
        readonly CancellationTokenRegistration _registration;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cancellationToken">The token the caller gave the execute call, which cancels this
        /// statement along with whatever else it cancels.</param>
        public StatementCancellation(CancellationToken cancellationToken)
        {
            _source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _cancelFlag = new AtomicBoolean(false);

            // the flag is the only channel a node of Calcite's convention has, and a plan that crosses a
            // converter has both kinds of node under one DataContext
            _registration = _source.Token.Register(static state => ((AtomicBoolean)state!).set(true), _cancelFlag);
        }

        /// <summary>
        /// Gets the token a plan of <c>ClrEnumerableConvention</c> is enumerated with.
        /// </summary>
        public CancellationToken Token => _source.Token;

        /// <summary>
        /// Gets the flag a plan of Calcite's <c>EnumerableConvention</c> polls, which goes into the
        /// statement's <c>DataContext</c> as <c>DataContext.Variable.CANCEL_FLAG</c>.
        /// </summary>
        public AtomicBoolean CancelFlag => _cancelFlag;

        /// <summary>
        /// Cancels this statement.
        /// </summary>
        public void Cancel()
        {
            if (_source.IsCancellationRequested == false)
                _source.Cancel();
        }

        /// <summary>
        /// Arranges for <paramref name="cancellationToken"/> to cancel this statement until the returned
        /// registration is disposed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>The registration, to dispose when the call that took the token returns.</returns>
        /// <remarks>
        /// What a per-call token on <c>DbDataReader.ReadAsync</c> gets, since the enumerator's own token was
        /// fixed at <c>GetAsyncEnumerator</c> and there is nowhere to put a later one. Scoped to the call,
        /// as <c>SqlDataReader.ReadAsync</c> scopes its registration, so a token cancelled after a read has
        /// returned does not reach back and kill a reader the caller went on using.
        /// </remarks>
        public CancellationTokenRegistration Register(CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled == false)
                return default;

            return cancellationToken.Register(static state => ((StatementCancellation)state!).Cancel(), this);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _registration.Dispose();
            _source.Dispose();
        }

    }

}
