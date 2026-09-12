using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a plan of the <c>ClrEnumerableConvention</c> calling convention.
    /// </summary>
    /// <remarks>
    /// The enumerator is the plan's own — a compiled delegate hands back an <see cref="IEnumerator{T}"/> of
    /// objects — so nothing stands between a row and the reader.
    /// </remarks>
    internal sealed class CalciteEnumerableResult : CalciteResult
    {

        readonly IEnumerator<object>? _enumerator;
        readonly StatementCancellation? _cancellation;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator">The plan's enumerator, or <see langword="null"/> where there is nothing
        /// to read — a DDL statement has already taken effect, and a DML one reports a count.</param>
        /// <param name="recordsAffected"></param>
        /// <param name="cancellation">The statement's cancellation, which this owns and disposes.</param>
        public CalciteEnumerableResult(IClrPrepare.Signature signature, IEnumerator<object>? enumerator, long recordsAffected = -1, StatementCancellation? cancellation = null) :
            base(signature, recordsAffected)
        {
            _enumerator = enumerator;
            _cancellation = cancellation;
        }

        /// <inheritdoc />
        public override bool Read()
        {
            ThrowIfDisposed();

            if (_enumerator is null || _enumerator.MoveNext() == false)
                return Accept(null, false);

            return Accept(_enumerator.Current, true);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <see cref="Read"/> in a completed task. There is nothing asynchronous here to be over: a
        /// synchronous plan produces its rows synchronously, and saying so is what lets a caller written
        /// against <c>ReadAsync</c> work over one.
        ///
        /// <para>The token is registered against the statement's cancellation for the duration of the read,
        /// as it is on the awaiting result. It cannot interrupt <see cref="Read"/> itself — a pulled plan
        /// carries no token and has nowhere to suspend — but it does set
        /// <c>DataContext.Variable.CANCEL_FLAG</c>, which is the one channel a pulled plan has and what a
        /// table of Calcite's polls between rows. Firing it takes another thread, which is the only way a
        /// blocking read can be cancelled at all.</para>
        /// </remarks>
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            // registered before the check, as on the awaiting result and as SqlDataReader.ReadAsync does
            using var registration = _cancellation?.Register(cancellationToken) ?? default;

            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult(Read());
        }

        /// <inheritdoc />
        protected override void Release()
        {
            try
            {
                _enumerator?.Dispose();
            }
            finally
            {
                _cancellation?.Dispose();
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// <see cref="Release"/>, there being nothing to await.
        /// </remarks>
        protected override ValueTask ReleaseAsync()
        {
            Release();

            return ValueTask.CompletedTask;
        }

    }

}
