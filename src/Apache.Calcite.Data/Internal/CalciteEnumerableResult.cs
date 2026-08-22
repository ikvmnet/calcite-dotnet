using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Common;
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

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="registry">The connection's type mapping.</param>
        /// <param name="signature"></param>
        /// <param name="enumerator">The plan's enumerator, or <see langword="null"/> where there is nothing
        /// to read — a DDL statement has already taken effect, and a DML one reports a count.</param>
        /// <param name="recordsAffected"></param>
        public CalciteEnumerableResult(ClrTypeRegistry registry, IClrPrepare.Signature signature, IEnumerator<object>? enumerator, long recordsAffected = -1) :
            base(registry, signature, recordsAffected)
        {
            _enumerator = enumerator;
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
        /// </remarks>
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return Task.FromResult(Read());
        }

        /// <inheritdoc />
        protected override void Release()
        {
            _enumerator?.Dispose();
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
