using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

using Apache.Calcite.Data.Common;

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
        readonly IDisposable? _dataContext;
        readonly CancellationTokenSource? _cancellation;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator">The plan's enumerator, or <see langword="null"/> where there is nothing
        /// to read — a DDL statement has already taken effect, and a DML one reports a count.</param>
        /// <param name="registry">The mappings the session reads values through.</param>
        /// <param name="recordsAffected"></param>
        /// <param name="dataContext">The statement's context, which holds the registration tying its token
        /// to Calcite's cancel flag.</param>
        /// <param name="cancellation">The statement's cancellation source.</param>
        public CalciteEnumerableResult(IClrPrepare.Signature signature, ClrTypeRegistry registry, IEnumerator<object>? enumerator, long recordsAffected = -1, IDisposable? dataContext = null, CancellationTokenSource? cancellation = null) :
            base(signature, registry, recordsAffected)
        {
            _enumerator = enumerator;
            _dataContext = dataContext;
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
        /// <para><b>The token stops the reader between rows and no further, and there is nothing else it
        /// could do.</b> A pulled plan carries no token — no operator of the synchronous set takes one and
        /// <c>JavaSequences.FromJava</c> has none to convert at a crossing into Calcite's convention — so a
        /// <see cref="Read"/> already under way cannot be interrupted. Registering the token against the
        /// statement would reach nothing on this route and read as though it reached something. A caller
        /// that needs a read it can cancel asks for the awaiting plan, which is the default.</para>
        /// </remarks>
        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
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
                _dataContext?.Dispose();
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
