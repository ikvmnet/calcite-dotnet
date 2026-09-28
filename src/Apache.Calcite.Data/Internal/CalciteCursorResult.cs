using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;
using Apache.Calcite.Extensions.Runtime;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a plan through the cursor the plan opened.
    /// </summary>
    /// <remarks>
    /// The plan's <see cref="IClrCursor"/> has a synchronous and an awaiting advance over one position, so
    /// <see cref="Read"/> calls the one and <see cref="ReadAsync"/> the other, and a caller may mix them row
    /// by row. The result owns the statement's data context and cancellation source and disposes them with
    /// the cursor.
    /// </remarks>
    internal sealed class CalciteCursorResult : CalciteResult
    {

        readonly IClrCursor? _cursor;
        readonly IDisposable? _dataContext;
        readonly CancellationTokenSource? _cancellation;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature">The prepared statement.</param>
        /// <param name="registry">The mappings the session reads values through.</param>
        /// <param name="cursor">The plan's cursor, or <see langword="null"/> where there is nothing to
        /// read: DDL, which has already taken effect, or a statement run for its affected-row count.</param>
        /// <param name="recordsAffected">The affected-row count to report.</param>
        /// <param name="dataContext">The statement's context, which holds the registration tying its token
        /// to Calcite's cancel flag. Disposed with this result.</param>
        /// <param name="cancellation">The source the plan was opened under. Disposed with this result.</param>
        public CalciteCursorResult(IClrPrepare.Signature signature, ClrTypeRegistry registry, IClrCursor? cursor, long recordsAffected = -1, IDisposable? dataContext = null, CancellationTokenSource? cancellation = null) :
            base(signature, registry, recordsAffected)
        {
            _cursor = cursor;
            _dataContext = dataContext;
            _cancellation = cancellation;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Blocks only where a table can only produce rows asynchronously; the cursor suppresses the
        /// synchronization context before blocking, so a caller holding one does not deadlock.
        /// </remarks>
        public override bool Read()
        {
            ThrowIfDisposed();

            if (_cursor is null || _cursor.Read() == false)
                return Accept(null, false);

            return Accept(_cursor.Current, true);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The token is passed down to every operator of the plan. For the length of the call it is also
        /// registered to cancel the statement's cancellation source, which Calcite's own operators observe
        /// through the cancel flag rather than a token. As in <c>SqlDataReader.ReadAsync</c>, the registration
        /// is made before the token is checked, so a token that is already cancelled cancels the statement
        /// rather than only this call.
        /// </remarks>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            using var registration = _cancellation is not null && cancellationToken.CanBeCanceled
                ? cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), _cancellation)
                : default;

            cancellationToken.ThrowIfCancellationRequested();

            if (_cursor is null || await _cursor.ReadAsync(cancellationToken).ConfigureAwait(false) == false)
                return Accept(null, false);

            return Accept(_cursor.Current, true);
        }

        /// <inheritdoc />
        protected override void Release()
        {
            try
            {
                _cursor?.Dispose();
            }
            finally
            {
                _dataContext?.Dispose();
                _cancellation?.Dispose();
            }
        }

        /// <inheritdoc />
        protected override async ValueTask ReleaseAsync()
        {
            try
            {
                if (_cursor is not null)
                    await _cursor.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _dataContext?.Dispose();
                _cancellation?.Dispose();
            }
        }

    }

}
