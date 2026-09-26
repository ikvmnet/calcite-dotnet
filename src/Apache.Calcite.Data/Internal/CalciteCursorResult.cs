using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;
using Apache.Calcite.Extensions.Runtime;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a plan through the cursor it opened.
    /// </summary>
    /// <remarks>
    /// The cursor is the plan's own: a <see cref="ClrCursor"/> with <see cref="ClrCursor.Read"/> and
    /// <see cref="ClrCursor.ReadAsync"/> over one position, so <see cref="Read"/> is the one and
    /// <see cref="ReadAsync"/> the other, with the token that call was given handed to the advance. Nothing
    /// stands between a row and the reader, and nothing here decides in advance which way the rows will be
    /// read.
    ///
    /// <para>There used to be two of these, one over an <c>IEnumerator</c> and one over an
    /// <c>IAsyncEnumerator</c>, and a connection-string key to choose between them, because a sequence
    /// states once whether it will be pulled or awaited. A cursor does not, so there is one result and no
    /// key.</para>
    /// </remarks>
    internal sealed class CalciteCursorResult : CalciteResult
    {

        readonly ClrCursor? _cursor;
        readonly IDisposable? _dataContext;
        readonly CancellationTokenSource? _cancellation;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="registry">The mappings the session reads values through.</param>
        /// <param name="cursor">The plan's cursor, or <see langword="null"/> where there is nothing to
        /// read — a DDL statement has already taken effect, and a DML one reports a count.</param>
        /// <param name="recordsAffected"></param>
        /// <param name="dataContext">The statement's context, which holds the registration tying its token
        /// to Calcite's cancel flag.</param>
        /// <param name="cancellation">The source the plan was opened under, linked to the caller's token.
        /// This owns and disposes both: they live as long as the rows do.</param>
        public CalciteCursorResult(IClrPrepare.Signature signature, ClrTypeRegistry registry, ClrCursor? cursor, long recordsAffected = -1, IDisposable? dataContext = null, CancellationTokenSource? cancellation = null) :
            base(signature, registry, recordsAffected)
        {
            _cursor = cursor;
            _dataContext = dataContext;
            _cancellation = cancellation;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The cursor's synchronous advance. It blocks only where the leaf can only be awaited, and the
        /// cursor does that blocking with the synchronization context suppressed before the call; everywhere
        /// else nothing waits, because nothing was made asynchronous to begin with.
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
        /// The cursor's awaiting advance, given this call's token: it reaches every operator down to the
        /// leaf, which is what <c>DbDataReader.ReadAsync(token)</c> means and what a sequence could not
        /// carry. The token is also registered against the statement's cancellation for the length of the
        /// call, so that a sub-plan of Calcite's, which polls the cancel flag rather than taking a token,
        /// stops too. That registration is scoped to the call, as <c>SqlDataReader.ReadAsync</c> scopes
        /// its own, and it is made before the token is checked, for the reason that reader gives: a token
        /// already cancelled kills the statement rather than being quietly declined.
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
