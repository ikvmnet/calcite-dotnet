using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a prepared <see cref="ClrSignature"/>.
    /// </summary>
    /// <remarks>
    /// The enumerator is the plan's own — a compiled delegate hands back an
    /// <see cref="IEnumerator{T}"/> of objects — so nothing stands between a row and the reader.
    /// </remarks>
    internal sealed record CalciteResult : IDisposable, IAsyncDisposable
    {

        readonly ClrSignature _signature;
        readonly CalciteResultColumns _columns;
        readonly IEnumerator<object>? _enumerator;
        readonly IAsyncEnumerator<object>? _asyncEnumerator;
        readonly long _recordsAffected;

        CalciteResultRow? _current = null;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator"></param>
        /// <param name="recordsAffected"></param>
        public CalciteResult(ClrSignature signature, IEnumerator<object>? enumerator, long recordsAffected = -1)
        {
            ArgumentNullException.ThrowIfNull(signature);

            _columns = new CalciteResultColumns(signature);
            _signature = signature;
            _enumerator = enumerator;
            _recordsAffected = recordsAffected;
        }

        /// <summary>
        /// Initializes a new instance over an asynchronous plan.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator"></param>
        /// <param name="recordsAffected"></param>
        public CalciteResult(ClrSignature signature, IAsyncEnumerator<object>? enumerator, long recordsAffected = -1)
        {
            ArgumentNullException.ThrowIfNull(signature);

            _columns = new CalciteResultColumns(signature);
            _signature = signature;
            _asyncEnumerator = enumerator;
            _recordsAffected = recordsAffected;
        }

        /// <summary>
        /// Gets whether the rows come from a plan of the asynchronous convention.
        /// </summary>
        /// <remarks>
        /// Readable because a caller has no other way to tell, and because a reader that fell back to a
        /// synchronous plan is not doing what it was asked. A silent fallback is how "it is asynchronous
        /// now" stops being true without anyone noticing.
        /// </remarks>
        public bool IsAsynchronous => _asyncEnumerator is not null;

        /// <summary>
        /// Gets the collection of columns returned by the Calcite query result.
        /// </summary>
        public CalciteResultColumns Columns => _columns;

        /// <summary>
        /// Gets the number of records affected by the operation, if available.
        /// </summary>
        public long RecordsAffected => _recordsAffected;

        /// <summary>
        /// Reads the next row from the enumerator.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <remarks>
        /// <b>The token that reaches the leaf is the one given to <c>ExecuteReaderAsync</c>, not this one.</b>
        /// An <see cref="IAsyncEnumerable{T}"/> takes its token at
        /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, which happened once when the reader was
        /// created; <c>DbDataReader.ReadAsync</c> offers a token per call, and there is nowhere to put a
        /// later one. So a token passed only here stops the reader between rows — that is what the check
        /// below does — but cannot interrupt a table already waiting on I/O.
        ///
        /// <para>Nothing can be done about that shape without giving every operator a token parameter the
        /// plan would have to thread, which is the design this convention deliberately does not have. A
        /// caller who wants a read interruptible mid-row passes the token to <c>ExecuteReaderAsync</c>.</para>
        /// </remarks>
        public async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (_asyncEnumerator is not null)
            {
                if (await _asyncEnumerator.MoveNextAsync().ConfigureAwait(false) == false)
                {
                    _current = null;
                    return false;
                }

                _current = new CalciteResultRow(_columns, _signature.CursorFactory, _asyncEnumerator.Current);
                return true;
            }

            // a synchronous plan completes synchronously, which blocks nothing: it is not asynchronous, and
            // that is a different thing from blocking a thread while pretending to be
            if (_enumerator is null || _enumerator.MoveNext() == false)
            {
                _current = null;
                return false;
            }

            _current = new CalciteResultRow(_columns, _signature.CursorFactory, _enumerator.Current);
            return true;
        }

        /// <summary>
        /// Reads the next row, refusing to block on an asynchronous plan.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">The rows come from an asynchronous plan.</exception>
        /// <remarks>
        /// The one place a caller could stumble into sync over async by accident, so it is the one place
        /// that refuses. Blocking on <c>MoveNextAsync</c> here would deadlock on a synchronization context
        /// and would waste a thread everywhere else; a caller holding an asynchronous reader has
        /// <see cref="ReadAsync"/>.
        /// </remarks>
        public bool Read()
        {
            if (_asyncEnumerator is not null)
                throw new InvalidOperationException(
                    "This result was prepared into the asynchronous convention and can only be read with ReadAsync.");

            return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gets the current row.
        /// </summary>
        public CalciteResultRow Current => _current ?? throw new InvalidOperationException();

        /// <summary>
        /// Disposes of the instance.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                _enumerator?.Dispose();
            }
            catch
            {
                // best-effort cleanup
            }

            if (_asyncEnumerator is not null)
            {
                // a synchronous Dispose of an asynchronous plan has nowhere to await, and blocking here is
                // the deadlock this convention exists to avoid. DisposeAsync is the way, and a caller
                // reading asynchronously has one.
                try
                {
                    var pending = _asyncEnumerator.DisposeAsync();
                    if (pending.IsCompleted)
                        pending.GetAwaiter().GetResult();
                }
                catch
                {
                    // best-effort cleanup
                }
            }
        }

        /// <summary>
        /// Disposes of the instance, awaiting an asynchronous plan's own disposal.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                _enumerator?.Dispose();

                if (_asyncEnumerator is not null)
                    await _asyncEnumerator.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // best-effort cleanup
            }
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CalciteResult));
        }

    }

}
