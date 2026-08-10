using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a prepared <see cref="PreparedPlan"/>.
    /// </summary>
    /// <remarks>
    /// What a reader holds, and what both execute paths return. Everything about a <em>row</em> is here —
    /// the columns, the cursor factory, the current row, the affected count — because a row is the same
    /// thing whichever convention produced it. Reading is what the two subclasses differ by, and it is the
    /// only thing they differ by.
    ///
    /// <para>Two classes rather than one holding two enumerators. A single class decided which it was
    /// from a nullable field, so nothing could be relied on at a call site; here the type is the answer and
    /// each subclass holds exactly the enumerator it has.</para>
    ///
    /// <para><b>Both read methods are on both</b>, and that is deliberate rather than a compromise.
    /// <c>DbDataReader</c> is a contract: a consumer that knows nothing but <c>DbDataReader</c> -- a
    /// micro-ORM, <c>DataTable.Load</c>, anything generic -- calls <c>Read</c>, and a provider whose reader
    /// throws there is not a provider. So a synchronous plan answers <c>ReadAsync</c> with a completed
    /// task, and an asynchronous plan blocks in <c>Read</c>. Which plan a query gets is the connection's
    /// mode, not the entry point's choice, so either crossing is the normal case rather than an edge.</para>
    ///
    /// <para>Neither is the sync-over-async this surface refuses, and the line is about who chose. A
    /// <em>plan's</em> internals can block too — <c>ClrAsyncEnumerableToClrEnumerableConverter</c> is exactly
    /// that, one blocked thread per row — and what makes it refusable there is that the planner would be
    /// choosing it, invisibly, on behalf of a caller who asked for nothing of the sort. That is why the
    /// prepare pipeline registers one convention's rules and not both, and so never produces a plan holding
    /// one. Here the block is at the boundary, where every ADO.NET provider blocks: it is what <c>Read</c>
    /// on an asynchronous source means, and the asynchronous surface never blocks at all — a synchronous
    /// part of a plan completes synchronously, which parks nothing.</para>
    /// </remarks>
    internal abstract class CalciteResult : IDisposable, IAsyncDisposable
    {

        readonly PreparedPlan _signature;
        readonly CalciteResultColumns _columns;
        readonly long _recordsAffected;

        CalciteResultRow? _current;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="recordsAffected"></param>
        protected CalciteResult(PreparedPlan signature, long recordsAffected)
        {
            ArgumentNullException.ThrowIfNull(signature);

            _signature = signature;
            _columns = new CalciteResultColumns(signature);
            _recordsAffected = recordsAffected;
        }

        /// <summary>
        /// Gets the collection of columns returned by the Calcite query result.
        /// </summary>
        public CalciteResultColumns Columns => _columns;

        /// <summary>
        /// Gets the number of records affected by the operation, if available.
        /// </summary>
        public long RecordsAffected => _recordsAffected;

        /// <summary>
        /// Gets the current row.
        /// </summary>
        public CalciteResultRow Current => _current ?? throw new InvalidOperationException();

        /// <summary>
        /// Reads the next row.
        /// </summary>
        /// <returns>Whether there was a row.</returns>
        public abstract bool Read();

        /// <summary>
        /// Reads the next row.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>Whether there was a row.</returns>
        public abstract Task<bool> ReadAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Records the row just read, or that there was none.
        /// </summary>
        /// <param name="row">The row the plan produced, or <see langword="null"/> where it is exhausted.</param>
        /// <param name="moved"></param>
        /// <returns>Whether there was a row.</returns>
        protected bool Accept(object? row, bool moved)
        {
            _current = moved ? new CalciteResultRow(_columns, _signature.CursorFactory, row) : null;
            return moved;
        }

        /// <summary>
        /// Throws where this instance has been disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        /// <summary>
        /// Releases the plan's enumerator.
        /// </summary>
        protected abstract void Release();

        /// <summary>
        /// Releases the plan's enumerator, awaiting it where it has something to await.
        /// </summary>
        protected abstract ValueTask ReleaseAsync();

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                Release();
            }
            catch
            {
                // best-effort cleanup
            }
        }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                await ReleaseAsync().ConfigureAwait(false);
            }
            catch
            {
                // best-effort cleanup
            }
        }

    }

}
