using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// One result set of an executed statement: its columns, its affected-row count and its current row.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is what a <see cref="CalciteDataReader"/> holds per result set and what every execute path in
    /// <see cref="CalciteSession"/> returns. Advancing is the subclass's; <see cref="CalciteCursorResult"/>
    /// is the only one.
    /// </para>
    /// <para>
    /// Both <see cref="Read"/> and <see cref="ReadAsync"/> are always supported, whichever way the statement
    /// was executed, because a generic <c>DbDataReader</c> consumer such as <c>DataTable.Load</c> calls
    /// <c>Read</c>. <see cref="Read"/> blocks only where a table can only produce rows asynchronously, and
    /// <see cref="ReadAsync"/> completes synchronously where nothing is awaited.
    /// </para>
    /// </remarks>
    internal abstract class CalciteResult : IDisposable, IAsyncDisposable
    {

        readonly IClrPrepare.Signature _signature;
        readonly CalciteResultColumns _columns;
        readonly long _recordsAffected;

        CalciteResultRow? _current;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature">The prepared statement, which gives the columns and the row shape.</param>
        /// <param name="registry">The mappings the session reads values through.</param>
        /// <param name="recordsAffected">The affected-row count <see cref="RecordsAffected"/> reports.</param>
        protected CalciteResult(IClrPrepare.Signature signature, ClrTypeRegistry registry, long recordsAffected)
        {
            ArgumentNullException.ThrowIfNull(signature);
            ArgumentNullException.ThrowIfNull(registry);

            _signature = signature;
            _columns = new CalciteResultColumns(signature, registry);
            _recordsAffected = recordsAffected;
        }

        /// <summary>
        /// Gets the result's columns.
        /// </summary>
        public CalciteResultColumns Columns => _columns;

        /// <summary>
        /// Gets the affected-row count the execute path recorded. The reader paths record 0; the non-query
        /// path records -1 for a query, 0 for DDL and the count for DML.
        /// </summary>
        public long RecordsAffected => _recordsAffected;

        /// <summary>
        /// Gets the current row.
        /// </summary>
        /// <exception cref="InvalidOperationException">No row has been read, or the last read found none.</exception>
        public CalciteResultRow Current => _current ?? throw new InvalidOperationException();

        /// <summary>
        /// Advances to the next row synchronously.
        /// </summary>
        /// <returns>Whether there was a row.</returns>
        public abstract bool Read();

        /// <summary>
        /// Advances to the next row, awaiting where the plan has something to await.
        /// </summary>
        /// <param name="cancellationToken">The token for this advance.</param>
        /// <returns>Whether there was a row.</returns>
        public abstract Task<bool> ReadAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Records the row just read, or that there was none.
        /// </summary>
        /// <param name="row">The row the plan produced, or <see langword="null"/> where it is exhausted.</param>
        /// <param name="moved">Whether the advance produced a row.</param>
        /// <returns>Whether there was a row.</returns>
        protected bool Accept(object? row, bool moved)
        {
            _current = moved ? new CalciteResultRow(_columns, _signature.CursorFactory, row) : null;
            return moved;
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> where this instance has been disposed.
        /// </summary>
        protected void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        /// <summary>
        /// Releases the plan's cursor.
        /// </summary>
        protected abstract void Release();

        /// <summary>
        /// Releases the plan's cursor, awaiting it where it has something to await.
        /// </summary>
        protected abstract ValueTask ReleaseAsync();

        /// <summary>
        /// Releases the plan's cursor. Exceptions thrown while releasing are swallowed.
        /// </summary>
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

        /// <summary>
        /// Releases the plan's cursor, awaiting where it has something to await. Exceptions thrown while
        /// releasing are swallowed.
        /// </summary>
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
