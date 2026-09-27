using System;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Prepare;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Reads the rows of a prepared <see cref="IClrPrepare.Signature"/>.
    /// </summary>
    /// <remarks>
    /// What a reader holds, and what both execute paths return. Everything about a <em>row</em> is here —
    /// the columns, the cursor factory, the current row, the affected count. Reading is the subclass's,
    /// and <see cref="CalciteCursorResult"/> is the only one: it steps the plan's cursor.
    ///
    /// <para><b>Both read methods are always answered</b>, and that is deliberate rather than a compromise.
    /// <c>DbDataReader</c> is a contract: a consumer that knows nothing but <c>DbDataReader</c> -- a
    /// micro-ORM, <c>DataTable.Load</c>, anything generic -- calls <c>Read</c>, and a provider whose reader
    /// throws there is not a provider. A plan has no mode, and its cursor carries both advances, so
    /// <c>Read</c> blocks only where a leaf can only be awaited and <c>ReadAsync</c> completes
    /// synchronously wherever nothing is awaited.</para>
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
        /// <param name="signature"></param>
        /// <param name="registry">The mappings the session reads values through.</param>
        /// <param name="recordsAffected"></param>
        protected CalciteResult(IClrPrepare.Signature signature, ClrTypeRegistry registry, long recordsAffected)
        {
            ArgumentNullException.ThrowIfNull(signature);
            ArgumentNullException.ThrowIfNull(registry);

            _signature = signature;
            _columns = new CalciteResultColumns(signature, registry);
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
        /// Releases the plan's cursor.
        /// </summary>
        protected abstract void Release();

        /// <summary>
        /// Releases the plan's cursor, awaiting it where it has something to await.
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
