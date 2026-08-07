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
    internal sealed record CalciteResult : IDisposable
    {

        readonly ClrSignature _signature;
        readonly CalciteResultColumns _columns;
        readonly IEnumerator<object>? _enumerator;
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
        public Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (_enumerator is null || _enumerator.MoveNext() == false)
            {
                _current = null;
                return Task.FromResult(false);
            }

            _current = new CalciteResultRow(_columns, _signature.CursorFactory, _enumerator.Current);
            return Task.FromResult(true);
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
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CalciteResult));
        }

    }

}
