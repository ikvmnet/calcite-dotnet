using System;
using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Adapts a Calcite <see cref="Enumerator"/> over a prepared <see cref="CalcitePrepare.CalciteSignature"/>.
    /// </summary>
    internal sealed record CalciteResult : IDisposable
    {

        readonly CalcitePrepare.CalciteSignature _signature;
        readonly CalciteResultColumns _columns;
        readonly Enumerator? _enumerator;
        readonly long _recordsAffected;
        readonly CancellationTokenRegistration _cancelRegistration;

        CalciteResultRow? _current = null;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="signature"></param>
        /// <param name="enumerator"></param>
        /// <param name="recordsAffected"></param>
        /// <param name="cancelRegistration"></param>
        public CalciteResult(CalcitePrepare.CalciteSignature signature, CancellationTokenRegistration cancelRegistration, Enumerator? enumerator, long recordsAffected = -1)
        {
            ArgumentNullException.ThrowIfNull(signature);
            ArgumentNullException.ThrowIfNull(cancelRegistration);

            _columns = new CalciteResultColumns(signature);
            _signature = signature;
            _cancelRegistration = cancelRegistration;
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
        /// Reads the next row from the enumerator. Returns <c>false</c> if there are no more rows to read.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (_enumerator is null || _enumerator.moveNext() == false)
            {
                _current = null;
                return Task.FromResult(false);
            }

            _current = new CalciteResultRow(_columns, _signature.cursorFactory, _enumerator.current());
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
            _cancelRegistration.Dispose();

            try
            {
                _enumerator?.close();
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
