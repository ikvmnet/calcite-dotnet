using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;


namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Adapts a Calcite <see cref="Enumerator"/> over a prepared <see cref="CalcitePrepare.CalciteSignature"/>
    /// into an <see cref="ICalciteResult"/> consumed by the ADO.NET reader.
    /// </summary>
    internal sealed class CalciteEngineResult : ICalciteResult
    {

        readonly IReadOnlyList<CalciteColumn> _columns;
        readonly CalcitePrepare.CalciteSignature _signature;
        readonly Enumerator _enumerator;
        readonly CancellationTokenRegistration _cancelRegistration;
        readonly RowMaterializer _materializer;
        IReadOnlyList<object?>? _current;
        bool _disposed;

        public CalciteEngineResult(
            IReadOnlyList<CalciteColumn> columns,
            CalcitePrepare.CalciteSignature signature,
            Enumerator enumerator,
            CancellationTokenRegistration cancelRegistration)
        {
            _columns = columns;
            _signature = signature;
            _enumerator = enumerator;
            _cancelRegistration = cancelRegistration;
            _materializer = RowMaterializer.For(signature.cursorFactory, columns);
        }

        public IReadOnlyList<CalciteColumn> Columns => _columns;

        public long RecordsAffected => -1;

        public IReadOnlyList<object?>? Current => _current;

        public Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            if (_enumerator.moveNext() == false)
            {
                _current = null;
                return Task.FromResult(false);
            }

            _current = _materializer.Materialize(_enumerator.current());
            return Task.FromResult(true);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _cancelRegistration.Dispose();

            try
            {
                _enumerator.close();
            }
            catch
            {
                // best-effort cleanup
            }
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CalciteEngineResult));
        }

    }

}
