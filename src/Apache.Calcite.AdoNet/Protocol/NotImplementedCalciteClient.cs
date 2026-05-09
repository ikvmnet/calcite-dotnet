using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.AdoNet.Protocol
{

    /// <summary>
    /// Placeholder client used while the native engine binding is being implemented.
    /// </summary>
    /// <remarks>
    /// All execution paths throw <see cref="NotSupportedException"/>. This keeps the public ADO.NET
    /// surface consumable and testable without yet providing real query execution.
    /// </remarks>
    internal sealed class NotImplementedCalciteClient : ICalciteClient
    {

        readonly CalciteConnectionStringBuilder _options;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="NotImplementedCalciteClient"/> class.
        /// </summary>
        /// <param name="options"></param>
        public NotImplementedCalciteClient(CalciteConnectionStringBuilder options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        /// <inheritdoc />
        public Task<ICalciteResult> ExecuteAsync(CalciteExecuteRequest request, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            throw new NotSupportedException("Calcite execution engine is not yet implemented.");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _disposed = true;
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(NotImplementedCalciteClient));
        }

    }

}
