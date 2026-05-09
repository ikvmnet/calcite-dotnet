using System;

using Apache.Calcite.AdoNet.Protocol;

namespace Apache.Calcite.AdoNet.Internal
{

    /// <summary>
    /// Represents the per-connection runtime state established when a <see cref="System.Data.Common.DbConnection"/>
    /// is opened. This is the boundary between the public ADO.NET surface and the internal protocol/engine layers.
    /// </summary>
    internal sealed class CalciteSession : IDisposable
    {

        readonly CalciteConnectionStringBuilder _options;
        readonly ICalciteClient _client;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteSession"/> class.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="client"></param>
        public CalciteSession(CalciteConnectionStringBuilder options, ICalciteClient client)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        /// <summary>
        /// Gets the resolved connection options for this session.
        /// </summary>
        public CalciteConnectionStringBuilder Options => _options;

        /// <summary>
        /// Gets the protocol client used to dispatch requests for this session.
        /// </summary>
        public ICalciteClient Client => _client;

        /// <summary>
        /// Gets a value indicating whether the session has been disposed.
        /// </summary>
        public bool IsDisposed => _disposed;

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _client.Dispose();
        }

    }

}
