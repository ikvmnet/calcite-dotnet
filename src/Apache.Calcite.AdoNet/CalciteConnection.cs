using System;
using System.Data;
using System.Data.Common;

using Apache.Calcite.AdoNet.Internal;
using Apache.Calcite.AdoNet.Protocol;

namespace Apache.Calcite.AdoNet
{

    /// <summary>
    /// Represents a connection to an Apache Calcite engine.
    /// </summary>
    public sealed class CalciteConnection : DbConnection
    {

        CalciteConnectionStringBuilder _options = new();
        CalciteSession? _session;
        ConnectionState _state = ConnectionState.Closed;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class.
        /// </summary>
        public CalciteConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class.
        /// </summary>
        /// <param name="connectionString"></param>
        public CalciteConnection(string? connectionString)
        {
            ConnectionString = connectionString ?? string.Empty;
        }

        /// <inheritdoc />
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => _options.ConnectionString;
            set
            {
                if (_state != ConnectionState.Closed)
                    throw new InvalidOperationException("Connection string cannot be changed while the connection is open.");
                _options = new CalciteConnectionStringBuilder(value);
            }
        }

        /// <inheritdoc />
        public override string Database => _options.Schema ?? string.Empty;

        /// <inheritdoc />
        public override string DataSource => _options.Model ?? string.Empty;

        /// <inheritdoc />
        public override string ServerVersion => "Apache Calcite (ADO.NET)";

        /// <inheritdoc />
        public override ConnectionState State => _state;

        /// <inheritdoc />
        public override void ChangeDatabase(string databaseName)
        {
            _options.Schema = databaseName;
        }

        /// <inheritdoc />
        public override void Open()
        {
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Connection is already open or in a transitional state.");

            _state = ConnectionState.Connecting;
            try
            {
                var client = CalciteClientFactory.Create(_options);
                _session = new CalciteSession(_options, client);
                _state = ConnectionState.Open;
            }
            catch
            {
                _state = ConnectionState.Closed;
                throw;
            }
        }

        /// <inheritdoc />
        public override void Close()
        {
            if (_state == ConnectionState.Closed)
                return;

            _session?.Dispose();
            _session = null;
            _state = ConnectionState.Closed;
        }

        /// <inheritdoc />
        protected override DbCommand CreateDbCommand() => new CalciteCommand { Connection = this };

        /// <summary>
        /// Creates a new <see cref="CalciteCommand"/> associated with this connection.
        /// </summary>
        /// <returns></returns>
        public new CalciteCommand CreateCommand() => new() { Connection = this };

        /// <inheritdoc />
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            new CalciteTransaction(this, isolationLevel);

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Close();

            base.Dispose(disposing);
        }

        /// <summary>
        /// Returns the active session, throwing if the connection is not open.
        /// </summary>
        /// <returns></returns>
        internal CalciteSession RequireSession()
        {
            if (_state != ConnectionState.Open || _session is null)
                throw new InvalidOperationException("Connection is not open.");

            return _session;
        }

    }

}
