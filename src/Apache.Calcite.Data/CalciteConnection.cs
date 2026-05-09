using System;
using System.Data;
using System.Data.Common;

using Apache.Calcite.Data.Internal;

using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents an open connection to an Apache Calcite engine. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// A <see cref="CalciteConnection"/> hosts a Calcite planner and runtime in-process via IKVM.
    /// The connection string follows the keys exposed by <see cref="CalciteConnectionStringBuilder"/>
    /// (for example <c>Model</c> and <c>Schema</c>) and mirrors the Calcite JDBC driver's properties
    /// where practical. Calcite-native objects associated with the open session are exposed through
    /// the <see cref="RootSchema"/>, <see cref="TypeFactory"/>, and <see cref="Config"/> properties.
    /// </remarks>
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
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string used to open the Calcite engine session, or <see langword="null"/> for an empty connection string. Recognized keys are described on <see cref="CalciteConnectionStringBuilder"/>.</param>
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
        /// <remarks>
        /// Calcite has no notion of a current database, so this property always returns <see cref="string.Empty"/>.
        /// </remarks>
        public override string Database => string.Empty;

        /// <inheritdoc />
        public override string DataSource => _options.Model ?? string.Empty;

        /// <inheritdoc />
        public override string ServerVersion => "Apache Calcite (ADO.NET)";

        /// <inheritdoc />
        public override ConnectionState State => _state;

        /// <inheritdoc />
        protected override DbProviderFactory DbProviderFactory => CalciteProviderFactory.Instance;

        /// <inheritdoc />
        /// <remarks>
        /// Not supported by Calcite. To change the default schema used to resolve unqualified
        /// identifiers, set the <c>Schema</c> connection-string property before opening the connection.
        /// </remarks>
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("Apache Calcite does not support changing the database on an open connection.");
        }

        /// <inheritdoc />
        public override void Open()
        {
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Connection is already open or in a transitional state.");

            SetState(ConnectionState.Connecting);
            try
            {
                var client = CalciteClientFactory.Create(_options);
                _session = new CalciteSession(_options, client);
                SetState(ConnectionState.Open);
            }
            catch
            {
                SetState(ConnectionState.Closed);
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
            SetState(ConnectionState.Closed);
        }

        void SetState(ConnectionState newState)
        {
            var oldState = _state;
            if (oldState == newState)
                return;

            _state = newState;
            OnStateChange(new StateChangeEventArgs(oldState, newState));
        }

        /// <inheritdoc />
        protected override DbCommand CreateDbCommand() => new CalciteCommand { Connection = this };

        /// <summary>
        /// Creates and returns a new <see cref="CalciteCommand"/> object associated with this connection.
        /// </summary>
        /// <returns>A new <see cref="CalciteCommand"/> whose <see cref="CalciteCommand.Connection"/> is set to this instance.</returns>
        public new CalciteCommand CreateCommand() => new() { Connection = this };

        /// <inheritdoc />
        public override bool CanCreateBatch => true;

        /// <inheritdoc />
        protected override DbBatch CreateDbBatch() => new CalciteBatch(this);

        /// <summary>
        /// Creates and returns a new <see cref="CalciteBatch"/> associated with this connection.
        /// </summary>
        /// <returns>A new <see cref="CalciteBatch"/> whose <see cref="CalciteBatch.Connection"/> is set to this instance.</returns>
        public new CalciteBatch CreateBatch() => new(this);

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

        /// <inheritdoc />
        public override DataTable GetSchema() => GetSchema(CalciteSchemaInfo.MetaDataCollections, null);

        /// <inheritdoc />
        public override DataTable GetSchema(string collectionName) => GetSchema(collectionName, null);

        /// <inheritdoc />
        public override DataTable GetSchema(string collectionName, string?[]? restrictionValues)
        {
            if (collectionName is null)
                throw new ArgumentNullException(nameof(collectionName));

            if (string.Equals(collectionName, CalciteSchemaInfo.MetaDataCollections, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildMetaDataCollections();

            if (string.Equals(collectionName, CalciteSchemaInfo.Restrictions, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildRestrictions();

            // The remaining collections require an open connection.
            RequireSession();

            if (string.Equals(collectionName, CalciteSchemaInfo.DataSourceInformation, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildDataSourceInformation(this);

            if (string.Equals(collectionName, CalciteSchemaInfo.DataTypes, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildDataTypes(this);

            if (string.Equals(collectionName, CalciteSchemaInfo.ReservedWords, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildReservedWords(this);

            throw new ArgumentException($"The metadata collection '{collectionName}' is not supported by this provider.", nameof(collectionName));
        }

        /// <summary>
        /// Gets the Calcite root <see cref="SchemaPlus"/> for the open connection. Use this to register
        /// schemas, tables, functions, or other Calcite-native artifacts that should be visible to
        /// statements executed on this connection.
        /// </summary>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        public SchemaPlus RootSchema => RequireSession().Client.RootSchema;

        /// <summary>
        /// Gets the Calcite <see cref="JavaTypeFactory"/> used by this connection's engine.
        /// </summary>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        public JavaTypeFactory TypeFactory => RequireSession().Client.TypeFactory;

        /// <summary>
        /// Gets the resolved <see cref="CalciteConnectionConfig"/> for this connection.
        /// </summary>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        public CalciteConnectionConfig Config => RequireSession().Client.Config;

        /// <summary>
        /// Returns the active session, throwing if the connection is not open.
        /// </summary>
        /// <returns>The current <see cref="CalciteSession"/> for this connection.</returns>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        internal CalciteSession RequireSession()
        {
            if (_state != ConnectionState.Open || _session is null)
                throw new InvalidOperationException("Connection is not open.");

            return _session;
        }

    }

}
