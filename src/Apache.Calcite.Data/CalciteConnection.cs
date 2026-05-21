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
    /// <para>
    /// The underlying Calcite session is created on the first call to <see cref="Open"/> and is
    /// intentionally kept alive across <see cref="Close"/>/<see cref="Open"/> cycles. This means
    /// that schema objects registered on <see cref="RootSchema"/>, tables created via DDL, and any
    /// other in-process state survive a close and are still visible after the connection is reopened.
    /// The session is torn down permanently only when the connection is disposed.
    /// </para>
    /// </remarks>
    public sealed class CalciteConnection : DbConnection
    {

        CalciteConnectionStringBuilder _options = new();
        CalciteSession? _session;
        ConnectionState _state = ConnectionState.Closed;
        bool _disposed;

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

        /// <summary>
        /// Gets or sets the connection string used to configure the Calcite engine session.
        /// </summary>
        /// <remarks>
        /// Unlike most ADO.NET providers, the connection string can only be set <em>before</em> the
        /// first call to <see cref="Open"/>. Once the session has been created it is reused for the
        /// lifetime of this instance, so changing the connection string after that point would have no
        /// effect. Attempting to do so throws an <see cref="InvalidOperationException"/>.
        /// <para>
        /// To use a different connection string, create a new <see cref="CalciteConnection"/> instance.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// The connection has already been opened. The connection string is fixed for the lifetime of
        /// the connection once <see cref="Open"/> has been called.
        /// </exception>
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString
        {
            get => _options.ConnectionString;
            set
            {
                ThrowIfDisposed();
                if (_session is not null)
                    throw new InvalidOperationException(
                        "The connection string cannot be changed after the connection has been opened. " +
                        "The Calcite session is fixed for the lifetime of this instance. " +
                        "To use a different connection string, create a new CalciteConnection.");

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

        /// <summary>
        /// Opens the connection and makes it ready to execute commands.
        /// </summary>
        /// <remarks>
        /// The underlying Calcite session is created the first time this method is called and is
        /// reused on subsequent calls. Closing and reopening the connection does not discard the
        /// session — any schema mutations made while the connection was open (for example DDL
        /// executed via <see cref="CalciteCommand"/>) remain visible after reopening.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection is already open.</exception>
        /// <exception cref="CalciteException">The Calcite session could not be initialized.</exception>
        public override void Open()
        {
            ThrowIfDisposed();
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Connection is already open or in a transitional state.");

            SetState(ConnectionState.Connecting);
            try
            {
                // Session is created once on the first Open() and reused across Close/Open cycles.
                _session ??= new CalciteSession(_options);
                SetState(ConnectionState.Open);
            }
            catch
            {
                SetState(ConnectionState.Closed);
                throw;
            }
        }

        /// <summary>
        /// Closes the connection. The underlying Calcite session is kept alive and will be reused
        /// if the connection is reopened.
        /// </summary>
        /// <remarks>
        /// To permanently release all Calcite resources, dispose the connection via
        /// <see cref="IDisposable.Dispose"/> rather than calling <see cref="Close"/>.
        /// </remarks>
        public override void Close()
        {
            if (_state == ConnectionState.Closed)
                return;

            // Session is intentionally kept alive; it will be reused on the next Open().
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
        protected override DbCommand CreateDbCommand()
        {
            ThrowIfDisposed();
            return new CalciteCommand { Connection = this };
        }

        /// <summary>
        /// Creates and returns a new <see cref="CalciteCommand"/> object associated with this connection.
        /// </summary>
        /// <returns>A new <see cref="CalciteCommand"/> whose <see cref="CalciteCommand.Connection"/> is set to this instance.</returns>
        public new CalciteCommand CreateCommand()
        {
            ThrowIfDisposed();
            return new CalciteCommand { Connection = this };
        }

        /// <inheritdoc />
        public override bool CanCreateBatch => true;

        /// <inheritdoc />
        protected override DbBatch CreateDbBatch()
        {
            ThrowIfDisposed();
            return new CalciteBatch(this);
        }

        /// <summary>
        /// Creates and returns a new <see cref="CalciteBatch"/> associated with this connection.
        /// </summary>
        /// <returns>A new <see cref="CalciteBatch"/> whose <see cref="CalciteBatch.Connection"/> is set to this instance.</returns>
        public new CalciteBatch CreateBatch() => new(this);

        /// <inheritdoc />
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            ThrowIfDisposed();
            throw new NotSupportedException("Transactions are not supported by Apache Calcite.");
        }

        /// <inheritdoc />
        public override void EnlistTransaction(System.Transactions.Transaction? transaction)
        {
            ThrowIfDisposed();
            throw new NotSupportedException("Transactions are not supported by Apache Calcite.");
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _disposed = true;
                Close();
                _session?.Dispose();
                _session = null;
            }

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

            if (string.Equals(collectionName, CalciteSchemaInfo.Tables, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildTables(this, restrictionValues);

            if (string.Equals(collectionName, CalciteSchemaInfo.Columns, StringComparison.OrdinalIgnoreCase))
                return CalciteSchemaInfo.BuildColumns(this, restrictionValues);

            throw new ArgumentException($"The metadata collection '{collectionName}' is not supported by this provider.", nameof(collectionName));
        }

        /// <summary>
        /// Gets the Calcite root <see cref="SchemaPlus"/> for the open connection. Use this to register
        /// schemas, tables, functions, or other Calcite-native artifacts that should be visible to
        /// statements executed on this connection.
        /// </summary>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        public SchemaPlus RootSchema => RequireSession().RootSchema;

        /// <summary>
        /// Gets the Calcite <see cref="JavaTypeFactory"/> used by this connection's engine.
        /// </summary>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        public JavaTypeFactory TypeFactory => RequireSession().TypeFactory;

        /// <summary>
        /// Gets the resolved <see cref="CalciteConnectionConfig"/> for this connection.
        /// </summary>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        public CalciteConnectionConfig Config => RequireSession().Config;

        /// <summary>
        /// Returns the active session, throwing if the connection is not open.
        /// </summary>
        /// <returns>The current <see cref="CalciteSession"/> for this connection.</returns>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        /// <summary>
        /// Returns the active session, throwing if the connection is not open.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        internal CalciteSession RequireSession()
        {
            ThrowIfDisposed();
            if (_state != ConnectionState.Open || _session is null)
                throw new InvalidOperationException("Connection is not open.");

            return _session;
        }

    }

}
