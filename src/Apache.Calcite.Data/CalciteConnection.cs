using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using Apache.Calcite.Data.Internal;

using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a connection to an in-process Apache Calcite engine. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Create a connection with a connection string whose keys match those on
    /// <see cref="CalciteConnectionStringBuilder"/> — for example <c>Model</c> and <c>Schema</c>.
    /// Call <see cref="Open"/> before executing commands, and <see cref="IDisposable.Dispose"/> when
    /// done to release all engine resources.
    /// </para>
    /// <para>
    /// The underlying Calcite session is created on the first call to <see cref="Open"/> and is kept
    /// alive across <see cref="Close"/>/<see cref="Open"/> cycles. Schema objects registered on
    /// <see cref="RootSchema"/>, tables created via DDL, and any other in-process state survive
    /// a close and remain visible after reopening. The session is torn down permanently only when
    /// the connection is disposed.
    /// </para>
    /// </remarks>
    public sealed class CalciteConnection : DbConnection
    {

        CalciteConnectionStringBuilder _options = new();
        CalciteSession? _session;
        ConnectionState _state = ConnectionState.Closed;
        bool _disposed;
        Func<org.apache.calcite.jdbc.CalcitePrepare>? _prepareFactory;
        List<CalciteHookEntry>? _hooks;

        /// <summary>
        /// Gets or sets a factory that supplies the <see cref="org.apache.calcite.jdbc.CalcitePrepare"/>
        /// instance used to plan and compile each query.
        /// </summary>
        /// <remarks>
        /// Set this before calling <see cref="Open"/> to substitute a custom planner implementation.
        /// When <see langword="null"/> (the default), Calcite's built-in planner is used.
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the property is set after the connection has already been opened.
        /// </exception>
        public Func<org.apache.calcite.jdbc.CalcitePrepare>? PrepareFactory
        {
            get => _prepareFactory;
            set
            {
                ThrowIfDisposed();
                if (_session is not null)
                    throw new InvalidOperationException("PrepareFactory cannot be changed after the connection has been opened.");

                _prepareFactory = value;
            }
        }

        /// <summary>
        /// Registers a Calcite hook that will be activated for every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook constant to activate, for example <c>Hook.ENABLE_BINDABLE</c>.</param>
        /// <param name="value">
        /// The value to supply to the hook while the statement is being planned. CLR primitives
        /// (<see cref="bool"/>, <see cref="int"/>, <see cref="long"/>, <see cref="double"/>, etc.)
        /// are automatically converted to their Java boxed equivalents; values that are already
        /// Java objects are passed through unchanged.
        /// </param>
        /// <remarks>
        /// Hooks registered here apply to all commands created from this connection. Connection-level
        /// hooks are always activated before any hooks added to an individual command via
        /// <see cref="CalciteCommand.RegisterHook"/>. Each hook is activated on the current thread
        /// before any part of statement execution begins and torn down automatically when execution completes.
        /// </remarks>
        public void RegisterHook(org.apache.calcite.runtime.Hook hook, object? value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, value));
        }

        internal List<CalciteHookEntry>? Hooks => _hooks;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class with an empty connection string.
        /// </summary>
        /// <remarks>
        /// Set <see cref="ConnectionString"/> before calling <see cref="Open"/>, or register schemas
        /// directly on <see cref="RootSchema"/> after opening.
        /// </remarks>
        public CalciteConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string that configures the Calcite engine session, or <see langword="null"/> for
        /// an empty connection string. Recognized keys are documented on <see cref="CalciteConnectionStringBuilder"/>.
        /// </param>
        public CalciteConnection(string? connectionString)
        {
            ConnectionString = connectionString ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the connection string that configures the Calcite engine session.
        /// </summary>
        /// <remarks>
        /// The connection string must be set <em>before</em> calling <see cref="Open"/> for the first
        /// time. Once the session has been started it cannot be changed — the Calcite engine is
        /// initialized once and reused for the lifetime of the connection. To use different settings,
        /// create a new <see cref="CalciteConnection"/>.
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the connection string is set after the connection has already been opened.
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
        /// Opens the connection, initializing the Calcite engine session if this is the first call.
        /// </summary>
        /// <remarks>
        /// The Calcite session is created once on the first call and reused on all subsequent
        /// <see cref="Open"/> calls. Closing and reopening the connection does not reset the engine —
        /// any schemas registered on <see cref="RootSchema"/> or tables created via DDL remain visible
        /// after reopening.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the connection is already open.</exception>
        /// <exception cref="CalciteException">Thrown when the Calcite engine could not be initialized.</exception>
        public override void Open()
        {
            ThrowIfDisposed();
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Connection is already open or in a transitional state.");

            SetState(ConnectionState.Connecting);
            try
            {
                // Session is created once on the first Open() and reused across Close/Open cycles.
                _session ??= new CalciteSession(_options, _prepareFactory);
                SetState(ConnectionState.Open);
            }
            catch
            {
                SetState(ConnectionState.Closed);
                throw;
            }
        }

        /// <summary>
        /// Closes the connection without destroying the underlying Calcite session.
        /// </summary>
        /// <remarks>
        /// Closing the connection only changes its state to <see cref="System.Data.ConnectionState.Closed"/>;
        /// the engine session is preserved so that calling <see cref="Open"/> again is inexpensive and
        /// retains all in-process state. To fully release engine resources, call
        /// <see cref="IDisposable.Dispose"/> instead.
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
        /// Creates a new <see cref="CalciteCommand"/> associated with this connection.
        /// </summary>
        /// <returns>A new <see cref="CalciteCommand"/> associated with this connection.</returns>
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
        /// Creates a new <see cref="CalciteBatch"/> associated with this connection.
        /// </summary>
        /// <remarks>
        /// A batch lets you send multiple SQL statements in a single round-trip to the engine.
        /// Add commands via <see cref="CalciteBatch.CreateBatchCommand"/> and execute the batch
        /// by calling <see cref="CalciteBatch.ExecuteNonQuery"/>.
        /// </remarks>
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

        /// <summary>
        /// Returns a <see cref="DataTable"/> listing the metadata collections supported by this provider.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> describing the available schema collections.</returns>
        public override DataTable GetSchema() => GetSchema(CalciteSchemaInfo.MetaDataCollections, null);

        /// <summary>
        /// Returns a <see cref="DataTable"/> containing schema information for the specified collection.
        /// </summary>
        /// <param name="collectionName">The name of the metadata collection to retrieve, such as <c>Tables</c> or <c>Columns</c>.</param>
        /// <returns>A <see cref="DataTable"/> containing the requested schema information.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="collectionName"/> is not supported by this provider.</exception>
        public override DataTable GetSchema(string collectionName) => GetSchema(collectionName, null);

        /// <summary>
        /// Returns a <see cref="DataTable"/> containing schema information for the specified collection,
        /// filtered by the supplied restriction values.
        /// </summary>
        /// <param name="collectionName">The name of the metadata collection to retrieve, such as <c>Tables</c> or <c>Columns</c>.</param>
        /// <param name="restrictionValues">
        /// An ordered array of restriction values that narrow the results, or <see langword="null"/> to return all rows.
        /// The number and meaning of restrictions for each collection are described by <see cref="GetSchema()"/>.
        /// </param>
        /// <returns>A <see cref="DataTable"/> containing the requested schema information.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="collectionName"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="collectionName"/> is not supported by this provider.</exception>
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
        /// Gets the root <see cref="SchemaPlus"/> for this connection's Calcite engine.
        /// </summary>
        /// <remarks>
        /// Use this to register schemas, tables, custom functions, or other Calcite artifacts that
        /// should be visible to SQL statements executed on this connection. Objects added here
        /// persist for the lifetime of the session, including across <see cref="Close"/>/<see cref="Open"/> cycles.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public SchemaPlus RootSchema => RequireSession().RootSchema;

        /// <summary>
        /// Gets the <see cref="JavaTypeFactory"/> used by this connection's Calcite engine.
        /// </summary>
        /// <remarks>
        /// The type factory translates between .NET and Calcite's internal type system. It is
        /// needed when constructing custom <see cref="SchemaPlus"/> table types or Calcite functions
        /// that must declare their SQL types programmatically.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public JavaTypeFactory TypeFactory => RequireSession().TypeFactory;

        /// <summary>
        /// Gets the resolved <see cref="CalciteConnectionConfig"/> for this connection.
        /// </summary>
        /// <remarks>
        /// Exposes the effective Calcite configuration derived from the connection string, such as
        /// the lexical policy, conformance level, and null collation. This is the same configuration
        /// object that the Calcite planner uses internally.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public CalciteConnectionConfig Config => RequireSession().Config;

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        internal CalciteSession RequireSession()
        {
            ThrowIfDisposed();
            if (_state != ConnectionState.Open || _session is null)
                throw new InvalidOperationException("Connection is not open.");

            return _session;
        }

    }

}
