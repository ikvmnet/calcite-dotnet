using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using Apache.Calcite.Data.Internal;

using java.util.function;

using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.runtime;
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
    /// A connection is cheap and short-lived, as ADO.NET intends. The root schema it plans against —
    /// the model, the schemas the model built, the tables DDL has created — belongs to a
    /// <see cref="CalciteDataSource"/>, which outlives it: either one the application built and opened this
    /// connection from, or the one the provider keeps for this connection string, shared by every connection
    /// opened with an equivalent string. What the connection keeps to itself is created on the first call to
    /// <see cref="Open"/>, survives <see cref="Close"/>/<see cref="Open"/> cycles, and is released when the
    /// connection is disposed. <c>Pooling=false</c> in the connection string gives the connection a root of
    /// its own instead, built when it first opens and released with it.
    /// </para>
    /// </remarks>
    public sealed class CalciteConnection : DbConnection
    {

        CalciteConnectionStringBuilder _options = new();
        CalciteDataSource? _dataSource;
        CalciteSession? _session;
        ConnectionState _state = ConnectionState.Closed;
        bool _disposed;
        List<CalciteHookEntry>? _hooks;

        /// <summary>
        /// Registers a Calcite hook with a Java <see cref="Consumer"/> for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="consumer">The Java consumer invoked by the hook.</param>
        public void RegisterHook(org.apache.calcite.runtime.Hook hook, Consumer consumer)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, consumer));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="bool"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The boolean value to set on the hook property.</param>
        public void RegisterHook(Hook hook, bool value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Boolean.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with an <see cref="int"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The integer value to set on the hook property.</param>
        public void RegisterHook(Hook hook, int value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Integer.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="long"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The long value to set on the hook property.</param>
        public void RegisterHook(Hook hook, long value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Long.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="double"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The double value to set on the hook property.</param>
        public void RegisterHook(Hook hook, double value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Double.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="float"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The float value to set on the hook property.</param>
        public void RegisterHook(Hook hook, float value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Float.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="short"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The short value to set on the hook property.</param>
        public void RegisterHook(Hook hook, short value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Short.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a <see cref="byte"/> property value for the duration of every statement executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="value">The byte value to set on the hook property.</param>
        public void RegisterHook(Hook hook, byte value)
        {
            ThrowIfDisposed();
            (_hooks ??= []).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Byte.valueOf(value))));
        }

        /// <summary>
        /// Registers a Calcite hook with a .NET <see cref="Action{T}"/> callback for the duration of every statement executed on this connection.
        /// The action is wrapped in a Java consumer and invoked with the hook's argument on each execution.
        /// </summary>
        /// <param name="hook">The Calcite hook to activate.</param>
        /// <param name="function">The .NET delegate invoked by the hook.</param>
        public void RegisterHook(Hook hook, Action<object> function)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, new DelegateConsumer<object>(function)));
        }

        internal List<CalciteHookEntry>? Hooks => _hooks;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class with an empty connection string.
        /// </summary>
        /// <remarks>
        /// Set <see cref="ConnectionString"/> before calling <see cref="Open"/>.
        /// </remarks>
        public CalciteConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class bound to a data source.
        /// </summary>
        /// <param name="dataSource">The data source whose root schema this connection plans against.</param>
        internal CalciteConnection(CalciteDataSource dataSource)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _options = new CalciteConnectionStringBuilder(dataSource.ConnectionString);
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
        /// create a new <see cref="CalciteConnection"/>. Setting it on a connection created by a
        /// <see cref="CalciteDataSource"/> detaches the connection from that data source: it draws on the
        /// data source the provider keeps for the new string instead.
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
                _dataSource = null;
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
        /// <see cref="Open"/> calls. The first connection to open on a data source is the one that reads
        /// the model and builds its schemas; every connection after it finds them built. Closing and
        /// reopening the connection does not reset anything — a table created via DDL remains visible after
        /// reopening, as it does on every other connection of the same data source.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the connection is already open.</exception>
        /// <exception cref="CalciteException">Thrown when the Calcite engine could not be initialized.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when the data source the connection was created from has been disposed.</exception>
        public override void Open()
        {
            ThrowIfDisposed();
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Connection is already open or in a transitional state.");

            SetState(ConnectionState.Connecting);
            try
            {
                // Session is created once on the first Open() and reused across Close/Open cycles.
                if (_session is null)
                {
                    CalciteDataSourceRoot root;
                    bool owned;
                    if (_dataSource is not null)
                    {
                        (root, owned) = _dataSource.Acquire();
                    }
                    else
                    {
                        // the data source the provider keeps for this string, looked up again if it was
                        // pruned between the lookup and the use
                        CalciteDataSource dataSource;
                        do
                            dataSource = CalciteDataSources.Resolve(_options);
                        while (dataSource.TryAcquire(out root, out owned) == false);

                        _dataSource = dataSource;
                    }

                    _session = new CalciteSession(_options, root, owned);
                }

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
        /// the engine session is preserved so that calling <see cref="Open"/> again is inexpensive. To
        /// release what the connection holds, call <see cref="IDisposable.Dispose"/> instead. Neither
        /// touches the data source's root, which is shared and outlives the connection.
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
        /// Drops the data source the provider keeps for <paramref name="connection"/>'s connection string, so
        /// that the next connection opened with that string reads the model again.
        /// </summary>
        /// <param name="connection">A connection whose connection string names the data source to drop.</param>
        /// <remarks>
        /// This is how a changed model file reaches a running process before the data source's idle
        /// lifetime would have released it. Connections already open keep the root they have, and it is
        /// disposed once the last of them is. A data source the application built with
        /// <see cref="CalciteDataSourceBuilder"/> is not kept by the provider and is unaffected —
        /// <see cref="CalciteDataSource.Clear"/> is its equivalent.
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="connection"/> is <see langword="null"/>.</exception>
        public static void ClearPool(CalciteConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);
            CalciteDataSources.Clear(connection._options);
        }

        /// <summary>
        /// Drops every data source the provider keeps, so that the next connection opened with any
        /// connection string reads its model again.
        /// </summary>
        /// <remarks>
        /// As <see cref="ClearPool"/>, for every connection string at once.
        /// </remarks>
        public static void ClearAllPools()
        {
            CalciteDataSources.ClearAll();
        }

        /// <summary>
        /// Gets the root schema this connection plans against, read-only.
        /// </summary>
        /// <remarks>
        /// The root belongs to the connection's <see cref="CalciteDataSource"/> and is shared by every
        /// connection opened on it, so what is seen here is what every one of them sees: the schemas the
        /// model built, the schemas registered on the data source's builder, the tables DDL has created.
        /// It is handed out as a <see cref="Schema"/>, Calcite's read interface, because a change made
        /// through one connection would reach all of them without any having asked; the
        /// <see cref="SchemaPlus"/> Calcite adds to it is the data source's, reached through
        /// <see cref="CalciteDataSourceBuilder.ConfigureRootSchema"/>, and a table is created with DDL. This
        /// is a type and not a guard — the object is the root itself.
        /// </remarks>
        /// <exception cref="InvalidOperationException">Thrown when the connection is not open.</exception>
        public Schema RootSchema => RequireSession().RootSchema;

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
