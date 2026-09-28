using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using System.Collections.Immutable;

using Apache.Calcite.Data.Common;
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
    /// Create a connection with a connection string whose keys are described on
    /// <see cref="CalciteConnectionStringBuilder"/>, such as <c>Model</c> and <c>Schema</c>, or from a
    /// <see cref="CalciteDataSource"/>. Call <see cref="Open"/> before executing commands, and dispose the
    /// connection when done.
    /// </para>
    /// <para>
    /// The root schema a connection plans against (the schemas its model defines and the tables DDL creates)
    /// belongs to a <see cref="CalciteDataSource"/> and outlives the connection. A connection created from a
    /// data source uses that data source. A connection created from a connection string alone uses a data
    /// source the provider keeps for that connection string, shared by every connection opened with an
    /// equivalent string; see <see cref="ClearPool"/>. With <c>Pooling=false</c> in the connection string the
    /// connection instead builds a root of its own when it first opens and releases it when disposed.
    /// </para>
    /// <para>
    /// The connection's own state (its configuration, type factory and type mappings) is created on the
    /// first call to <see cref="Open"/>, kept across <see cref="Close"/> and <see cref="Open"/>, and released
    /// when the connection is disposed. Transactions are not supported.
    /// </para>
    /// <para>
    /// A connection is not thread-safe. Different connections on the same data source may be used
    /// concurrently.
    /// </para>
    /// </remarks>
    public sealed class CalciteConnection : DbConnection
    {

        CalciteConnectionStringBuilder _options = new();
        CalciteDataSource? _dataSource;
        ClrTypeMapper? _typeMapper;
        CalciteSession? _session;
        ConnectionState _state = ConnectionState.Closed;
        bool _disposed;
        List<CalciteHookEntry>? _hooks;

        /// <summary>
        /// Attaches a Java <see cref="Consumer"/> to a Calcite hook for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="consumer">The consumer the hook invokes with its argument.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// The hook is attached to the executing thread while a <see cref="CalciteCommand"/> on this connection
        /// plans its statement and opens its result, and detached before the execute method returns; it is not
        /// attached while rows are read. Connection hooks are attached before the command's own. A
        /// registration cannot be removed. Commands executed through a <see cref="CalciteBatch"/> do not
        /// attach hooks.
        /// </remarks>
        public void RegisterHook(org.apache.calcite.runtime.Hook hook, Consumer consumer)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, consumer));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="bool"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook, such as <see cref="Hook.ENABLE_BINDABLE"/>.</param>
        /// <param name="value">The value the hook supplies.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// The value is supplied through <c>Hook.propertyJ</c>. When the hook is attached is described on
        /// <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, bool value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Boolean.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to an <see cref="int"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Integer</c>.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, int value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Integer.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="long"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Long</c>.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, long value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Long.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="double"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Double</c>.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, double value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Double.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="float"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Float</c>.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, float value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Float.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="short"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Short</c>.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, short value)
        {
            ThrowIfDisposed();
            (_hooks ??= new List<CalciteHookEntry>()).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Short.valueOf(value))));
        }

        /// <summary>
        /// Sets a Calcite property hook to a <see cref="byte"/> value for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook.</param>
        /// <param name="value">The value the hook supplies, as a <c>java.lang.Byte</c>.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// When the hook is attached is described on <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
        public void RegisterHook(Hook hook, byte value)
        {
            ThrowIfDisposed();
            (_hooks ??= []).Add(new CalciteHookEntry(hook, Hook.propertyJ(java.lang.Byte.valueOf(value))));
        }

        /// <summary>
        /// Attaches a .NET callback to a Calcite hook for every command executed on this connection.
        /// </summary>
        /// <param name="hook">The Calcite hook, such as <see cref="Hook.PLAN_BEFORE_IMPLEMENTATION"/>.</param>
        /// <param name="function">The callback, invoked with the hook's argument.</param>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <remarks>
        /// The callback runs on the thread executing the command, and receives the hook's argument as Calcite
        /// passes it, which is usually a Java object. When the hook is attached is described on
        /// <see cref="RegisterHook(Hook, Consumer)"/>.
        /// </remarks>
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
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class that draws on a data source.
        /// </summary>
        /// <param name="dataSource">The data source whose root schema this connection plans against.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/> is <see langword="null"/>.</exception>
        internal CalciteConnection(CalciteDataSource dataSource)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _options = new CalciteConnectionStringBuilder(dataSource.ConnectionString);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnection"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string, or <see langword="null"/> for an empty one. Recognized keys are described on
        /// <see cref="CalciteConnectionStringBuilder"/>.
        /// </param>
        /// <exception cref="ArgumentException">The connection string is malformed.</exception>
        public CalciteConnection(string? connectionString)
        {
            ConnectionString = connectionString ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the connection string that configures this connection.
        /// </summary>
        /// <remarks>
        /// The connection string can be set only before the first call to <see cref="Open"/>; closing the
        /// connection does not make it settable again. To use different settings, create a new
        /// <see cref="CalciteConnection"/>. Setting it on a connection created from a
        /// <see cref="CalciteDataSource"/> detaches the connection from that data source, and the connection
        /// then draws on the data source the provider keeps for the new connection string.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection has been opened.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        /// <exception cref="ArgumentException">The connection string is malformed.</exception>
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
        /// Calcite has no current database, so this property always returns <see cref="string.Empty"/>.
        /// </remarks>
        public override string Database => string.Empty;

        /// <inheritdoc />
        /// <remarks>
        /// Returns the <c>Model</c> connection string value, or <see cref="string.Empty"/> where none is set.
        /// </remarks>
        public override string DataSource => _options.Model ?? string.Empty;

        /// <inheritdoc />
        /// <remarks>
        /// Returns a fixed product string; it does not carry a version number and does not require the
        /// connection to be open.
        /// </remarks>
        public override string ServerVersion => "Apache Calcite (ADO.NET)";

        /// <inheritdoc />
        public override ConnectionState State => _state;

        /// <inheritdoc />
        protected override DbProviderFactory DbProviderFactory => CalciteProviderFactory.Instance;

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="databaseName">Ignored.</param>
        /// <exception cref="NotSupportedException">Always.</exception>
        /// <remarks>
        /// To choose the schema that resolves unqualified identifiers, set the <c>Schema</c> connection string
        /// key before opening the connection.
        /// </remarks>
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("Apache Calcite does not support changing the database on an open connection.");
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The first call creates the connection's session over its data source's root schema. If no connection
        /// has opened on that root yet, this is also when the model is read and the schemas are built, which can
        /// take noticeable time and can fail with a <see cref="CalciteException"/>. Later calls, after
        /// <see cref="Close"/>, reuse the session and do no work.
        /// </para>
        /// <para>
        /// Opening is synchronous; <see cref="DbConnection.OpenAsync(System.Threading.CancellationToken)"/> is not
        /// overridden and runs this method.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection is already open.</exception>
        /// <exception cref="ObjectDisposedException">The connection, or the data source it was created from, has
        /// been disposed.</exception>
        /// <exception cref="CalciteException">The model could not be loaded or the session could not be
        /// initialized.</exception>
        public override void Open()
        {
            ThrowIfDisposed();
            if (_state != ConnectionState.Closed)
                throw new InvalidOperationException("Connection is already open or in a transitional state.");

            SetState(ConnectionState.Connecting);
            try
            {
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

                    _session = new CalciteSession(_options, root, owned, typeResolvers: SessionResolvers);
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
        /// Closes the connection.
        /// </summary>
        /// <remarks>
        /// Closing only sets <see cref="State"/> to <see cref="ConnectionState.Closed"/>. The connection keeps its
        /// session, so a later <see cref="Open"/> is immediate, and tables created by DDL remain. Dispose the
        /// connection to release the session. Calling <see cref="Close"/> on a closed connection does nothing.
        /// </remarks>
        public override void Close()
        {
            if (_state == ConnectionState.Closed)
                return;

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
        /// Creates a <see cref="CalciteCommand"/> associated with this connection.
        /// </summary>
        /// <returns>A new <see cref="CalciteCommand"/> whose <see cref="CalciteCommand.Connection"/> is this connection.</returns>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        public new CalciteCommand CreateCommand()
        {
            ThrowIfDisposed();
            return new CalciteCommand { Connection = this };
        }

        /// <inheritdoc />
        /// <remarks>
        /// Always <see langword="true"/>.
        /// </remarks>
        public override bool CanCreateBatch => true;

        /// <inheritdoc />
        protected override DbBatch CreateDbBatch()
        {
            ThrowIfDisposed();
            return new CalciteBatch(this);
        }

        /// <summary>
        /// Creates a <see cref="CalciteBatch"/> associated with this connection.
        /// </summary>
        /// <remarks>
        /// Add commands with <see cref="CalciteBatch.CreateBatchCommand"/> and
        /// <see cref="CalciteBatch.BatchCommands"/>. The batch executes its commands one after another on this
        /// connection.
        /// </remarks>
        /// <returns>A new <see cref="CalciteBatch"/> whose <see cref="CalciteBatch.Connection"/> is this connection.</returns>
        public new CalciteBatch CreateBatch() => new(this);

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="isolationLevel">Ignored.</param>
        /// <returns>Does not return.</returns>
        /// <exception cref="NotSupportedException">Always, unless the connection has been disposed.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            ThrowIfDisposed();
            throw new NotSupportedException("Transactions are not supported by Apache Calcite.");
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="transaction">Ignored.</param>
        /// <exception cref="NotSupportedException">Always, unless the connection has been disposed.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        public override void EnlistTransaction(System.Transactions.Transaction? transaction)
        {
            ThrowIfDisposed();
            throw new NotSupportedException("Transactions are not supported by Apache Calcite.");
        }

        /// <inheritdoc />
        /// <remarks>
        /// Closes the connection and releases its session. The root schema it planned against belongs to its
        /// data source and is not affected, except under <c>Pooling=false</c>, where the root was built for this
        /// connection and is released with it.
        /// </remarks>
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
        /// Returns the <c>MetaDataCollections</c> collection, which lists the metadata collections this
        /// provider supports.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> with one row per collection.</returns>
        /// <remarks>
        /// The collections are <c>MetaDataCollections</c>, <c>Restrictions</c>, <c>DataSourceInformation</c>,
        /// <c>DataTypes</c>, <c>ReservedWords</c>, <c>Tables</c> and <c>Columns</c>. This collection does not
        /// require an open connection.
        /// </remarks>
        public override DataTable GetSchema() => GetSchema(CalciteSchemaInfo.MetaDataCollections, null);

        /// <summary>
        /// Returns schema information for the specified metadata collection.
        /// </summary>
        /// <param name="collectionName">The name of the collection, such as <c>Tables</c> or <c>Columns</c>,
        /// matched ignoring case.</param>
        /// <returns>A <see cref="DataTable"/> containing the requested schema information.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="collectionName"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="collectionName"/> is not a supported collection.</exception>
        /// <exception cref="InvalidOperationException">The collection requires an open connection and the
        /// connection is not open.</exception>
        /// <remarks>
        /// See <see cref="GetSchema(string, string?[])"/>.
        /// </remarks>
        public override DataTable GetSchema(string collectionName) => GetSchema(collectionName, null);

        /// <summary>
        /// Returns schema information for the specified metadata collection, filtered by restriction values.
        /// </summary>
        /// <param name="collectionName">The name of the collection, such as <c>Tables</c> or <c>Columns</c>,
        /// matched ignoring case.</param>
        /// <param name="restrictionValues">
        /// Restriction values in the order the <c>Restrictions</c> collection lists them, or
        /// <see langword="null"/>. A <see langword="null"/> element applies no restriction.
        /// </param>
        /// <returns>A <see cref="DataTable"/> containing the requested schema information.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="collectionName"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="collectionName"/> is not a supported collection.</exception>
        /// <exception cref="InvalidOperationException">The collection requires an open connection and the
        /// connection is not open.</exception>
        /// <remarks>
        /// <para>
        /// <c>MetaDataCollections</c> and <c>Restrictions</c> can be read without opening the connection; every
        /// other collection requires it.
        /// </para>
        /// <para>
        /// <c>Tables</c> takes the restrictions catalog, schema, table and table type, and <c>Columns</c> takes
        /// catalog, schema, table and column. Both list the tables and views of the root schema's immediate
        /// sub-schemas. The catalog restriction is ignored, and the others are exact names compared ignoring
        /// case, not patterns. Describing a view requires Calcite to expand its SQL, so a listing that is not
        /// restricted by table name expands every view in the schemas it covers and fails if one of them no
        /// longer resolves.
        /// </para>
        /// </remarks>
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
        /// Releases the data source the provider keeps for a connection's connection string, so that the next
        /// connection opened with that string reads the model again.
        /// </summary>
        /// <param name="connection">A connection whose connection string identifies the data source.</param>
        /// <remarks>
        /// Use this to pick up a changed model file, or to discard tables created by DDL, without waiting for
        /// the idle lifetime to expire. Connections already open keep the root schema they have, and it is
        /// released once the last of them is disposed. A data source created by the application is not kept by
        /// the provider and is not affected; call <see cref="CalciteDataSource.Clear"/> on it instead.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="connection"/> is <see langword="null"/>.</exception>
        public static void ClearPool(CalciteConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);
            CalciteDataSources.Clear(connection._options);
        }

        /// <summary>
        /// Releases every data source the provider keeps, so that the next connection opened with any
        /// connection string reads its model again.
        /// </summary>
        /// <remarks>
        /// Behaves as <see cref="ClearPool"/> for every connection string at once.
        /// </remarks>
        public static void ClearAllPools()
        {
            CalciteDataSources.ClearAll();
        }

        /// <summary>
        /// Gets the root schema this connection plans against.
        /// </summary>
        /// <remarks>
        /// The root belongs to the connection's <see cref="CalciteDataSource"/> and is shared by every connection
        /// on it: it holds the schemas the model defines, the schemas and steps registered through
        /// <see cref="CalciteDataSourceBuilder"/>, and the tables DDL has created. It is exposed as Calcite's
        /// read interface, <see cref="Schema"/>, because a change made through one connection would affect all
        /// of them. To add to the root, use <see cref="CalciteDataSourceBuilder.ConfigureRootSchema"/> or DDL.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        public Schema RootSchema => RequireSession().RootSchema;

        /// <summary>
        /// Gets the <see cref="JavaTypeFactory"/> this connection's statements are planned with.
        /// </summary>
        /// <remarks>
        /// Use it to construct Calcite <c>RelDataType</c> instances, for example when declaring the row type
        /// of a table or the signature of a function. Each connection has its own type factory.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        public JavaTypeFactory TypeFactory => RequireSession().TypeFactory;

        /// <summary>
        /// Gets the chain of type resolvers that decides which .NET types this connection reads columns as and
        /// accepts parameters as.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A resolver added to the front of the chain decides first which .NET type a Calcite type is presented
        /// as and how values convert in both directions. The chain starts as a copy of the data source's
        /// <see cref="CalciteDataSource.TypeResolvers"/>, so a resolver added here affects this connection only;
        /// register a mapping that belongs to the data on the data source instead.
        /// </para>
        /// <para>
        /// The chain is read once, when the connection first opens, and bound to the connection's type factory.
        /// It can therefore only be obtained before the first call to <see cref="Open"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection has been opened.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
        public ClrTypeMapper TypeMapper
        {
            get
            {
                ThrowIfDisposed();

                // a resolver registered after the session has read the chain would never run, so the chain
                // is not handed out once there is a session
                if (_session is not null)
                    throw new InvalidOperationException(
                        "The type mappings cannot be changed after the connection has been opened. " +
                        "The session reads the chain once, when it opens, because what a Calcite type is held in " +
                        "is its type factory's answer and the mappings are bound to it. " +
                        "Register the resolver before Open, or on the CalciteDataSource every connection is drawn on.");

                return _typeMapper ??= new ClrTypeMapper(Inherited);
            }
        }

        /// <summary>
        /// The built-in chain, used by a connection with no data source.
        /// </summary>
        static readonly ImmutableArray<IClrTypeResolver> DefaultResolvers = new ClrTypeMapper().Resolvers;

        /// <summary>
        /// Gets the chain this connection starts from: its data source's, or the built-in one.
        /// </summary>
        ImmutableArray<IClrTypeResolver> Inherited => _dataSource?.TypeResolvers ?? DefaultResolvers;

        /// <summary>
        /// Gets the chain the session binds: this connection's own mapper where <see cref="TypeMapper"/> was
        /// read, otherwise the inherited chain unchanged.
        /// </summary>
        ImmutableArray<IClrTypeResolver> SessionResolvers => _typeMapper?.Resolvers ?? Inherited;

        /// <summary>
        /// Gets the Calcite configuration this connection's statements are planned with.
        /// </summary>
        /// <remarks>
        /// The effective settings derived from the connection string, such as the lexical policy, the
        /// conformance level and the null collation, with Calcite's defaults for keys not set.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The connection is not open.</exception>
        /// <exception cref="ObjectDisposedException">The connection has been disposed.</exception>
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
