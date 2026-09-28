using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

using System.Collections.Immutable;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a source of <see cref="CalciteConnection"/> objects that share one Apache Calcite root
    /// schema.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The data source holds the root schema: the schemas the model defines, any schemas and steps added
    /// through <see cref="CalciteDataSourceBuilder"/>, and every table, view or schema DDL creates. The root is
    /// built once, when the first connection opens, and every connection the data source produces plans
    /// against it. A table created by DDL on one connection is therefore visible on the others. Each
    /// connection keeps its own configuration and type factory.
    /// </para>
    /// <para>
    /// Create one data source per logical database and share it for the life of the application, for
    /// example as a singleton service, with connections created and disposed per unit of work. A data source
    /// is created with a constructor or with <see cref="CalciteDataSourceBuilder"/>, which also accepts schema
    /// instances and configuration steps a connection string cannot carry; either way the application owns
    /// it and disposes it. A connection created from a connection string alone, without a data source, draws
    /// on a data source the provider keeps for that string, shared by every connection opened with an
    /// equivalent string and released once no connection has been open on it for
    /// <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/> seconds.
    /// </para>
    /// <para>
    /// With <c>Pooling=false</c> in the connection string, each connection builds a root of its own when it
    /// first opens and releases it when disposed, and nothing is shared.
    /// </para>
    /// <para>
    /// Because connections share the root, a schema reachable from a data source may be read from several
    /// threads at once and must tolerate concurrent reads. Statements plan under a shared lock on the root
    /// and DDL runs under an exclusive one, so no statement plans against a root that DDL is changing.
    /// Execution does not hold the lock.
    /// </para>
    /// <para>
    /// Every schema on the root that implements <see cref="IDisposable"/> is disposed when the root is
    /// released: after <see cref="Clear"/> or <see cref="DbDataSource.Dispose()"/>, once the last connection
    /// open on that root has been disposed.
    /// </para>
    /// </remarks>
    public class CalciteDataSource : DbDataSource
    {

        /// <summary>
        /// The <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>, in seconds, used where the
        /// connection string does not set one.
        /// </summary>
        public const int DefaultConnectionIdleLifetime = 300;

        /// <summary>
        /// The <see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/>, in seconds, used where the
        /// connection string does not set one.
        /// </summary>
        public const int DefaultConnectionPruningInterval = 10;

        readonly CalciteConnectionStringBuilder _options;
        readonly IReadOnlyList<Action<org.apache.calcite.schema.SchemaPlus>> _configure;
        readonly bool _pooling;
        readonly TimeSpan _idleLifetime;
        readonly TimeSpan _pruningInterval;
        readonly ImmutableArray<IClrTypeResolver> _typeResolvers;
        readonly long _created = Environment.TickCount64;
        readonly object _sync = new();
        CalciteDataSourceRoot? _root;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string used by every connection produced from this data source.
        /// Recognized keys are described on <see cref="CalciteConnectionStringBuilder"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">The connection string is malformed, or
        /// <see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/> is not positive or exceeds
        /// <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>.</exception>
        /// <remarks>
        /// Nothing is read or built until the first connection opens.
        /// </remarks>
        public CalciteDataSource(string connectionString) :
            this(new CalciteConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), [])
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class using the specified builder.
        /// </summary>
        /// <param name="connectionStringBuilder">The builder whose <see cref="DbConnectionStringBuilder.ConnectionString"/>
        /// configures every connection produced. It is copied; later changes to it have no effect.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionStringBuilder"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/>
        /// is not positive or exceeds <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>.</exception>
        public CalciteDataSource(CalciteConnectionStringBuilder connectionStringBuilder) :
            this(new CalciteConnectionStringBuilder((connectionStringBuilder ?? throw new ArgumentNullException(nameof(connectionStringBuilder))).ConnectionString), [])
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options">The connection string, which this instance owns.</param>
        /// <param name="configure">Steps to run over the root after the model, in order.</param>
        /// <param name="pooled">Whether the root is shared, or <see langword="null"/> to read the
        /// <c>Pooling</c> key.</param>
        /// <param name="typeMapper">The type-mapping chain every connection starts from, or
        /// <see langword="null"/> for the built-in one. Its current resolvers are captured.</param>
        /// <exception cref="ArgumentException">The pooling settings are not valid.</exception>
        internal CalciteDataSource(CalciteConnectionStringBuilder options, IReadOnlyList<Action<org.apache.calcite.schema.SchemaPlus>> configure, bool? pooled = null, ClrTypeMapper? typeMapper = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _configure = configure ?? throw new ArgumentNullException(nameof(configure));
            _pooling = pooled ?? options.Pooling ?? true;

            var idleLifetime = options.ConnectionIdleLifetime ?? DefaultConnectionIdleLifetime;
            var pruningInterval = options.ConnectionPruningInterval ?? DefaultConnectionPruningInterval;
            if (pruningInterval <= 0)
                throw new ArgumentException($"{CalciteConnectionStringBuilder.ConnectionPruningIntervalKey} can't be 0.", nameof(options));
            if (idleLifetime < pruningInterval)
                throw new ArgumentException($"Connection can't have {CalciteConnectionStringBuilder.ConnectionIdleLifetimeKey} {idleLifetime} under {CalciteConnectionStringBuilder.ConnectionPruningIntervalKey} {pruningInterval}.", nameof(options));

            _idleLifetime = TimeSpan.FromSeconds(idleLifetime);
            _pruningInterval = TimeSpan.FromSeconds(pruningInterval);
            // captured once and immutable: a chain that changed after some connections had bound it would
            // leave connections on one data source reading the same column differently
            _typeResolvers = (typeMapper ?? new ClrTypeMapper()).Resolvers;
        }

        /// <summary>
        /// Gets the type resolvers every connection from this data source starts with, in the order they are
        /// consulted.
        /// </summary>
        /// <remarks>
        /// Fixed when the data source is created; configure it with
        /// <see cref="CalciteDataSourceBuilder.TypeMapper"/>. A connection's
        /// <see cref="CalciteConnection.TypeMapper"/> starts as a copy of this chain, so a resolver added there
        /// affects that connection only.
        /// </remarks>
        public ImmutableArray<IClrTypeResolver> TypeResolvers => _typeResolvers;

        /// <inheritdoc />
        public override string ConnectionString => _options.ConnectionString;

        /// <summary>
        /// Gets how often the provider checks this data source for pruning, where the provider keeps it.
        /// </summary>
        internal TimeSpan PruningInterval => _pruningInterval;

        /// <summary>
        /// Gets the root a connection should plan against, and whether that connection owns it.
        /// </summary>
        /// <returns>The root, and <see langword="true"/> where it was built for this caller alone and is the
        /// caller's to retire.</returns>
        /// <exception cref="ObjectDisposedException">The data source has been disposed.</exception>
        /// <exception cref="CalciteException">The root could not be built.</exception>
        /// <remarks>
        /// The shared root is built under a lock by the first caller, and concurrent callers wait for it. A
        /// failed build leaves nothing behind, so the next caller tries again. With <c>Pooling=false</c> every
        /// call builds a root, which the caller owns and retires.
        /// </remarks>
        internal (CalciteDataSourceRoot Root, bool Owned) Acquire()
        {
            return TryAcquire(out var root, out var owned) ? (root, owned) : throw new ObjectDisposedException(GetType().Name);
        }

        /// <summary>
        /// As <see cref="Acquire"/>, but answers <see langword="false"/> rather than throwing where the data
        /// source has been disposed. For a data source the provider keeps, that means it was pruned after
        /// being looked up, and the caller looks it up again.
        /// </summary>
        /// <exception cref="CalciteException">The root could not be built.</exception>
        internal bool TryAcquire(out CalciteDataSourceRoot root, out bool owned)
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    root = null!;
                    owned = false;
                    return false;
                }

                if (_pooling == false)
                {
                    root = CalciteDataSourceRoot.Build(_options, _configure);
                    owned = true;
                    return true;
                }

                _root ??= CalciteDataSourceRoot.Build(_options, _configure);
                root = _root;
                owned = false;
                return true;
            }
        }

        /// <summary>
        /// Marks this data source disposed and retires its root where no connection has been open on it for
        /// its idle lifetime.
        /// </summary>
        /// <param name="now">The current <see cref="Environment.TickCount64"/>.</param>
        /// <returns>Whether the data source is now disposed.</returns>
        internal bool TryPrune(long now)
        {
            CalciteDataSourceRoot? root;
            lock (_sync)
            {
                if (_disposed)
                    return true;

                if (_root is not null && _root.Users > 0)
                    return false;

                var idleSince = _root?.IdleSince ?? _created;
                if (now - idleSince < _idleLifetime.TotalMilliseconds)
                    return false;

                _disposed = true;
                root = _root;
                _root = null;
            }

            root?.Retire();
            return true;
        }

        /// <summary>
        /// Drops the root schema, so that the next connection to open builds a new one and reads the model
        /// again.
        /// </summary>
        /// <remarks>
        /// Use this to pick up a changed model file, or to discard tables created by DDL. Connections already
        /// open keep the old root, which is released once the last of them is disposed. Has no effect on a
        /// data source with <c>Pooling=false</c>, whose connections each own their root.
        /// </remarks>
        public void Clear()
        {
            CalciteDataSourceRoot? root;
            lock (_sync)
            {
                root = _root;
                _root = null;
            }

            root?.Retire();
        }

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection() => new CalciteConnection(this);

        /// <summary>
        /// Creates a new, closed <see cref="CalciteConnection"/> that draws on this data source.
        /// </summary>
        /// <returns>A new <see cref="CalciteConnection"/>.</returns>
        public new CalciteConnection CreateConnection() => (CalciteConnection)base.CreateConnection();

        /// <summary>
        /// Creates and opens a new <see cref="CalciteConnection"/> that draws on this data source.
        /// </summary>
        /// <returns>An open <see cref="CalciteConnection"/>.</returns>
        /// <exception cref="ObjectDisposedException">The data source has been disposed.</exception>
        /// <exception cref="CalciteException">The root schema or the connection could not be initialized.</exception>
        public new CalciteConnection OpenConnection() => (CalciteConnection)base.OpenConnection();

        /// <summary>
        /// Creates and opens a new <see cref="CalciteConnection"/> that draws on this data source.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task whose result is an open <see cref="CalciteConnection"/>.</returns>
        /// <exception cref="ObjectDisposedException">The data source has been disposed.</exception>
        /// <exception cref="CalciteException">The root schema or the connection could not be initialized.</exception>
        /// <remarks>
        /// The connection opens synchronously; <see cref="CalciteConnection"/> does not override
        /// <see cref="DbConnection.OpenAsync(CancellationToken)"/>.
        /// </remarks>
        public new async ValueTask<CalciteConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            return (CalciteConnection)await base.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        /// <remarks>
        /// No new connection can be opened from a disposed data source. Connections already open keep
        /// working, and the root's disposable schemas are disposed once the last of them is disposed.
        /// </remarks>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                CalciteDataSourceRoot? root;
                lock (_sync)
                {
                    if (_disposed)
                        return;

                    _disposed = true;
                    root = _root;
                    _root = null;
                }

                root?.Retire();
            }

            base.Dispose(disposing);
        }

    }

}
