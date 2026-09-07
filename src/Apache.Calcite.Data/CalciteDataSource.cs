using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;
using Apache.Calcite.Data.Types;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a source of <see cref="CalciteConnection"/> instances sharing one root schema. This is the
    /// Apache Calcite implementation of the .NET 7+ <see cref="DbDataSource"/> pattern.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A Calcite connection is meant to be long-lived — it owns the root schema and everything an adapter's
    /// schema has learnt — and an ADO.NET connection is not. The data source is where the long-lived half
    /// goes. It reads the model and builds the schemas once, on the first connection to open, and every
    /// connection it produces plans against that root; what a connection keeps to itself is its
    /// configuration, its type factory and the convention it plans into. A table created by DDL on one
    /// connection is therefore visible on the next, as it is in any database, and an adapter that discovers
    /// something at schema-build time discovers it once.
    /// </para>
    /// <para>
    /// A <see cref="CalciteDataSource"/> is intended to be created once per logical data source and shared
    /// across the application — registered as a singleton, with connections transient. There are two ways
    /// to one. A bare <c>new CalciteConnection(connectionString)</c> draws on a data source the provider
    /// keeps for that connection string, made the first time the string is seen, shared by every
    /// connection opened with an equivalent string afterwards, and released once it has gone
    /// <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/> with no connection open on it;
    /// <c>Pooling=false</c> in the string opts a connection out, giving it a root of its own. Or the
    /// application builds one with <see cref="CalciteDataSourceBuilder"/>, which is the only way to hand
    /// over a schema instance the application constructed, and which is the application's to dispose.
    /// </para>
    /// <para>
    /// Sharing a root means an adapter's schema may be read from several threads at once. Calcite serialises
    /// nothing; a schema reachable from a data source has to tolerate concurrent reads. Planning takes the
    /// root's read lock and DDL its write lock, so a statement never plans against a root another is
    /// altering. Disposing the data source retires its root: every schema on it that implements
    /// <see cref="IDisposable"/> is disposed once the last connection open on it is disposed, and no new
    /// connection can be opened from it.
    /// </para>
    /// </remarks>
    public class CalciteDataSource : DbDataSource
    {

        /// <summary>
        /// The <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/> where the string gives none.
        /// </summary>
        public const int DefaultConnectionIdleLifetime = 300;

        /// <summary>
        /// The <see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/> where the string gives none.
        /// </summary>
        public const int DefaultConnectionPruningInterval = 10;

        readonly CalciteConnectionStringBuilder _options;
        readonly IReadOnlyList<Action<org.apache.calcite.schema.SchemaPlus>> _configure;
        readonly ClrTypeMapper _typeMapper;
        readonly bool _pooling;
        readonly TimeSpan _idleLifetime;
        readonly TimeSpan _pruningInterval;
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
        /// <exception cref="ArgumentException">The connection string's pooling settings are not valid.</exception>
        public CalciteDataSource(string connectionString) :
            this(new CalciteConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), [])
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class using the specified builder.
        /// </summary>
        /// <param name="connectionStringBuilder">The builder whose <see cref="DbConnectionStringBuilder.ConnectionString"/> is used to configure produced connections.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionStringBuilder"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">The connection string's pooling settings are not valid.</exception>
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
        /// <exception cref="ArgumentException">The pooling settings are not valid.</exception>
        internal CalciteDataSource(CalciteConnectionStringBuilder options, IReadOnlyList<Action<org.apache.calcite.schema.SchemaPlus>> configure, bool? pooled = null, ClrTypeMapper? typeMapper = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _configure = configure ?? throw new ArgumentNullException(nameof(configure));
            _typeMapper = typeMapper ?? new ClrTypeMapper();
            _pooling = pooled ?? options.Pooling ?? true;

            var idleLifetime = options.ConnectionIdleLifetime ?? DefaultConnectionIdleLifetime;
            var pruningInterval = options.ConnectionPruningInterval ?? DefaultConnectionPruningInterval;
            if (pruningInterval <= 0)
                throw new ArgumentException($"{CalciteConnectionStringBuilder.ConnectionPruningIntervalKey} can't be 0.", nameof(options));
            if (idleLifetime < pruningInterval)
                throw new ArgumentException($"Connection can't have {CalciteConnectionStringBuilder.ConnectionIdleLifetimeKey} {idleLifetime} under {CalciteConnectionStringBuilder.ConnectionPruningIntervalKey} {pruningInterval}.", nameof(options));

            _idleLifetime = TimeSpan.FromSeconds(idleLifetime);
            _pruningInterval = TimeSpan.FromSeconds(pruningInterval);
        }

        /// <inheritdoc />
        public override string ConnectionString => _options.ConnectionString;

        /// <summary>
        /// Gets the CLR type mapping every connection of this data source starts from.
        /// </summary>
        /// <remarks>
        /// What <see cref="CalciteDataSourceBuilder.TypeMapper"/> was configured with, or the built-in
        /// chain where the data source was built from a connection string alone. A connection copies it
        /// when it is created, so a resolver added to a connection is that connection's own.
        /// </remarks>
        public ClrTypeMapper TypeMapper => _typeMapper;

        /// <summary>
        /// Gets how often the provider looks at this data source for pruning, where it keeps it.
        /// </summary>
        internal TimeSpan PruningInterval => _pruningInterval;

        /// <summary>
        /// Gets the root a connection should plan against, and whether that connection owns it.
        /// </summary>
        /// <returns>The root, and <see langword="true"/> where it was built for this caller alone and is the
        /// caller's to retire.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the data source has been disposed.</exception>
        /// <remarks>
        /// Built once, by whichever connection opens first, with the others waiting on it — a failed build
        /// leaves nothing behind, so the next connection tries again. With <c>Pooling=false</c> there is no
        /// shared root: every call builds one, and the connection that asked retires it.
        /// </remarks>
        internal (CalciteDataSourceRoot Root, bool Owned) Acquire()
        {
            return TryAcquire(out var root, out var owned) ? (root, owned) : throw new ObjectDisposedException(GetType().Name);
        }

        /// <summary>
        /// <see cref="Acquire"/>, answering <see langword="false"/> rather than throwing where the data source
        /// has been disposed — which for one the provider keeps means it was pruned between being looked up
        /// and being used, and the caller looks it up again.
        /// </summary>
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
        /// Disposes this data source where no connection is open on it and none has been for its idle
        /// lifetime, answering whether it is now disposed.
        /// </summary>
        /// <param name="now">The current <see cref="Environment.TickCount64"/>.</param>
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
        /// Drops the root schema, so that the next connection to open builds a new one — re-reading the
        /// model, and with it a model file that has changed on disk.
        /// </summary>
        /// <remarks>
        /// A connection already open keeps the root it has, and the dropped root is disposed once the last
        /// such connection is.
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
        /// Creates a new closed <see cref="CalciteConnection"/> bound to this data source.
        /// </summary>
        /// <returns>A new <see cref="CalciteConnection"/> instance.</returns>
        public new CalciteConnection CreateConnection() => (CalciteConnection)base.CreateConnection();

        /// <summary>
        /// Creates and opens a new <see cref="CalciteConnection"/> bound to this data source.
        /// </summary>
        /// <returns>An opened <see cref="CalciteConnection"/> instance.</returns>
        public new CalciteConnection OpenConnection() => (CalciteConnection)base.OpenConnection();

        /// <summary>
        /// Asynchronously creates and opens a new <see cref="CalciteConnection"/> bound to this data source.
        /// </summary>
        /// <param name="cancellationToken">A token that may be used to cancel the operation.</param>
        /// <returns>A task whose result is an opened <see cref="CalciteConnection"/> instance.</returns>
        public new async ValueTask<CalciteConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            return (CalciteConnection)await base.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Retires the root: no new connection can be opened, a connection already open keeps working, and
        /// the root's disposable schemas are disposed once the last such connection is disposed.
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
