using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

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
    /// keeps for that connection string, made the first time the string is seen and shared by every
    /// connection opened with an equivalent string afterwards; <c>Pooling=false</c> in the string opts a
    /// connection out, giving it a root of its own. Or the application builds one with
    /// <see cref="CalciteDataSourceBuilder"/>, which is the only way to hand over a schema instance the
    /// application constructed, and which is the application's to dispose.
    /// </para>
    /// <para>
    /// Sharing a root means an adapter's schema may be read from several threads at once. Calcite serialises
    /// nothing; a schema reachable from a data source has to tolerate concurrent reads. DDL is serialised
    /// against DDL by the provider. Disposing the data source disposes every schema on its root that
    /// implements <see cref="IDisposable"/>, and a connection still open on it fails at its next statement.
    /// </para>
    /// </remarks>
    public class CalciteDataSource : DbDataSource
    {

        readonly CalciteConnectionStringBuilder _options;
        readonly IReadOnlyList<Action<org.apache.calcite.schema.SchemaPlus>> _configure;
        readonly bool _pooling;
        readonly object _sync = new();
        CalciteDataSourceRoot? _root;
        bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string used by every connection produced from this data source.
        /// Recognized keys are described on <see cref="CalciteConnectionStringBuilder"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is <see langword="null"/>.</exception>
        public CalciteDataSource(string connectionString) :
            this(new CalciteConnectionStringBuilder(connectionString ?? throw new ArgumentNullException(nameof(connectionString))), [])
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class using the specified builder.
        /// </summary>
        /// <param name="connectionStringBuilder">The builder whose <see cref="DbConnectionStringBuilder.ConnectionString"/> is used to configure produced connections.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionStringBuilder"/> is <see langword="null"/>.</exception>
        public CalciteDataSource(CalciteConnectionStringBuilder connectionStringBuilder) :
            this(new CalciteConnectionStringBuilder((connectionStringBuilder ?? throw new ArgumentNullException(nameof(connectionStringBuilder))).ConnectionString), [])
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="options">The connection string, which this instance owns.</param>
        /// <param name="configure">Steps to run over the root after the model, in order.</param>
        internal CalciteDataSource(CalciteConnectionStringBuilder options, IReadOnlyList<Action<org.apache.calcite.schema.SchemaPlus>> configure)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _configure = configure ?? throw new ArgumentNullException(nameof(configure));
            _pooling = options.Pooling ?? true;
        }

        /// <inheritdoc />
        public override string ConnectionString => _options.ConnectionString;

        /// <summary>
        /// Gets the root a connection should plan against, and whether that connection owns it.
        /// </summary>
        /// <returns>The root, and <see langword="true"/> where it was built for this caller alone and is the
        /// caller's to dispose.</returns>
        /// <exception cref="ObjectDisposedException">Thrown when the data source has been disposed.</exception>
        /// <remarks>
        /// Built once, by whichever connection opens first, with the others waiting on it — a failed build
        /// leaves nothing behind, so the next connection tries again. With <c>Pooling=false</c> there is no
        /// shared root: every call builds one, and the connection that asked disposes it.
        /// </remarks>
        internal (CalciteDataSourceRoot Root, bool Owned) Acquire()
        {
            ThrowIfDisposed();

            if (_pooling == false)
                return (CalciteDataSourceRoot.Build(_options, _configure), true);

            lock (_sync)
            {
                ThrowIfDisposed();
                _root ??= CalciteDataSourceRoot.Build(_options, _configure);
                return (_root, false);
            }
        }

        /// <summary>
        /// Drops the root schema, so that the next connection to open builds a new one — re-reading the
        /// model, and with it a model file that has changed on disk.
        /// </summary>
        /// <remarks>
        /// A connection already open keeps the root it has. The dropped root is not disposed, for that
        /// reason; it goes when the last such connection lets go of it.
        /// </remarks>
        public void Clear()
        {
            lock (_sync)
                _root = null;
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

                root?.Dispose();
            }

            base.Dispose(disposing);
        }

        void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().Name);
        }

    }

}
