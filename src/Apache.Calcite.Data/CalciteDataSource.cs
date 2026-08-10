using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a source of <see cref="CalciteConnection"/> instances configured by a single
    /// connection string. This is the Apache Calcite implementation of the .NET 7+ <see cref="DbDataSource"/> pattern.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <see cref="CalciteDataSource"/> is intended to be created once per logical data source and shared across the application. Each call to <see cref="OpenConnection"/>,
    /// <see cref="OpenConnectionAsync(CancellationToken)"/>, or the <see cref="DbDataSource.CreateConnection"/> override returns a fresh <see cref="CalciteConnection"/>
    /// bound to the configured <see cref="DbDataSource.ConnectionString"/>.
    /// </para>
    /// <para>
    /// The Calcite provider does not perform connection pooling; each opened connection initializes its own in-process Calcite engine session.
    /// </para>
    /// <para>
    /// To share a pre-built schema across all connections produced by this data source, subclass <see cref="CalciteDataSource"/> and override <see cref="CreateDbConnection"/> to
    /// open the connection and register schemas on <see cref="CalciteConnection.RootSchema"/> before returning it:
    /// <code>
    /// public class MyDataSource : CalciteDataSource
    /// {
    ///     readonly Schema _sharedSchema = new MySchema();
    ///
    ///     public MyDataSource(string connectionString) : base(connectionString) { }
    ///
    ///     protected override DbConnection CreateDbConnection()
    ///     {
    ///         var conn = (CalciteConnection)base.CreateDbConnection();
    ///         conn.Open();
    ///         conn.RootSchema.add("MY", _sharedSchema);
    ///         return conn;
    ///     }
    /// }
    /// </code>
    /// Because each <see cref="CalciteConnection"/> owns its own Calcite engine session, the <c>Schema</c> instance itself must be thread-safe if the same instance is added to
    /// connections that may be used concurrently.
    /// </para>
    /// </remarks>
    public class CalciteDataSource : DbDataSource
    {

        readonly string _connectionString;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string used by every connection produced from this data source.
        /// Recognized keys are described on <see cref="CalciteConnectionStringBuilder"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is <see langword="null"/>.</exception>
        public CalciteDataSource(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSource"/> class using the specified builder.
        /// </summary>
        /// <param name="connectionStringBuilder">The builder whose <see cref="DbConnectionStringBuilder.ConnectionString"/> is used to configure produced connections.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionStringBuilder"/> is <see langword="null"/>.</exception>
        public CalciteDataSource(CalciteConnectionStringBuilder connectionStringBuilder)
            : this((connectionStringBuilder ?? throw new ArgumentNullException(nameof(connectionStringBuilder))).ConnectionString)
        {

        }

        /// <inheritdoc />
        public override string ConnectionString => _connectionString;

        /// <summary>
        /// Gets or sets the plan cache factory applied to each connection this data source creates.
        /// </summary>
        /// <remarks>
        /// A data source is created once and shared, which makes it the natural owner of a shared plan
        /// cache: <c>PlanCacheFactory = CalcitePlanCacheFactory.From(new LruPlanCache(256))</c> gives
        /// every connection it creates one capacity budget to share. Read when a connection is created;
        /// a connection already handed out keeps the factory it was created with.
        /// </remarks>
        public CalcitePlanCacheFactory? PlanCacheFactory { get; set; }

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection() => new CalciteConnection(_connectionString) { PlanCacheFactory = PlanCacheFactory };

        /// <summary>
        /// Creates a new closed <see cref="CalciteConnection"/> bound to this data source's connection string.
        /// </summary>
        /// <returns>A new <see cref="CalciteConnection"/> instance.</returns>
        public new CalciteConnection CreateConnection() => (CalciteConnection)base.CreateConnection();

        /// <summary>
        /// Creates and opens a new <see cref="CalciteConnection"/> bound to this data source's connection string.
        /// </summary>
        /// <returns>An opened <see cref="CalciteConnection"/> instance.</returns>
        public new CalciteConnection OpenConnection() => (CalciteConnection)base.OpenConnection();

        /// <summary>
        /// Asynchronously creates and opens a new <see cref="CalciteConnection"/> bound to this data source's connection string.
        /// </summary>
        /// <param name="cancellationToken">A token that may be used to cancel the operation.</param>
        /// <returns>A task whose result is an opened <see cref="CalciteConnection"/> instance.</returns>
        public new async ValueTask<CalciteConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            return (CalciteConnection)await base.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

    }

}
