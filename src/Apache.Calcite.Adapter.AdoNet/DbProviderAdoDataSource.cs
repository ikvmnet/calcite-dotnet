using System;
using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An <see cref="AdoDataSource"/> backed by a <see cref="DbProviderFactory"/> and a connection string.
    /// </summary>
    /// <remarks>
    /// Use this class when you want to expose an ADO.NET provider to the Calcite adapter using the
    /// provider-factory pattern. Each call to <see cref="AdoDataSource.OpenConnection"/> creates and
    /// opens a new connection via the factory.
    /// </remarks>
    public class DbProviderAdoDataSource : AdoDataSource
    {

        readonly DbProviderFactory _factory;
        readonly string _connectionString;
        readonly AdoDatabaseMetadata _metadata;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbProviderAdoDataSource"/> class.
        /// </summary>
        /// <param name="factory">The provider factory used to create connections.</param>
        /// <param name="connectionString">The connection string passed to each new connection.</param>
        /// <param name="metadata">The metadata provider that describes the data source's schema.</param>
        /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
        public DbProviderAdoDataSource(DbProviderFactory factory, string connectionString, AdoDatabaseMetadata metadata)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        }

        /// <inheritdoc />
        public override DbConnection OpenConnection()
        {
            var cnn = _factory.CreateConnection() ?? throw new AdoCalciteException("Null result creating connection.");
            cnn.ConnectionString = _connectionString;
            cnn.Open();
            return cnn;
        }

        /// <inheritdoc />
        public override string ConnectionString => _connectionString;

        /// <inheritdoc />
        public override AdoDatabaseMetadata Metadata => _metadata;

    }

}
