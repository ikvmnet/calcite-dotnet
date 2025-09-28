using System;
using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Provides an implementation of <see cref="AdoDataSource"/> backed by a <see cref="DbProviderFactory"/>.
    /// </summary>
    class DbProviderAdoDataSource : AdoDataSource
    {

        readonly DbProviderFactory _factory;
        readonly string _connectionString;
        readonly AdoDatabaseMetadata _metadata;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="factory"></param>
        /// <param name="connectionString"></param>
        /// <param name="metadata"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public DbProviderAdoDataSource(DbProviderFactory factory, string connectionString, AdoDatabaseMetadata metadata)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        }

        /// <inheritdoc />
        public override DbConnection OpenConnection()
        {
            var cnn = _factory.CreateConnection() ?? throw new AdoSchemaException("Null result creating connection.");
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
