using System;
using System.Data.Common;

using Apache.Calcite.Adapter.Ado.Metadata;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Provides an implementation of <see cref="AdoDataSource"/> backed by a <see cref="DbDataSource"/> and explicit metadata provider.
    /// </summary>
    class DbDataSourceAdoDataSource : AdoDataSource
    {

        readonly DbDataSource _dataSource;
        readonly AdoDatabaseMetadata _metadata;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="metadata"></param>
        public DbDataSourceAdoDataSource(DbDataSource dataSource, AdoDatabaseMetadata metadata)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        }

        /// <inheritdoc />
        public override DbConnection OpenConnection() => _dataSource.OpenConnection();

        /// <inheritdoc />
        public override string ConnectionString => _dataSource.ConnectionString;

        /// <inheritdoc />
        public override AdoDatabaseMetadata Metadata => _metadata;

    }

}
