using System;
using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An <see cref="AdoDataSource"/> backed by a <see cref="DbDataSource"/> and an explicitly
    /// supplied metadata provider.
    /// </summary>
    /// <remarks>
    /// Use this class when your application already manages a <see cref="DbDataSource"/> and you
    /// want to expose it to the Calcite ADO.NET adapter without wrapping a provider factory.
    /// </remarks>
    public class DbDataSourceAdoDataSource : AdoDataSource
    {

        readonly DbDataSource _dataSource;
        readonly AdoDatabaseMetadata _metadata;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbDataSourceAdoDataSource"/> class.
        /// </summary>
        /// <param name="dataSource">The <see cref="DbDataSource"/> used to open connections.</param>
        /// <param name="metadata">The metadata provider that describes the data source's schema.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/> or <paramref name="metadata"/> is <see langword="null"/>.</exception>
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
