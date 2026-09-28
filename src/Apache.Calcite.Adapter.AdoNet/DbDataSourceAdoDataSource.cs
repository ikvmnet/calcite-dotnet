using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An <see cref="AdoDataSource"/> that opens connections from a <see cref="DbDataSource"/>.
    /// </summary>
    /// <remarks>
    /// Both <see cref="OpenConnection"/> and <see cref="OpenConnectionAsync"/> go to the <see cref="DbDataSource"/>.
    /// The caller keeps ownership of the <see cref="DbDataSource"/>; this class does not dispose it.
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
        public override ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default) => _dataSource.OpenConnectionAsync(cancellationToken);

        /// <inheritdoc />
        public override string ConnectionString => _dataSource.ConnectionString;

        /// <inheritdoc />
        public override AdoDatabaseMetadata Metadata => _metadata;

    }

}
