using System;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A <see cref="DbDataSource"/> over a <see cref="DbProviderFactory"/> and a connection string.
    /// </summary>
    /// <remarks>
    /// <see cref="DbProviderFactory.CreateDataSource"/> is a throwing base implementation, and none of the
    /// providers this adapter ships metadata for overrides it: Microsoft.Data.Sqlite, System.Data.Odbc and
    /// System.Data.OleDb declare no <see cref="DbDataSource"/> at all, and Microsoft.Data.SqlClient offers
    /// only <c>CreateDataSourceEnumerator</c>, which is unrelated. Asking a factory for one is therefore not
    /// a way to reach a data source; opening the connection ourselves is. Metadata detection wants a
    /// <see cref="DbDataSource"/> because that is what <see cref="Metadata.AdoDatabaseMetadataFactory"/>
    /// takes, and this supplies one.
    /// </remarks>
    sealed class DbProviderFactoryDataSource : DbDataSource
    {

        readonly DbProviderFactory _factory;
        readonly string _connectionString;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="factory">The provider factory used to create connections.</param>
        /// <param name="connectionString">The connection string given to each new connection.</param>
        /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
        public DbProviderFactoryDataSource(DbProviderFactory factory, string connectionString)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <inheritdoc />
        public override string ConnectionString => _connectionString;

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection()
        {
            var cnn = _factory.CreateConnection() ?? throw new AdoCalciteException("Null result creating connection.");
            cnn.ConnectionString = _connectionString;
            return cnn;
        }

    }

}
