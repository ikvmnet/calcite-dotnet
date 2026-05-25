using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// The default <see cref="AdoDatabaseMetadataFactory"/> that selects the best-fit
    /// <see cref="AdoDatabaseMetadata"/> implementation for a given ADO.NET connection type.
    /// </summary>
    /// <remarks>
    /// Supports SQL Server, SQLite, ODBC, and OLE DB connections out of the box. For any other
    /// provider, this factory throws <see cref="AdoCalciteException"/>. Use
    /// <see cref="AdoDatabaseMetadataFactory"/> directly if you need to supply a custom metadata
    /// provider for an unsupported driver.
    /// </remarks>
    public class AdoDatabaseMetadataFactoryImpl : AdoDatabaseMetadataFactory
    {

        /// <summary>
        /// Gets the shared singleton instance of <see cref="AdoDatabaseMetadataFactoryImpl"/>.
        /// </summary>
        public static readonly AdoDatabaseMetadataFactoryImpl Instance = new AdoDatabaseMetadataFactoryImpl();

        /// <inheritdoc />
        public override AdoDatabaseMetadata Create(DbDataSource dbDataSource)
        {
            // temporary connection object just to test type
            using var connection = dbDataSource.CreateConnection();

            // Windows-only OLEDB connection
            if (connection is OleDbConnection oledb)
                return new OleDbDatabaseMetadata(dbDataSource);

            // ODBC connection
            if (connection is OdbcConnection odbc)
                return new OdbcDatabaseMetadata(dbDataSource);

            // selection based on type name
            switch (connection.GetType().FullName)
            {
                case "System.Data.SqlClient.SqlConnection":
                case "Microsoft.Data.SqlClient.SqlConnection":
                    return new SqlServerDatabaseMetadata(dbDataSource);
                case "Microsoft.Data.Sqlite.SqliteConnection":
                    return new SqliteDatabaseMetadata(dbDataSource);
            }

            throw new AdoCalciteException($"No metadata provider available for connection of type '{connection.GetType().FullName}'.");
        }

    }

}
