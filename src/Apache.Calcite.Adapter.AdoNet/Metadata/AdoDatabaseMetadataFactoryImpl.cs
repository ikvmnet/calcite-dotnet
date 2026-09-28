using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// The default <see cref="AdoDatabaseMetadataFactory"/>, which chooses the metadata provider by the type of
    /// connection the data source creates.
    /// </summary>
    /// <remarks>
    /// Recognises <c>Microsoft.Data.SqlClient.SqlConnection</c> and <c>System.Data.SqlClient.SqlConnection</c>
    /// (SQL Server), <c>Microsoft.Data.Sqlite.SqliteConnection</c>, <see cref="OdbcConnection"/> and
    /// <see cref="OleDbConnection"/>. The connection is created to inspect its type and is not opened.
    /// </remarks>
    public class AdoDatabaseMetadataFactoryImpl : AdoDatabaseMetadataFactory
    {

        /// <summary>
        /// A shared instance.
        /// </summary>
        public static readonly AdoDatabaseMetadataFactoryImpl Instance = new AdoDatabaseMetadataFactoryImpl();

        /// <inheritdoc />
        /// <exception cref="AdoCalciteException">The connection is of a type this factory does not recognise.</exception>
        public override AdoDatabaseMetadata Create(DbDataSource dbDataSource)
        {
            using var connection = dbDataSource.CreateConnection();

            if (connection is OleDbConnection oledb)
                return new OleDbDatabaseMetadata(dbDataSource);

            if (connection is OdbcConnection odbc)
                return new OdbcDatabaseMetadata(dbDataSource);

            // matched by name, so that neither SQL Server client nor SQLite needs to be referenced
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
