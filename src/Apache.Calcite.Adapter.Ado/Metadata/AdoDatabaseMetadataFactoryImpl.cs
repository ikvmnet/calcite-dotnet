using System.Data.Common;

namespace Apache.Calcite.Adapter.Ado.Metadata
{

    /// <summary>
    /// Default implementation of <see cref="AdoDatabaseMetadataFactory"/> that discovers metadata from a connection.
    /// </summary>
    public class AdoDatabaseMetadataFactoryImpl : AdoDatabaseMetadataFactory
    {

        /// <summary>
        /// Gets the default singleton instance.
        /// </summary>
        public static readonly AdoDatabaseMetadataFactoryImpl Instance = new AdoDatabaseMetadataFactoryImpl();

        /// <inheritdoc />
        public override AdoDatabaseMetadata Create(DbDataSource dbDataSource)
        {
            // temporary connection object just to test type
            using var connection = dbDataSource.CreateConnection();

            // selection based on type name
            switch (connection.GetType().FullName)
            {
                case "System.Data.SqlClient.SqlConnection":
                case "Microsoft.Data.SqlClient.SqlConnection":
                    return new SqlServerDatabaseMetadata(dbDataSource);
                case "Microsoft.Data.Sqlite.SqliteConnection":
                    return new SqliteDatabaseMetadata(dbDataSource);
            }

            throw new AdoSchemaException($"No metadata provider available for connection of type '{dbDataSource}'.");
        }

    }

}
