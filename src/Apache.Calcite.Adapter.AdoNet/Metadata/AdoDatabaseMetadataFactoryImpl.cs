using System.Data.Common;

using Microsoft.Data.SqlClient;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
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
            var connection = dbDataSource.CreateConnection();
            if (connection is SqlConnection)
                return new SqlServerDatabaseMetadata(dbDataSource);

            throw new AdoSchemaException($"No metadata provider available for connection of type '{dbDataSource}'.");
        }

    }

}
