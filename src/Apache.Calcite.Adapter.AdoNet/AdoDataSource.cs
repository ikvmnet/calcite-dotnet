using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Provides open ADO connections and corresponding metadata to Calcite upon demand. This exists to allow down-level
    /// .NET Framework support, as well as because ADO.NET does not provide a DatabaseMetadats infrastructure similar
    /// to JDBC.
    /// </summary>
    public abstract class AdoDataSource
    {

        /// <summary>
        /// Options a new connection to the underlying data source.
        /// </summary>
        /// <returns></returns>
        public abstract DbConnection OpenConnection();

        /// <summary>
        /// Gets the connection string used to open new connections.
        /// </summary>
        public abstract string ConnectionString { get; }

        /// <summary>
        /// Gets the provider of database metadata.
        /// </summary>
        public abstract AdoDatabaseMetadata Metadata { get; }

    }

}
