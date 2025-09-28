using System.Data.Common;

using Apache.Calcite.Adapter.Ado.Metadata;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Provides open ADO connections.
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
