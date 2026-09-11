using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Provides open ADO.NET connections and database metadata to the Calcite adapter engine.
    /// </summary>
    /// <remarks>
    /// Implement this class to connect Calcite's ADO.NET adapter to a specific data source.
    /// The adapter calls <see cref="OpenConnection"/>, or <see cref="OpenConnectionAsync"/> where the plan
    /// is asynchronous, for each query it needs to execute, and <see cref="Metadata"/> to discover schemas,
    /// tables, and column definitions at planning time.
    /// </remarks>
    public abstract class AdoDataSource
    {

        /// <summary>
        /// Opens a new connection to the underlying data source.
        /// </summary>
        /// <returns>An open <see cref="DbConnection"/> ready for query execution.</returns>
        public abstract DbConnection OpenConnection();

        /// <summary>
        /// Opens a new connection to the underlying data source, without blocking.
        /// </summary>
        /// <param name="cancellationToken">Abandons the attempt.</param>
        /// <returns>An open <see cref="DbConnection"/> ready for query execution.</returns>
        /// <remarks>
        /// What a plan of <c>ClrAsyncEnumerableConvention</c> opens its connection by. The default blocks on
        /// <see cref="OpenConnection"/>, so a source written before this member still answers; a source over
        /// a provider that opens asynchronously should override it, as both sources here do.
        /// </remarks>
        public virtual ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return new ValueTask<DbConnection>(OpenConnection());
        }

        /// <summary>
        /// Gets the connection string used to open connections to the underlying data source.
        /// </summary>
        public abstract string ConnectionString { get; }

        /// <summary>
        /// Gets the metadata provider that describes the databases, schemas, tables, and columns
        /// exposed by this data source.
        /// </summary>
        public abstract AdoDatabaseMetadata Metadata { get; }

    }

}
