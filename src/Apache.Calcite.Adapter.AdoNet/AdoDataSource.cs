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
    /// The adapter calls <see cref="OpenConnection"/> for each query it needs to execute and
    /// <see cref="Metadata"/> to discover schemas, tables, and column definitions at planning time.
    /// </remarks>
    public abstract class AdoDataSource
    {

        /// <summary>
        /// Opens a new connection to the underlying data source.
        /// </summary>
        /// <returns>An open <see cref="DbConnection"/> ready for query execution.</returns>
        public abstract DbConnection OpenConnection();

        /// <summary>
        /// Opens a new connection to the underlying data source without blocking the calling thread.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>An open <see cref="DbConnection"/> ready for query execution.</returns>
        /// <remarks>
        /// The default opens the connection synchronously and returns a completed task, because a source
        /// that cannot open without blocking still has to answer. That is not the sync-over-async the
        /// asynchronous convention refuses — nothing here waits on a task — it is a caller that is simply
        /// not asynchronous over the open. Both sources this assembly ships override it, and a source whose
        /// provider offers a real asynchronous open should.
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
