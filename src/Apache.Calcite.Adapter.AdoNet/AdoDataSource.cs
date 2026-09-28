using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Adapter.AdoNet.Metadata;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Opens connections to a database and describes it: what the adapter needs of an ADO.NET source.
    /// </summary>
    /// <remarks>
    /// The adapter opens a new connection for every statement it executes, and disposes it when the statement's
    /// rows have been read. <see cref="DbProviderAdoDataSource"/> and <see cref="DbDataSourceAdoDataSource"/> cover
    /// the usual cases; derive from this class to supply connections some other way.
    /// </remarks>
    public abstract class AdoDataSource
    {

        /// <summary>
        /// Opens a new connection. The caller owns it.
        /// </summary>
        /// <returns>An open connection.</returns>
        public abstract DbConnection OpenConnection();

        /// <summary>
        /// Opens a new connection asynchronously. The caller owns it. The adapter calls this when the rows of a
        /// plan are read with <c>ReadAsync</c>.
        /// </summary>
        /// <param name="cancellationToken">Cancels the attempt.</param>
        /// <returns>An open connection.</returns>
        /// <remarks>
        /// The default calls <see cref="OpenConnection"/> synchronously. Override it where the provider can open a
        /// connection asynchronously.
        /// </remarks>
        public virtual ValueTask<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            return new ValueTask<DbConnection>(OpenConnection());
        }

        /// <summary>
        /// Gets the connection string connections are opened with.
        /// </summary>
        public abstract string ConnectionString { get; }

        /// <summary>
        /// Gets the metadata that describes the database's schemas, tables and columns, its dialect and its
        /// parameter syntax.
        /// </summary>
        public abstract AdoDatabaseMetadata Metadata { get; }

    }

}
