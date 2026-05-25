using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Creates <see cref="AdoDatabaseMetadata"/> instances for a given <see cref="DbDataSource"/>.
    /// </summary>
    /// <remarks>
    /// Implement this factory when the metadata for a data source cannot be determined until an
    /// actual <see cref="DbDataSource"/> is available at runtime. Pass the factory to the adapter
    /// configuration so it can obtain metadata on demand.
    /// </remarks>
    public abstract class AdoDatabaseMetadataFactory
    {

        /// <summary>
        /// Creates an <see cref="AdoDatabaseMetadata"/> instance that describes the schema of the
        /// specified <paramref name="dbDataSource"/>.
        /// </summary>
        /// <param name="dbDataSource">The data source to create metadata for.</param>
        /// <returns>An <see cref="AdoDatabaseMetadata"/> that describes <paramref name="dbDataSource"/>.</returns>
        public abstract AdoDatabaseMetadata Create(DbDataSource dbDataSource);

    }

}
