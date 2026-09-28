using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Chooses and creates the <see cref="AdoDatabaseMetadata"/> for a <see cref="DbDataSource"/>.
    /// </summary>
    /// <remarks>
    /// <see cref="AdoDatabaseMetadataFactoryImpl"/> is the default. Derive from this class to choose differently,
    /// and pass the factory to <see cref="AdoSchema.Create(org.apache.calcite.schema.SchemaPlus, string, DbDataSource, AdoDatabaseMetadataFactory, string, string)"/>.
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
