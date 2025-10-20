using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Provides instances of the appropriate <see cref="AdoDatabaseMetadata"/>.
    /// </summary>
    public abstract class AdoDatabaseMetadataFactory
    {

        /// <summary>
        /// Creates a <see cref="AdoDatabaseMetadata"/> for the specified <see cref="DbDataSource"/>.
        /// </summary>
        /// <param name="dbDataSource"></param>
        /// <returns></returns>
        public abstract AdoDatabaseMetadata Create(DbDataSource dbDataSource);

    }

}
