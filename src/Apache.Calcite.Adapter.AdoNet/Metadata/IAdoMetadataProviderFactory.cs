using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Provides instances of the appropriate <see cref="AdoMetadataProvider"/>.
    /// </summary>
    public interface IAdoMetadataProviderFactory
    {

        /// <summary>
        /// Creates a <see cref="AdoMetadataProvider"/> for the specified <see cref="DbDataSource"/>.
        /// </summary>
        /// <param name="ds"></param>
        /// <returns></returns>
        AdoMetadataProvider Create(DbDataSource ds);

    }

}
