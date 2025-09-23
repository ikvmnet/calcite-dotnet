using System;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Default implementation of <see cref="IAdoMetadataProviderFactory"/>.
    /// </summary>
    public class AdoMetadataProviderFactoryImpl : IAdoMetadataProviderFactory
    {

        /// <summary>
        /// Gets the default singleton instance.
        /// </summary>
        public static readonly AdoMetadataProviderFactoryImpl Instance = new AdoMetadataProviderFactoryImpl();

        /// <inheritdoc />
        public AdoMetadataProvider Create(DbDataSource ds)
        {
            throw new NotImplementedException();
        }

    }

}
