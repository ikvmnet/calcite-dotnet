using System;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implementation of <see cref="AdoDatabaseMetadataFactory"/> that instantiates metadata directly from a specified type name.
    /// </summary>
    class AdoDatabaseMetadataTypeFactory : AdoDatabaseMetadataFactory
    {

        readonly Type _type;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="type"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public AdoDatabaseMetadataTypeFactory(Type type)
        {
            _type = type ?? throw new ArgumentNullException(nameof(type));
        }

        /// <inheritdoc />
        public override AdoDatabaseMetadata Create(DbDataSource dbDataSource)
        {
            return Activator.CreateInstance(_type, dbDataSource) as AdoDatabaseMetadata ?? throw new AdoCalciteException($"Could not create instance of type '{_type.FullName}' as AdoDatabaseMetadata.");
        }

    }

}
