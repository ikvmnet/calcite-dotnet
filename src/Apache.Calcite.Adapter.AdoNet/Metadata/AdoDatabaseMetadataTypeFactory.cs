using System;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// An <see cref="AdoDatabaseMetadataFactory"/> that creates an instance of one given type, passing the
    /// <see cref="DbDataSource"/> to its constructor. Backs the <c>adoDatabaseMetadata</c> model operand.
    /// </summary>
    class AdoDatabaseMetadataTypeFactory : AdoDatabaseMetadataFactory
    {

        readonly Type _type;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="type">An <see cref="AdoDatabaseMetadata"/> type with a public constructor taking a <see cref="DbDataSource"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
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
