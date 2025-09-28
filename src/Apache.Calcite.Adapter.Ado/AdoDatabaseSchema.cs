using System;
using System.Data;

using com.google.common.collect;

using java.util;

using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Schema based upon a ADO.NET database name.
    /// </summary>
    /// <remarks>
    /// This schema does not directly contain tables, but contains a sub-schema for each schema in the database in
    /// the back-end. Each of those sub-schemas is an instance of <see cref="AdoSchema"/>.
    /// 
    /// This schema is lazy: it does not compute teh list of schema names until the first call to <see
    /// cref="subSchemas"/> and <see cref="Lookup.get(string)"/>. Then it create sa <see cref="AdoSchema"/> for
    /// this schema name. Each <see cref="AdoSchema"/> will populate its tables on demand.
    /// </remarks>
    public class AdoDatabaseSchema : AdoBaseSchema
    {

        class SchemasLookup : IgnoreCaseLookup
        {

            readonly AdoDataSource _dataSource;
            readonly AdoConvention _convention;
            readonly string? _databaseName;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="dataSource"></param>
            /// <param name="convention"></param>
            /// <param name="databaseName"></param>
            public SchemasLookup(AdoDataSource dataSource, AdoConvention convention, string? databaseName)
            {
                _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
                _convention = convention ?? throw new ArgumentNullException(nameof(convention));
                _databaseName = databaseName;
            }

            /// <inheritdoc />
            public override Set getNames(LikePattern pattern)
            {
                var builder = ImmutableSet.builder();

                try
                {
                    foreach (var schema in _dataSource.Metadata.GetSchemas(_databaseName))
                        builder.add(schema);
                }
                catch (DataException e)
                {
                    throw new AdoSchemaException("Exception listing schema names.", e);
                }

                return builder.build();
            }

            /// <inheritdoc />
            public override object? get(string name)
            {
                try
                {
                    foreach (var schema in _dataSource.Metadata.GetSchemas(_databaseName))
                        if (schema.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                            return new AdoSchema(_dataSource, _convention, _databaseName, schema.Name);
                }
                catch (DataException e)
                {
                    throw new AdoSchemaException("Exception listing schema names.", e);
                }

                return null;
            }

        }

        readonly AdoDataSource _dataSource;
        readonly AdoConvention _convention;
        readonly string? _databaseName;
        readonly LoadingCacheLookup _subSchemas;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="convention"></param>
        /// <param name="databaseName"></param>
        public AdoDatabaseSchema(AdoDataSource dataSource, AdoConvention convention, string? databaseName)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _convention = convention ?? throw new ArgumentNullException(nameof(convention));
            _databaseName = databaseName;
            _subSchemas = new LoadingCacheLookup(new SchemasLookup(_dataSource, _convention, _databaseName));
        }

        /// <inheritdoc />
        public override Lookup tables()
        {
            return Lookup.empty();
        }

        /// <inheritdoc />
        public override Lookup subSchemas()
        {
            return _subSchemas;
        }

    }

}
