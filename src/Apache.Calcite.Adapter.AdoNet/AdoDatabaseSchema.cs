using System;
using System.Data;

using com.google.common.collect;

using java.util;

using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A Calcite schema for one database of an ADO.NET data source, with one <see cref="AdoSchema"/> per
    /// schema of that database as its sub-schemas and no tables of its own.
    /// </summary>
    /// <remarks>
    /// Sub-schema names come from <see cref="Metadata.AdoDatabaseMetadata.GetSchemas"/>, and a sub-schema that
    /// has been looked up is cached for a minute (Calcite's <c>LoadingCacheLookup</c>). Every sub-schema
    /// shares this schema's data source and convention.
    /// </remarks>
    public class AdoDatabaseSchema : AdoBaseSchema
    {

        /// <summary>
        /// Resolves the schemas of one database from the data source's metadata.
        /// </summary>
        /// <remarks>
        /// TODO: <see cref="getNames"/> adds the <see cref="Metadata.AdoSchemaMetadata"/> values rather than their
        /// names and ignores the pattern.
        /// </remarks>
        class SchemasLookup : IgnoreCaseLookup
        {

            readonly AdoDataSource _dataSource;
            readonly AdoConvention _convention;
            readonly string? _databaseName;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="dataSource">The data source whose schemas are listed.</param>
            /// <param name="convention">The convention every sub-schema plans into.</param>
            /// <param name="databaseName">The database, or <see langword="null"/> for the default.</param>
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
                    throw new AdoCalciteException("Exception listing schema names.", e);
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
                    throw new AdoCalciteException("Exception listing schema names.", e);
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
        /// <param name="dataSource">The data source the database belongs to.</param>
        /// <param name="convention">The convention every sub-schema plans into.</param>
        /// <param name="databaseName">The database, or <see langword="null"/> for the provider's default.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/> or <paramref name="convention"/> is <see langword="null"/>.</exception>
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
