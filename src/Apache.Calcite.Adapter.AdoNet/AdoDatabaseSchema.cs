using System;
using System.Data;

using com.google.common.collect;

using java.util;

using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A Calcite schema that represents a named database in an ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This schema does not expose tables directly. Instead it exposes one child <see cref="AdoSchema"/>
    /// per database schema (e.g. SQL Server <c>dbo</c>, <c>sales</c>). Each child schema then
    /// lazily discovers its tables on demand.
    /// </para>
    /// <para>
    /// Schema names are resolved from <see cref="AdoDatabaseMetadata.GetSchemas"/> and cached after
    /// the first lookup. Tables within each child schema are populated on first access.
    /// </para>
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
