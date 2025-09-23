using System;
using System.Data;
using System.Data.Common;

using com.google.common.collect;

using java.util;

using org.apache.calcite.schema.lookup;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
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

            readonly DbDataSource _dataSource;
            readonly SqlDialect _dialect;
            readonly AdoConvention _convention;
            readonly string? _databaseName;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="dataSource"></param>
            /// <param name="dialect"></param>
            /// <param name="convention"></param>
            /// <param name="databaseName"></param>
            public SchemasLookup(DbDataSource dataSource, SqlDialect dialect, AdoConvention convention, string? databaseName)
            {
                _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
                _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
                _convention = convention ?? throw new ArgumentNullException(nameof(convention));
                _databaseName = databaseName;
            }

            /// <inheritdoc />
            public override Set getNames(LikePattern pattern)
            {
                var builder = ImmutableSet.builder();

                try
                {
                    using var connection = _dataSource.OpenConnection();
                    var metadata = new AdoDatabaseMetaDataAdaptor(connection);
                    var resultSet = metadata.getSchemas(_databaseName, pattern.pattern);
                    while (resultSet.next())
                        builder.add(resultSet.getString(1) ?? throw new AdoSchemaException("Got null SchemaName from database."));
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
                    using var connection = _dataSource.OpenConnection();
                    var metadata = new AdoDatabaseMetaDataAdaptor(connection);
                    var resultSet = metadata.getSchemas(_databaseName, name);
                    while (resultSet.next())
                        return new AdoSchema(_dataSource, _dialect, _convention, _databaseName, resultSet.getString(1) ?? throw new AdoSchemaException("Got null SchemaName from database."));
                }
                catch (DataException e)
                {
                    throw new AdoSchemaException("Exception listing schema names.", e);
                }

                return null;
            }

        }

        readonly DbDataSource _dataSource;
        readonly SqlDialect _dialect;
        readonly AdoConvention _convention;
        readonly string? _databaseName;
        readonly LoadingCacheLookup _subSchemas;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="dialect"></param>
        /// <param name="convention"></param>
        /// <param name="databaseName"></param>
        public AdoDatabaseSchema(DbDataSource dataSource, SqlDialect dialect, AdoConvention convention, string? databaseName)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _convention = convention ?? throw new ArgumentNullException(nameof(convention));
            _databaseName = databaseName;
            _subSchemas = new LoadingCacheLookup(new SchemasLookup(_dataSource, _dialect, _convention, _databaseName));
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
