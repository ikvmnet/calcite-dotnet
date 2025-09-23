using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using com.google.common.collect;

using java.sql;
using java.util;

using org.apache.calcite.avatica;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Implementation of <see cref="Schema"/> that is backed by ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// The tables in the ADO.NET data source appear to be tables in this schema; queries against this schema are
    /// executed against those tables, pushing down as much as possible of the query logic to SQL.
    /// </remarks>
    public class AdoSchema : AdoBaseSchema
    {

        /// <summary>
        /// Lookup for resolving the tables of this schema.
        /// </summary>
        class TablesLookup : IgnoreCaseLookup
        {

            readonly AdoMetadataProvider _metadata;
            readonly string _databaseName;
            readonly string _schemaName;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="metadata"></param>
            /// <param name="databaseName"></param>
            /// <param name="schemaName"></param>
            public TablesLookup(AdoMetadataProvider metadata, string databaseName, string schemaName)
            {
                _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
                _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
                _schemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
            }

            /// <inheritdoc />
            public override Set getNames(LikePattern pattern)
            {
                var builder = ImmutableSet.builder();

                foreach (var table in _metadata.GetTables(_databaseName, _schemaName))
                    if (pattern.matcher().apply(table.Name))
                        builder.add(table.Name);

                return builder.build();
            }

            /// <inheritdoc />
            public override object get(string name)
            {
                throw new NotImplementedException();
            }

        }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="dataSource"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, DbDataSource dataSource, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, dataSource, AdoMetadataProviderFactoryImpl.Instance, databaseName, schemaName);
        }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="dataSource"></param>
        /// <param name="metadataFactory"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, DbDataSource dataSource, IAdoMetadataProviderFactory? metadataFactory, string? databaseName, string? schemaName)
        {
            // resolve associated metadata provider
            var metadata = metadataFactory.Create(ds);
            if (metadata is null)
                throw new AdoSchemaException("Could not determine AdoMetadataProvider from factory.");

            // create schema
            return Create(parentSchema, name, dataSource, metadata, databaseName, schemaName);
        }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="dataSource"></param>
        /// <param name="metadata"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, DbDataSource dataSource, IAdoMetadataProvider metadata, string? databaseName, string? schemaName)
        {
            // fallback to current connection database
            if (string.IsNullOrWhiteSpace(databaseName))
                database = metadata.GetDefaultDatabase();
            if (string.IsNullOrWhiteSpace(databaseName))
                throw new AdoSchemaException("Could not determine database name.");

            // fallback to current connection schema
            if (string.IsNullOrWhiteSpace(schemaName))
                schemaName = metadata.GetDefaultSchema();
            if (string.IsNullOrWhiteSpace(schemaName))
                throw new AdoSchemaException("Could not determine schema name.");

            // generate schema
            var expression = Schemas.subSchemaExpression(parentSchema, name, typeof(AdoSchema));
            var dialect = metadata.GetDialect();
            var convention = AdoConvention.Create(dialect, expression, name);
            return new AdoSchema(dataSource, dialect, metadata, convention, databaseName, schemaName);
        }

        /// <summary>
        /// Creates a <see cref="AdoSchema"/>, taking credentials from a map.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="operand"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, Map operand)
        {
            DbDataSource? ds = null;
            IAdoMetadataProviderFactory? metadataFactory = null;

            // data source name explicitly specified
            var dataSourceName = (string)operand.get("dataSource");
            if (dataSourceName != null)
            {
                ds = (DbDataSource)AvaticaUtils.instantiatePlugin(typeof(DbDataSource), dataSourceName);
                if (ds is null)
                    throw new AdoSchemaException("Failed to instantiate DbDataSource plugin.");
            }

            // fallback to provider name and connection string
            if (ds is null)
            {
                var providerName = (string?)operand.get("providerName");
                if (providerName is null || string.IsNullOrWhiteSpace(providerName))
                    throw new AdoSchemaException("Required missing property 'providerName'.");

                var connectionString = (string?)operand.get("connectionString");
                if (connectionString is null || string.IsNullOrWhiteSpace(connectionString))
                    throw new AdoSchemaException("Required missing property 'connectionString'.");

                ds = CreateDataSource(providerName, connectionString);
                if (ds is null)
                    throw new AdoSchemaException("Failed to instantiate DbDataSource from providerName and connectionString.");
            }

            // metadata factory name explicitly specified
            var metadataFactoryName = (string?)operand.get("metadataProviderFactory");
            if (string.IsNullorWhiteSpace(metadataFactoryName) == false)
                metadataFactory = (IAdoMetadataProviderFactory)AvaticaUtils.instantiatePlugin(typeof(IAdoMetadataProviderFactory), metadataFactoryName);

            return Create(
                parentSchema,
                name,
                ds,
                metadataFactory,
                (string?)operand.get("database"),
                (string?)operand.get("schema"));
        }

        /// <summary>
        /// Gets a <see cref="DataSource"/> based on the given provider name and connection string.
        /// </summary>
        /// <param name="providerName"></param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        public static DbDataSource CreateDataSource(string providerName, string connectionString)
        {
            DbProviderFactory factory;

            try
            {
                factory = DbProviderFactories.GetFactory(providerName);
            }
            catch (ArgumentException e)
            {
                throw new AdoSchemaException("Could not find ADO provider factory.", e);
            }

            return factory.CreateDataSource(connectionString);
        }

        readonly DbDataSource _dataSource;
        readonly SqlDialect _dialect;
        readonly AdoMetadataProvider _metadata;
        readonly AdoConvention _convention;
        readonly string _databaseName;
        readonly string _schemaName;

        LoadingCacheLookup? _tables;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="dialect"></param>
        /// <param name="metadata"></param>
        /// <param name="convention"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        public AdoSchema(DbDataSource dataSource, SqlDialect dialect, AdoMetadataProvider metadata, AdoConvention convention, string databaseName, string schemaName)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _convention = convention ?? throw new ArgumentNullException(nameof(convention));
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _schemaName = schemaName ?? throw new ArgumentNullException(nameof(schemaName));
        }

        /// <summary>
        /// Gets the ADO connection provider.
        /// </summary>
        public DbDataSource DataSource => _dataSource;

        /// <summary>
        /// Gets the SQL dialect.
        /// </summary>
        public SqlDialect Dialect => _dialect;

        /// <summary>
        /// Gets the convention.
        /// </summary>
        public AdoConvention Convention => _convention;

        /// <summary>
        /// Gets the database refered to by this <see cref="AdoSchema"/>.
        /// </summary>
        public string DatabaseName => _databaseName;

        /// <summary>
        /// Gets the schema refered to by this <see cref="AdoSchema"/>.
        /// </summary>
        public string SchemaName => _schemaName;

        /// <inheritdoc />
        public override Lookup tables()
        {
            if (_tables is null)
                Interlocked.CompareExchange(ref _tables, new LoadingCacheLookup(new TablesLookup(_metadata, _databaseName, _schemaName)), null);

            return _tables;
        }

        /// <inheritdoc />
        public override Lookup subSchemas()
        {
            return Lookup.empty();
        }

        /// <inheritdoc />
        public override Expression getExpression(SchemaPlus parentSchema, string name)
        {
            return Schemas.subSchemaExpression(parentSchema, name, typeof(AdoSchema));
        }

        /// <summary>
        /// Gets the <see cref="RelProtoDataType"/> for a given table name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        internal RelProtoDataType GetRelDataType(string tableName)
        {
            var metadata = _metadata.GetFields(_databaseName, _schemaName, tableName);
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var fieldBuilder = typeFactory.builder();

            foreach (var field in metadata)
            {
                if (field.Name is null)
                    throw new AdoSchemaException("Null value encountered for field name.");

                fieldBuilder.add(field.Name, GetSqlType(typeFactory, field.DbType, field.Precision ?? 0, field.Scale ?? 0, field.Size ?? 0)).nullable(field.Nullable);
            }

            return RelDataTypeImpl.proto(fieldBuilder.build());
        }

        /// <summary>
        /// Transforms a <see cref="DbType"/> and its various additional information into a <see cref="RelDataType"/>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="dbType"></param>
        /// <param name="precision"></param>
        /// <param name="scale"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        internal RelDataType GetSqlType(RelDataTypeFactory typeFactory, DbType dbType, int precision, int scale, int size)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
                case DbType.Binary:
                    break;
                case DbType.Byte:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                case DbType.Boolean:
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case DbType.Currency:
                    break;
                case DbType.Date:
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case DbType.DateTime:
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                case DbType.Decimal:
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL);
                case DbType.Double:
                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                case DbType.Guid:
                    break;
                case DbType.Int16:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case DbType.Int32:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case DbType.Int64:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case DbType.Object:
                    break;
                case DbType.SByte:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                case DbType.Single:
                    break;
                case DbType.String:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR);
                case DbType.Time:
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                case DbType.UInt16:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case DbType.UInt32:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case DbType.UInt64:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case DbType.VarNumeric:
                    break;
                case DbType.AnsiStringFixedLength:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, size);
                case DbType.StringFixedLength:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, size);
                case DbType.Xml:
                    break;
                case DbType.DateTime2:
                    break;
                case DbType.DateTimeOffset:
                    break;
            }

            throw new AdoSchemaException($"Unsupported database type: {dbType}.");
        }
    }

}
