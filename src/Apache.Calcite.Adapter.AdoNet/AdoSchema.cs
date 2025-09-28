using System;
using System.Data;
using System.Data.Common;
using System.Threading;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.avatica;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;
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
    public class AdoSchema : AdoBaseSchema, Schema, Wrapper
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        static AdoSchema()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchema).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(DbCommand).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(Action<DbCommand>).Assembly);
        }

        /// <summary>
        /// Lookup for resolving the tables of this schema.
        /// </summary>
        class TablesLookup : IgnoreCaseLookup
        {

            readonly AdoSchema _schema;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="schema"></param>
            public TablesLookup(AdoSchema schema)
            {
                _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            }

            /// <inheritdoc />
            public override Set getNames(LikePattern pattern)
            {
                var builder = ImmutableSet.builder();

                foreach (var table in _schema.DataSource.Metadata.GetTables(_schema.DatabaseName, _schema.SchemaName))
                    if (pattern.matcher().apply(table.Name))
                        builder.add(table.Name);

                return builder.build();
            }

            /// <inheritdoc />
            public override object? get(string name)
            {
                foreach (var table in _schema.DataSource.Metadata.GetTables(_schema.DatabaseName, _schema.SchemaName))
                    if (table.Name == name)
                        return new AdoTable(_schema, table.DatabaseName, table.SchemaName, table.Name, Schema.TableType.TABLE);

                return null;
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
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, DbDataSource dataSource, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, dataSource, AdoDatabaseMetadataFactoryImpl.Instance, databaseName, schemaName);
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
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, DbDataSource dataSource, AdoDatabaseMetadataFactory metadataFactory, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, new DbDataSourceAdoDataSource(dataSource, metadataFactory.Create(dataSource)), databaseName, schemaName);
        }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="dataSource"></param>
        /// <param name="metadataProvider"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, DbDataSource dataSource, AdoDatabaseMetadata metadataProvider, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, new DbDataSourceAdoDataSource(dataSource, metadataProvider), databaseName, schemaName);
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
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, AdoDataSource dataSource, string? databaseName, string? schemaName)
        {
            ArgumentNullException.ThrowIfNull(dataSource);

            // fallback to current connection database
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = dataSource.Metadata.GetDefaultDatabase();
            if (string.IsNullOrWhiteSpace(databaseName))
                throw new AdoSchemaException("Could not determine database name.");

            // fallback to current connection schema
            if (string.IsNullOrWhiteSpace(schemaName))
                schemaName = dataSource.Metadata.GetDefaultSchema();
            if (string.IsNullOrWhiteSpace(schemaName))
                throw new AdoSchemaException("Could not determine schema name.");

            // generate schema
            var expression = Schemas.subSchemaExpression(parentSchema, name, typeof(AdoSchema));
            var convention = AdoConvention.Create(dataSource.Metadata.GetDialect(), expression, name);
            return new AdoSchema(dataSource, convention, databaseName, schemaName);
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
            AdoDataSource? adoDataSource = null;
            AdoDatabaseMetadataFactory? metadataFactory = null;

            // configure metadata factory
            var adoMetadataFactoryName = (string?)operand.get("adoMetadataFactory");
            if (string.IsNullOrWhiteSpace(adoMetadataFactoryName) == false)
                metadataFactory = (AdoDatabaseMetadataFactory)AvaticaUtils.instantiatePlugin(typeof(AdoDatabaseMetadataFactory), adoMetadataFactoryName);
            else
                metadataFactory = AdoDatabaseMetadataFactoryImpl.Instance;

            // data source explicitly specified
            var adoDataSourceName = (string)operand.get("adoDataSource");
            if (adoDataSourceName != null)
            {
                var dbDataSource = (DbDataSource)AvaticaUtils.instantiatePlugin(typeof(DbDataSource), adoDataSourceName);
                if (dbDataSource is null)
                    throw new AdoSchemaException("Failed to instantiate DbDataSource plugin.");

                // create new data source from data source and metadata
                adoDataSource = new DbDataSourceAdoDataSource(dbDataSource, metadataFactory.Create(dbDataSource));
            }

            // fallback to provider name and connection string
            if (adoDataSource is null)
            {
                var adoProviderName = (string?)operand.get("adoProviderName");
                if (adoProviderName is null || string.IsNullOrWhiteSpace(adoProviderName))
                    throw new AdoSchemaException("Required missing property 'adoProviderName'.");

                var adoConnectionString = (string?)operand.get("adoConnectionString");
                if (adoConnectionString is null || string.IsNullOrWhiteSpace(adoConnectionString))
                    throw new AdoSchemaException("Required missing property 'adoConnectionString'.");

                adoDataSource = CreateDataSource(adoProviderName, adoConnectionString, metadataFactory);
                if (adoDataSource is null)
                    throw new AdoSchemaException("Failed to instantiate DbDataSource from providerName and connectionString.");
            }

            return Create(
                parentSchema,
                name,
                adoDataSource,
                (string?)operand.get("adoDatabase"),
                (string?)operand.get("adoSchema"));
        }

        /// <summary>
        /// Gets a <see cref="AdoDataSource"/> based on the given provider name and connection string and metadata factory.
        /// </summary>
        /// <param name="providerName"></param>
        /// <param name="connectionString"></param>
        /// <param name="metadataFactory"></param>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        public static AdoDataSource CreateDataSource(string providerName, string connectionString, AdoDatabaseMetadataFactory metadataFactory)
        {
            ArgumentException.ThrowIfNullOrEmpty(providerName);
            ArgumentException.ThrowIfNullOrEmpty(connectionString);
            ArgumentNullException.ThrowIfNull(metadataFactory);

            var factory = DbProviderFactories.GetFactory(providerName);
            var dataSource = factory.CreateDataSource(connectionString);
            var metadata = metadataFactory.Create(dataSource);
            return new DbProviderAdoDataSource(factory, connectionString, metadata);
        }

        /// <summary>
        /// Gets a <see cref="AdoDataSource"/> based on the given provider name and connection string and metadata factory.
        /// </summary>
        /// <param name="providerName"></param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        public static AdoDataSource CreateDataSource(string providerName, string connectionString)
        {
            return CreateDataSource(providerName, connectionString, AdoDatabaseMetadataFactoryImpl.Instance);
        }

        readonly AdoDataSource _dataSource;
        readonly AdoConvention _convention;
        readonly string? _databaseName;
        readonly string? _schemaName;

        LoadingCacheLookup? _tables;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="convention"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        public AdoSchema(AdoDataSource dataSource, AdoConvention convention, string? databaseName, string? schemaName)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _convention = convention ?? throw new ArgumentNullException(nameof(convention));
            _databaseName = databaseName;
            _schemaName = schemaName;
        }

        /// <summary>
        /// Gets the ADO data source.
        /// </summary>
        internal AdoDataSource DataSource => _dataSource;

        /// <summary>
        /// Gets the convention.
        /// </summary>
        internal AdoConvention Convention => _convention;

        /// <summary>
        /// Gets the database refered to by this <see cref="AdoSchema"/>.
        /// </summary>
        public string? DatabaseName => _databaseName;

        /// <summary>
        /// Gets the schema refered to by this <see cref="AdoSchema"/>.
        /// </summary>
        public string? SchemaName => _schemaName;

        /// <inheritdoc />
        public override Lookup tables()
        {
            if (_tables is null)
                Interlocked.CompareExchange(ref _tables, new LoadingCacheLookup(new TablesLookup(this)), null);

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
        /// Gets the <see cref="RelProtoDataType"/> for a given table.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        internal RelProtoDataType GetRowProtoDataType(string? databaseName, string? schemaName, string tableName)
        {
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var types = typeFactory.builder();

            // derive a type for each field
            foreach (var field in _dataSource.Metadata.GetFields(databaseName, schemaName, tableName))
            {
                if (field.Name is null)
                    throw new AdoSchemaException("Null value encountered for field name.");

                types.add(field.Name, GetSqlType(typeFactory, field.DbType, field.Precision ?? -1, field.Scale ?? -1, field.Size ?? -1)).nullable(field.Nullable);
            }

            return RelDataTypeImpl.proto(types.build());
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
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case DbType.Decimal:
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                case DbType.Double:
                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                case DbType.Guid:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, 36);
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
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
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
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case DbType.DateTimeOffset:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ);
            }

            throw new AdoSchemaException($"Unsupported database type: {dbType}.");
        }

        /// <inheritdoc />
        public object? unwrap(Class clazz)
        {
            if (clazz.isInstance(this))
                return clazz.cast(this);

            if (clazz == (Class)typeof(AdoDataSource))
                return clazz.cast(DataSource);

            return null;
        }

        /// <inheritdoc />
        public object unwrapOrThrow(Class aClass)
        {
            return Wrapper.__DefaultMethods.unwrapOrThrow(this, aClass);
        }

        /// <inheritdoc />
        public Optional maybeUnwrap(Class aClass)
        {
            return Wrapper.__DefaultMethods.maybeUnwrap(this, aClass);
        }

    }

}
