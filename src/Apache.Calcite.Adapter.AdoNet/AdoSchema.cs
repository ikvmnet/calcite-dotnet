using System;
using org.apache.calcite.sql.type;
using org.apache.calcite.rel.type;
using System.Data;
using System.Data.Common;
using System.Threading;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.schema;
using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A Calcite <see cref="Schema"/> whose tables are the tables of one schema of an ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Queries against these tables are planned into the schema's <see cref="AdoConvention"/>, and as much of
    /// each query as the source's dialect can express is sent to the source as one SQL statement.
    /// </para>
    /// <para>
    /// Table names come from <see cref="AdoDatabaseMetadata.GetTables"/>, and a name can be matched with or
    /// without regard to case, as the connection asks. A table that has been looked up is cached for a minute
    /// (Calcite's <c>LoadingCacheLookup</c>), and its columns are read from
    /// <see cref="AdoDatabaseMetadata.GetFields"/> when its row type is first needed.
    /// </para>
    /// <para>
    /// <see cref="unwrap"/> answers this schema and its <see cref="AdoDataSource"/>.
    /// </para>
    /// </remarks>
    public class AdoSchema : AdoBaseSchema, Schema, Wrapper
    {

        /// <summary>
        /// Puts this assembly and the ADO.NET assemblies on IKVM's boot class path, so that Java code
        /// generated for a plan can name their types.
        /// </summary>
        static AdoSchema()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchema).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(DbCommand).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(Action<DbCommand>).Assembly);
        }

        /// <summary>
        /// Resolves the tables of an <see cref="AdoSchema"/> from its data source's metadata.
        /// </summary>
        class TablesLookup : IgnoreCaseLookup
        {

            readonly AdoSchema _schema;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="schema">The schema whose tables are resolved.</param>
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
        /// Creates a schema over a <see cref="DbDataSource"/>, choosing its metadata provider with
        /// <see cref="AdoDatabaseMetadataFactoryImpl"/>.
        /// </summary>
        /// <param name="parentSchema">The schema the new schema will be added to. Generated code locates the schema
        /// through it, so it cannot be <see langword="null"/>.</param>
        /// <param name="name">The name the new schema will be added under. It must match the name used when adding it.</param>
        /// <param name="dataSource">The data source that opens connections to the database.</param>
        /// <param name="databaseName">The database whose tables to expose, or <see langword="null"/> for the provider's default.</param>
        /// <param name="schemaName">The schema whose tables to expose, or <see langword="null"/> for the provider's default.</param>
        /// <returns>The new schema. The caller adds it to <paramref name="parentSchema"/>.</returns>
        /// <exception cref="AdoCalciteException">The connection's provider is not one <see cref="AdoDatabaseMetadataFactoryImpl"/> recognises.</exception>
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, DbDataSource dataSource, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, dataSource, AdoDatabaseMetadataFactoryImpl.Instance, databaseName, schemaName);
        }

        /// <summary>
        /// Creates a schema over a <see cref="DbDataSource"/>, choosing its metadata provider with
        /// <paramref name="metadataFactory"/>.
        /// </summary>
        /// <param name="parentSchema">The schema the new schema will be added to. Generated code locates the schema
        /// through it, so it cannot be <see langword="null"/>.</param>
        /// <param name="name">The name the new schema will be added under. It must match the name used when adding it.</param>
        /// <param name="dataSource">The data source that opens connections to the database.</param>
        /// <param name="metadataFactory">Creates the metadata provider for <paramref name="dataSource"/>.</param>
        /// <param name="databaseName">The database whose tables to expose, or <see langword="null"/> for the provider's default.</param>
        /// <param name="schemaName">The schema whose tables to expose, or <see langword="null"/> for the provider's default.</param>
        /// <returns>The new schema. The caller adds it to <paramref name="parentSchema"/>.</returns>
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, DbDataSource dataSource, AdoDatabaseMetadataFactory metadataFactory, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, new DbDataSourceAdoDataSource(dataSource, metadataFactory.Create(dataSource)), databaseName, schemaName);
        }

        /// <summary>
        /// Creates a schema over a <see cref="DbDataSource"/> with a given metadata provider.
        /// </summary>
        /// <param name="parentSchema">The schema the new schema will be added to. Generated code locates the schema
        /// through it, so it cannot be <see langword="null"/>.</param>
        /// <param name="name">The name the new schema will be added under. It must match the name used when adding it.</param>
        /// <param name="dataSource">The data source that opens connections to the database.</param>
        /// <param name="metadataProvider">Describes the database's tables, columns, dialect and parameter syntax.</param>
        /// <param name="databaseName">The database whose tables to expose, or <see langword="null"/> for the provider's default.</param>
        /// <param name="schemaName">The schema whose tables to expose, or <see langword="null"/> for the provider's default.</param>
        /// <returns>The new schema. The caller adds it to <paramref name="parentSchema"/>.</returns>
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, DbDataSource dataSource, AdoDatabaseMetadata metadataProvider, string? databaseName, string? schemaName)
        {
            return Create(parentSchema, name, new DbDataSourceAdoDataSource(dataSource, metadataProvider), databaseName, schemaName);
        }

        /// <summary>
        /// Creates a schema over an <see cref="AdoDataSource"/>.
        /// </summary>
        /// <param name="parentSchema">The schema the new schema will be added to. Generated code locates the schema
        /// through it, so it cannot be <see langword="null"/>.</param>
        /// <param name="name">The name the new schema will be added under. It must match the name used when adding it.</param>
        /// <param name="dataSource">The data source, which carries its own metadata provider.</param>
        /// <param name="databaseName">The database whose tables to expose. Null or blank means
        /// <see cref="AdoDatabaseMetadata.GetDefaultDatabase"/>.</param>
        /// <param name="schemaName">The schema whose tables to expose. Null or blank means
        /// <see cref="AdoDatabaseMetadata.GetDefaultSchema"/>.</param>
        /// <returns>The new schema. The caller adds it to <paramref name="parentSchema"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// Resolving a default database may open a connection. The dialect is read from the metadata here, which
        /// for SQL Server, ODBC and OLE DB also opens a connection to ask the server what it is.
        /// </remarks>
        public static AdoSchema Create(SchemaPlus? parentSchema, string name, AdoDataSource dataSource, string? databaseName, string? schemaName)
        {
            ArgumentNullException.ThrowIfNull(dataSource);

            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = dataSource.Metadata.GetDefaultDatabase();

            if (string.IsNullOrWhiteSpace(schemaName))
                schemaName = dataSource.Metadata.GetDefaultSchema();

            var expression = Schemas.subSchemaExpression(parentSchema, name, typeof(AdoSchema));
            var convention = AdoConvention.Create(dataSource.Metadata.Dialect, dataSource.Metadata.Syntax, expression, name);
            return new AdoSchema(dataSource, convention, databaseName, schemaName);
        }

        /// <summary>
        /// Creates a schema from the operands of a Calcite JSON model.
        /// </summary>
        /// <param name="parentSchema">The schema the new schema will be added to.</param>
        /// <param name="name">The name the new schema will be added under.</param>
        /// <param name="operand">The model's operands. Every value is a string:
        /// <list type="bullet">
        /// <item><c>adoProviderName</c> and <c>adoConnectionString</c>: the invariant name of a provider registered
        /// with <see cref="DbProviderFactories"/>, and the connection string to give it. Required unless
        /// <c>adoDataSource</c> is given.</item>
        /// <item><c>adoDataSource</c>: the assembly-qualified name of a <see cref="DbDataSource"/> type with a
        /// public parameterless constructor, used in place of the two above.</item>
        /// <item><c>adoDatabaseMetadata</c>: the assembly-qualified name of an <see cref="AdoDatabaseMetadata"/> type
        /// with a public constructor taking a <see cref="DbDataSource"/>, used in place of the provider
        /// <see cref="AdoDatabaseMetadataFactoryImpl"/> would choose.</item>
        /// <item><c>adoDatabase</c> and <c>adoSchema</c>: the database and schema whose tables to expose. Either
        /// may be omitted to take the provider's default.</item>
        /// </list>
        /// </param>
        /// <returns>The new schema.</returns>
        /// <exception cref="AdoCalciteException">A required operand is missing, or a named type cannot be loaded or
        /// is not of the expected kind.</exception>
        /// <remarks>
        /// An <c>adoDatabaseMetadataFactory</c> operand is read only when no factory has already been chosen, and
        /// one always has been, so it has no effect.
        /// </remarks>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, Map operand)
        {
            AdoDataSource? adoDataSource = null;
            AdoDatabaseMetadata? adoDatabaseMetadata = null;
            AdoDatabaseMetadataFactory? adoDatabaseMetadataFactory = AdoDatabaseMetadataFactoryImpl.Instance;

            var adoDatabaseMetadataName = (string?)operand.get("adoDatabaseMetadata");
            if (string.IsNullOrWhiteSpace(adoDatabaseMetadataName) == false)
            {
                var adoDatabaseMetadataType = Type.GetType(adoDatabaseMetadataName);
                if (adoDatabaseMetadataType is null)
                    throw new AdoCalciteException($"Failed to instantiate AdoDatabaseMetadata type: {adoDatabaseMetadataName}.");

                adoDatabaseMetadataFactory = new AdoDatabaseMetadataTypeFactory(adoDatabaseMetadataType);
            }

            // TODO: adoDatabaseMetadataFactory starts non-null, so this branch never runs and the
            // adoDatabaseMetadataFactory operand is ignored.
            if (adoDatabaseMetadataFactory == null)
            {
                var adoDatabaseMetadataFactoryName = (string?)operand.get("adoDatabaseMetadataFactory");
                if (string.IsNullOrWhiteSpace(adoDatabaseMetadataFactoryName) == false)
                {
                    var adoDatabaseMetadataFactoryType = Type.GetType(adoDatabaseMetadataFactoryName);
                    if (adoDatabaseMetadataFactoryType is null)
                        throw new AdoCalciteException($"Failed to instantiate AdoDatabaseMetadataFactory type: {adoDatabaseMetadataFactoryName}.");

                    adoDatabaseMetadataFactory = Activator.CreateInstance(adoDatabaseMetadataFactoryType) as AdoDatabaseMetadataFactory;
                    if (adoDatabaseMetadataFactory is null)
                        throw new AdoCalciteException($"Could not create instance of type '{adoDatabaseMetadataFactoryType.FullName}' as AdoDatabaseMetadataFactory.");
                }
            }

            if (adoDatabaseMetadataFactory == null)
                throw new AdoCalciteException("Could not establish AdoDatabaseMetadataFactory.");

            var adoDataSourceName = (string)operand.get("adoDataSource");
            if (adoDataSourceName != null)
            {
                var dbDataSourceType = Type.GetType(adoDataSourceName);
                if (dbDataSourceType is null)
                    throw new AdoCalciteException($"Failed to instantiate DbDataSource type: {adoDataSourceName}.");

                if (Activator.CreateInstance(dbDataSourceType) is not DbDataSource dbDataSource)
                    throw new AdoCalciteException($"Failed to instantiate DbDataSource type: {dbDataSourceType.FullName}.");

                adoDataSource = new DbDataSourceAdoDataSource(dbDataSource, adoDatabaseMetadata ?? adoDatabaseMetadataFactory.Create(dbDataSource));
            }

            if (adoDataSource is null)
            {
                var adoProviderName = (string?)operand.get("adoProviderName");
                if (adoProviderName is null || string.IsNullOrWhiteSpace(adoProviderName))
                    throw new AdoCalciteException("Required missing property 'adoProviderName'.");

                var adoConnectionString = (string?)operand.get("adoConnectionString");
                if (adoConnectionString is null || string.IsNullOrWhiteSpace(adoConnectionString))
                    throw new AdoCalciteException("Required missing property 'adoConnectionString'.");

                var dbFactory = DbProviderFactories.GetFactory(adoProviderName);
                var dbDataSource = dbFactory.CreateDataSource(adoConnectionString);
                adoDataSource = new DbProviderAdoDataSource(dbFactory, adoConnectionString, adoDatabaseMetadata ?? adoDatabaseMetadataFactory.Create(dbDataSource));
                if (adoDataSource is null)
                    throw new AdoCalciteException("Failed to instantiate DbDataSource from adoProviderName and adoConnectionString.");
            }

            return Create(
                parentSchema,
                name,
                adoDataSource,
                (string?)operand.get("adoDatabase"),
                (string?)operand.get("adoSchema"));
        }

        readonly AdoDataSource _dataSource;
        readonly AdoConvention _convention;
        readonly string? _databaseName;
        readonly string? _schemaName;

        LoadingCacheLookup? _tables;

        /// <summary>
        /// Initializes a new instance. <c>Create</c> is the usual
        /// way to make one, since it builds the convention and resolves the default database and schema.
        /// </summary>
        /// <param name="dataSource">The data source the tables are read from.</param>
        /// <param name="convention">The convention queries against these tables are planned into.</param>
        /// <param name="databaseName">The database whose tables to expose, passed to the metadata as given.</param>
        /// <param name="schemaName">The schema whose tables to expose, passed to the metadata as given.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/> or <paramref name="convention"/> is <see langword="null"/>.</exception>
        public AdoSchema(AdoDataSource dataSource, AdoConvention convention, string? databaseName, string? schemaName)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _convention = convention ?? throw new ArgumentNullException(nameof(convention));
            _databaseName = databaseName;
            _schemaName = schemaName;
        }

        /// <summary>
        /// Gets the data source the tables are read from.
        /// </summary>
        internal AdoDataSource DataSource => _dataSource;

        /// <summary>
        /// Gets the convention queries against these tables are planned into.
        /// </summary>
        internal AdoConvention Convention => _convention;

        /// <summary>
        /// Gets the database whose tables this schema exposes, or <see langword="null"/> where the provider has
        /// no default and none was given.
        /// </summary>
        public string? DatabaseName => _databaseName;

        /// <summary>
        /// Gets the schema whose tables this schema exposes, or <see langword="null"/> where the provider has no
        /// default and none was given. For ODBC and OLE DB, <see langword="null"/> means every schema.
        /// </summary>
        public string? SchemaName => _schemaName;

        /// <summary>
        /// Returns the row type of a table, read from this schema's metadata. Mirrors the three-argument
        /// <c>JdbcSchema.getRelDataType</c>, with <see cref="AdoDatabaseMetadata"/> in place of the
        /// <c>DatabaseMetaData</c> it opens a connection for.
        /// </summary>
        /// <param name="databaseName">The table's database.</param>
        /// <param name="schemaName">The table's schema.</param>
        /// <param name="tableName">The table's name.</param>
        /// <returns>A prototype of the row type.</returns>
        /// <exception cref="AdoCalciteException">A column has no name, or a type this adapter cannot map.</exception>
        internal RelProtoDataType GetRelDataType(string? databaseName, string? schemaName, string tableName)
        {
            return GetRelDataType(_dataSource.Metadata, databaseName, schemaName, tableName);
        }

        /// <summary>
        /// Returns the row type of a table from column metadata. Mirrors <c>JdbcSchema.getRelDataType</c>, with
        /// <see cref="AdoDatabaseMetadata"/> in place of <c>DatabaseMetaData</c>.
        /// </summary>
        /// <param name="metaData">The metadata to read the columns from.</param>
        /// <param name="databaseName">The table's database.</param>
        /// <param name="schemaName">The table's schema.</param>
        /// <param name="tableName">The table's name.</param>
        /// <returns>A prototype of the row type.</returns>
        /// <exception cref="AdoCalciteException">A column has no name, or a type this adapter cannot map.</exception>
        internal RelProtoDataType GetRelDataType(AdoDatabaseMetadata metaData, string? databaseName, string? schemaName, string tableName)
        {
            // a temporary type factory is enough, as in JdbcSchema: the prototype copies the type into the
            // caller's factory
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var types = typeFactory.builder();

            foreach (var field in metaData.GetFields(databaseName, schemaName, tableName))
            {
                if (field.Name is null)
                    throw new AdoCalciteException("Null value encountered for field name.");

                types.add(field.Name, SqlType(typeFactory, field.DbType, field.Precision ?? -1, field.Scale ?? -1, field.Size ?? -1)).nullable(field.Nullable);
            }

            return RelDataTypeImpl.proto(types.build());
        }

        /// <summary>
        /// Returns the Calcite type for a column's <see cref="DbType"/>, length, precision and scale. Takes the
        /// place of <c>JdbcSchema.sqlType</c>.
        /// </summary>
        /// <param name="typeFactory">The factory to create the type in.</param>
        /// <param name="dbType">The column's type.</param>
        /// <param name="precision">The column's precision, or -1.</param>
        /// <param name="scale">The column's scale, or -1.</param>
        /// <param name="size">The column's length, or -1.</param>
        /// <returns>The type.</returns>
        /// <exception cref="AdoCalciteException"><paramref name="dbType"/> has no mapping.</exception>
        static RelDataType SqlType(RelDataTypeFactory typeFactory, DbType dbType, int precision, int scale, int size)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
                case DbType.Binary:
                    return typeFactory.createSqlType(SqlTypeName.VARBINARY, size);
                // DbType.Byte is unsigned (0..255) and TINYINT is signed, so it maps to UTINYINT
                case DbType.Byte:
                    return typeFactory.createSqlType(SqlTypeName.UTINYINT);
                case DbType.Boolean:
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                // money has four decimal places wherever a provider has a distinct type for it
                case DbType.Currency:
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL, 19, 4);
                case DbType.Date:
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case DbType.DateTime:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case DbType.Decimal:
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                case DbType.Double:
                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                case DbType.Guid:
                    return typeFactory.createSqlType(SqlTypeName.UUID);
                case DbType.Int16:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case DbType.Int32:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case DbType.Int64:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                // a column of unknown type is read as OTHER and its value passed through, so the rest of the
                // table stays usable
                case DbType.Object:
                    return typeFactory.createSqlType(SqlTypeName.OTHER);
                case DbType.SByte:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                // Calcite's REAL is four bytes and DOUBLE eight
                case DbType.Single:
                    return typeFactory.createSqlType(SqlTypeName.REAL);
                case DbType.String:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
                case DbType.Time:
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                case DbType.UInt16:
                    return typeFactory.createSqlType(SqlTypeName.USMALLINT);
                case DbType.UInt32:
                    return typeFactory.createSqlType(SqlTypeName.UINTEGER);
                case DbType.UInt64:
                    return typeFactory.createSqlType(SqlTypeName.UBIGINT);
                case DbType.VarNumeric:
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                case DbType.AnsiStringFixedLength:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, size);
                case DbType.StringFixedLength:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, size);
                case DbType.Xml:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
                case DbType.DateTime2:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case DbType.DateTimeOffset:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ);
            }

            throw new AdoCalciteException($"Unsupported database type: {dbType}.");
        }

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
        /// Returns this schema if it is an instance of <paramref name="clazz"/>, its <see cref="AdoDataSource"/> if
        /// <paramref name="clazz"/> is that type, and otherwise <see langword="null"/>.
        /// </summary>
        /// <param name="clazz">The class to unwrap to.</param>
        /// <returns>The object, or <see langword="null"/>.</returns>
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
