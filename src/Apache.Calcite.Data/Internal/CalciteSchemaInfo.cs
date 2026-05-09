using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.avatica.util;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;


namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Builds the standard ADO.NET metadata <see cref="DataTable"/>s served by
    /// <see cref="CalciteConnection.GetSchema()"/> overloads by walking the engine's
    /// <see cref="SchemaPlus"/> tree.
    /// </summary>
    internal static class CalciteSchemaInfo
    {

        public static readonly string MetaDataCollections = DbMetaDataCollectionNames.MetaDataCollections;
        public static readonly string Restrictions = DbMetaDataCollectionNames.Restrictions;
        public static readonly string DataSourceInformation = DbMetaDataCollectionNames.DataSourceInformation;
        public static readonly string DataTypes = DbMetaDataCollectionNames.DataTypes;
        public static readonly string ReservedWords = DbMetaDataCollectionNames.ReservedWords;
        public const string Schemas = "Schemas";
        public const string Tables = "Tables";
        public const string Columns = "Columns";
        public const string Views = "Views";
        public const string ViewColumns = "ViewColumns";
        public const string Indexes = "Indexes";
        public const string IndexColumns = "IndexColumns";
        public const string PrimaryKeys = "PrimaryKeys";
        public const string ForeignKeys = "ForeignKeys";
        public const string Procedures = "Procedures";
        public const string ProcedureParameters = "ProcedureParameters";
        public const string Functions = "Functions";
        public const string FunctionParameters = "FunctionParameters";
        public const string UserDefinedTypes = "UserDefinedTypes";

        /// <summary>
        /// Returns the names and shapes of every metadata collection supported by the provider.
        /// </summary>
        public static DataTable BuildMetaDataCollections()
        {
            var t = new DataTable(MetaDataCollections);
            t.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.NumberOfRestrictions, typeof(int));
            t.Columns.Add(DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int));

            t.Rows.Add(MetaDataCollections, 0, 0);
            t.Rows.Add(Restrictions, 0, 0);
            t.Rows.Add(DataSourceInformation, 0, 0);
            t.Rows.Add(DataTypes, 0, 0);
            t.Rows.Add(ReservedWords, 0, 0);
            t.Rows.Add(Schemas, 1, 1);
            t.Rows.Add(Tables, 3, 3);
            t.Rows.Add(Columns, 4, 4);
            t.Rows.Add(Views, 3, 3);
            t.Rows.Add(ViewColumns, 4, 4);
            t.Rows.Add(Indexes, 4, 4);
            t.Rows.Add(IndexColumns, 5, 5);
            t.Rows.Add(PrimaryKeys, 4, 4);
            t.Rows.Add(ForeignKeys, 4, 4);
            t.Rows.Add(Procedures, 4, 4);
            t.Rows.Add(ProcedureParameters, 5, 5);
            t.Rows.Add(Functions, 4, 4);
            t.Rows.Add(FunctionParameters, 5, 5);
            t.Rows.Add(UserDefinedTypes, 2, 2);

            return t;
        }

        /// <summary>
        /// Returns the restriction descriptors that may be supplied to <c>GetSchema(string, string?[])</c>.
        /// </summary>
        public static DataTable BuildRestrictions()
        {
            var t = new DataTable(Restrictions);
            t.Columns.Add(DbMetaDataColumnNames.CollectionName, typeof(string));
            t.Columns.Add("RestrictionName", typeof(string));
            t.Columns.Add("RestrictionDefault", typeof(string));
            t.Columns.Add("RestrictionNumber", typeof(int));

            t.Rows.Add(Schemas, "Schema", null, 1);

            t.Rows.Add(Tables, "Catalog", null, 1);
            t.Rows.Add(Tables, "Schema", null, 2);
            t.Rows.Add(Tables, "Table", null, 3);

            t.Rows.Add(Columns, "Catalog", null, 1);
            t.Rows.Add(Columns, "Schema", null, 2);
            t.Rows.Add(Columns, "Table", null, 3);
            t.Rows.Add(Columns, "Column", null, 4);

            t.Rows.Add(Views, "Catalog", null, 1);
            t.Rows.Add(Views, "Schema", null, 2);
            t.Rows.Add(Views, "View", null, 3);

            t.Rows.Add(ViewColumns, "Catalog", null, 1);
            t.Rows.Add(ViewColumns, "Schema", null, 2);
            t.Rows.Add(ViewColumns, "View", null, 3);
            t.Rows.Add(ViewColumns, "Column", null, 4);

            t.Rows.Add(Indexes, "Catalog", null, 1);
            t.Rows.Add(Indexes, "Schema", null, 2);
            t.Rows.Add(Indexes, "Table", null, 3);
            t.Rows.Add(Indexes, "Index", null, 4);

            t.Rows.Add(IndexColumns, "Catalog", null, 1);
            t.Rows.Add(IndexColumns, "Schema", null, 2);
            t.Rows.Add(IndexColumns, "Table", null, 3);
            t.Rows.Add(IndexColumns, "Index", null, 4);
            t.Rows.Add(IndexColumns, "Column", null, 5);

            t.Rows.Add(PrimaryKeys, "Catalog", null, 1);
            t.Rows.Add(PrimaryKeys, "Schema", null, 2);
            t.Rows.Add(PrimaryKeys, "Table", null, 3);
            t.Rows.Add(PrimaryKeys, "Constraint", null, 4);

            t.Rows.Add(ForeignKeys, "Catalog", null, 1);
            t.Rows.Add(ForeignKeys, "Schema", null, 2);
            t.Rows.Add(ForeignKeys, "Table", null, 3);
            t.Rows.Add(ForeignKeys, "Constraint", null, 4);

            t.Rows.Add(Procedures, "Catalog", null, 1);
            t.Rows.Add(Procedures, "Schema", null, 2);
            t.Rows.Add(Procedures, "Procedure", null, 3);
            t.Rows.Add(Procedures, "ProcedureType", null, 4);

            t.Rows.Add(ProcedureParameters, "Catalog", null, 1);
            t.Rows.Add(ProcedureParameters, "Schema", null, 2);
            t.Rows.Add(ProcedureParameters, "Procedure", null, 3);
            t.Rows.Add(ProcedureParameters, "ProcedureType", null, 4);
            t.Rows.Add(ProcedureParameters, "Parameter", null, 5);

            t.Rows.Add(Functions, "Catalog", null, 1);
            t.Rows.Add(Functions, "Schema", null, 2);
            t.Rows.Add(Functions, "Function", null, 3);
            t.Rows.Add(Functions, "FunctionType", null, 4);

            t.Rows.Add(FunctionParameters, "Catalog", null, 1);
            t.Rows.Add(FunctionParameters, "Schema", null, 2);
            t.Rows.Add(FunctionParameters, "Function", null, 3);
            t.Rows.Add(FunctionParameters, "FunctionType", null, 4);
            t.Rows.Add(FunctionParameters, "Parameter", null, 5);

            t.Rows.Add(UserDefinedTypes, "Catalog", null, 1);
            t.Rows.Add(UserDefinedTypes, "Type", null, 2);

            return t;
        }

        /// <summary>
        /// Returns metadata describing the data source itself.
        /// </summary>
        public static DataTable BuildDataSourceInformation(CalciteConnection connection)
        {
            var t = new DataTable(DataSourceInformation);
            t.Columns.Add(DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.DataSourceProductName, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.DataSourceProductVersion, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.DataSourceProductVersionNormalized, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.GroupByBehavior, typeof(GroupByBehavior));
            t.Columns.Add(DbMetaDataColumnNames.IdentifierPattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.IdentifierCase, typeof(IdentifierCase));
            t.Columns.Add(DbMetaDataColumnNames.OrderByColumnsInSelect, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.ParameterMarkerFormat, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.ParameterMarkerPattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.ParameterNameMaxLength, typeof(int));
            t.Columns.Add(DbMetaDataColumnNames.ParameterNamePattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.QuotedIdentifierPattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.QuotedIdentifierCase, typeof(IdentifierCase));
            t.Columns.Add(DbMetaDataColumnNames.StatementSeparatorPattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.StringLiteralPattern, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.SupportedJoinOperators, typeof(SupportedJoinOperators));

            var row = t.NewRow();
            var config = connection.Config;
            row[DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern] = @"\.";
            row[DbMetaDataColumnNames.DataSourceProductName] = "Apache Calcite";
            row[DbMetaDataColumnNames.DataSourceProductVersion] = connection.ServerVersion;
            row[DbMetaDataColumnNames.DataSourceProductVersionNormalized] = connection.ServerVersion;
            row[DbMetaDataColumnNames.GroupByBehavior] = GroupByBehavior.Unrelated;
            row[DbMetaDataColumnNames.IdentifierPattern] = @"(^\[\p{Lo}\p{Lu}\p{Ll}_@#][\p{Lo}\p{Lu}\p{Ll}\p{Nd}@$#_]*$)|(^\[[^\]\0]|\]\]+\]$)|(^\""[^\""\0]|\""\""+\""$)";
            row[DbMetaDataColumnNames.IdentifierCase] = ToIdentifierCase(config.unquotedCasing());
            row[DbMetaDataColumnNames.OrderByColumnsInSelect] = false;
            row[DbMetaDataColumnNames.ParameterMarkerFormat] = "?";
            row[DbMetaDataColumnNames.ParameterMarkerPattern] = @"\?";
            row[DbMetaDataColumnNames.ParameterNameMaxLength] = 0;
            row[DbMetaDataColumnNames.ParameterNamePattern] = string.Empty;
            row[DbMetaDataColumnNames.QuotedIdentifierPattern] = QuotedIdentifierPattern(config.quoting());
            row[DbMetaDataColumnNames.QuotedIdentifierCase] = ToIdentifierCase(config.quotedCasing());
            row[DbMetaDataColumnNames.StatementSeparatorPattern] = ";";
            row[DbMetaDataColumnNames.StringLiteralPattern] = @"'(([^']|'')*)'";
            row[DbMetaDataColumnNames.SupportedJoinOperators] =
                SupportedJoinOperators.Inner |
                SupportedJoinOperators.LeftOuter |
                SupportedJoinOperators.RightOuter |
                SupportedJoinOperators.FullOuter;
            t.Rows.Add(row);

            return t;
        }

        /// <summary>
        /// Returns the SQL data types supported by Calcite.
        /// </summary>
        /// <param name="connection">The open connection whose <see cref="RelDataTypeSystem"/> dictates precision and scale limits.</param>
        public static DataTable BuildDataTypes(CalciteConnection connection)
        {
            var typeSystem = connection.TypeFactory.getTypeSystem();

            var t = new DataTable(DataTypes);
            t.Columns.Add(DbMetaDataColumnNames.TypeName, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.ProviderDbType, typeof(int));
            t.Columns.Add(DbMetaDataColumnNames.ColumnSize, typeof(long));
            t.Columns.Add(DbMetaDataColumnNames.CreateFormat, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.CreateParameters, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.DataType, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.IsAutoIncrementable, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsBestMatch, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsCaseSensitive, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsFixedLength, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsFixedPrecisionScale, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsLong, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsNullable, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsSearchable, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsSearchableWithLike, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsUnsigned, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.MaximumScale, typeof(short));
            t.Columns.Add(DbMetaDataColumnNames.MinimumScale, typeof(short));
            t.Columns.Add(DbMetaDataColumnNames.IsConcurrencyType, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.IsLiteralSupported, typeof(bool));
            t.Columns.Add(DbMetaDataColumnNames.LiteralPrefix, typeof(string));
            t.Columns.Add(DbMetaDataColumnNames.LiteralSuffix, typeof(string));

            void Add(SqlTypeName sqlType, string name, DbType dbType, Type clr, bool fixedLen = false, bool fixedScale = false, bool isLong = false, bool isUnsigned = false, string? prefix = null, string? suffix = null)
            {
                var maxPrecision = typeSystem.getMaxPrecision(sqlType);
                var maxScale = typeSystem.getMaxScale(sqlType);
                var minScale = typeSystem.getMinScale(sqlType);

                var row = t.NewRow();
                row[DbMetaDataColumnNames.TypeName] = name;
                row[DbMetaDataColumnNames.ProviderDbType] = (int)dbType;
                row[DbMetaDataColumnNames.ColumnSize] = maxPrecision >= 0 ? (object)(long)maxPrecision : DBNull.Value;
                row[DbMetaDataColumnNames.CreateFormat] = name;
                row[DbMetaDataColumnNames.CreateParameters] = DBNull.Value;
                row[DbMetaDataColumnNames.DataType] = clr.FullName!;
                row[DbMetaDataColumnNames.IsAutoIncrementable] = false;
                row[DbMetaDataColumnNames.IsBestMatch] = true;
                row[DbMetaDataColumnNames.IsCaseSensitive] = clr == typeof(string);
                row[DbMetaDataColumnNames.IsFixedLength] = fixedLen;
                row[DbMetaDataColumnNames.IsFixedPrecisionScale] = fixedScale;
                row[DbMetaDataColumnNames.IsLong] = isLong;
                row[DbMetaDataColumnNames.IsNullable] = true;
                row[DbMetaDataColumnNames.IsSearchable] = true;
                row[DbMetaDataColumnNames.IsSearchableWithLike] = clr == typeof(string);
                row[DbMetaDataColumnNames.IsUnsigned] = isUnsigned;
                row[DbMetaDataColumnNames.MaximumScale] = ClampToShort(maxScale);
                row[DbMetaDataColumnNames.MinimumScale] = ClampToShort(minScale);
                row[DbMetaDataColumnNames.IsConcurrencyType] = false;
                row[DbMetaDataColumnNames.IsLiteralSupported] = true;
                row[DbMetaDataColumnNames.LiteralPrefix] = prefix ?? (object)DBNull.Value;
                row[DbMetaDataColumnNames.LiteralSuffix] = suffix ?? (object)DBNull.Value;
                t.Rows.Add(row);
            }

            Add(SqlTypeName.BOOLEAN, "BOOLEAN", DbType.Boolean, typeof(bool), fixedLen: true, fixedScale: true);
            Add(SqlTypeName.TINYINT, "TINYINT", DbType.SByte, typeof(sbyte), fixedLen: true, fixedScale: true);
            Add(SqlTypeName.SMALLINT, "SMALLINT", DbType.Int16, typeof(short), fixedLen: true, fixedScale: true);
            Add(SqlTypeName.INTEGER, "INTEGER", DbType.Int32, typeof(int), fixedLen: true, fixedScale: true);
            Add(SqlTypeName.BIGINT, "BIGINT", DbType.Int64, typeof(long), fixedLen: true, fixedScale: true);
            Add(SqlTypeName.REAL, "REAL", DbType.Single, typeof(float), fixedLen: true);
            Add(SqlTypeName.FLOAT, "FLOAT", DbType.Double, typeof(double), fixedLen: true);
            Add(SqlTypeName.DOUBLE, "DOUBLE", DbType.Double, typeof(double), fixedLen: true);
            Add(SqlTypeName.DECIMAL, "DECIMAL", DbType.Decimal, typeof(decimal));
            Add(SqlTypeName.CHAR, "CHAR", DbType.StringFixedLength, typeof(string), fixedLen: true, prefix: "'", suffix: "'");
            Add(SqlTypeName.VARCHAR, "VARCHAR", DbType.String, typeof(string), prefix: "'", suffix: "'");
            Add(SqlTypeName.BINARY, "BINARY", DbType.Binary, typeof(byte[]), fixedLen: true, prefix: "X'", suffix: "'");
            Add(SqlTypeName.VARBINARY, "VARBINARY", DbType.Binary, typeof(byte[]), prefix: "X'", suffix: "'");
            Add(SqlTypeName.DATE, "DATE", DbType.Date, typeof(DateTime), fixedLen: true, prefix: "DATE '", suffix: "'");
            Add(SqlTypeName.TIME, "TIME", DbType.Time, typeof(TimeSpan), fixedLen: true, prefix: "TIME '", suffix: "'");
            Add(SqlTypeName.TIMESTAMP, "TIMESTAMP", DbType.DateTime, typeof(DateTime), fixedLen: true, prefix: "TIMESTAMP '", suffix: "'");

            return t;
        }

        /// <summary>
        /// Returns the SQL reserved words honored by Calcite's currently configured parser.
        /// </summary>
        /// <param name="connection">The open connection whose parser configuration determines the reserved-word set.</param>
        public static DataTable BuildReservedWords(CalciteConnection connection)
        {
            var t = new DataTable(ReservedWords);
            t.Columns.Add(DbMetaDataColumnNames.ReservedWord, typeof(string));

            var config = connection.Config;
            var parserFactory = (org.apache.calcite.sql.parser.SqlParserImplFactory?)config.parserFactory(typeof(org.apache.calcite.sql.parser.SqlParserImplFactory), null)
                ?? org.apache.calcite.sql.parser.impl.SqlParserImpl.FACTORY;
            var parserConfig = SqlParser.config()
                .withParserFactory(parserFactory)
                .withQuoting(config.quoting())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuotedCasing(config.quotedCasing())
                .withConformance(config.conformance());

            var metadata = SqlParser.create("", parserConfig).getMetadata();
            var tokens = metadata.getTokens().iterator();
            while (tokens.hasNext())
            {
                var token = (string)tokens.next();
                if (metadata.isReservedWord(token))
                    t.Rows.Add(token);
            }

            return t;
        }

        /// <summary>
        /// Returns the schemas visible in the engine's root schema.
        /// </summary>
        /// <param name="connection">The open connection.</param>
        /// <param name="schemaFilter">Optional schema-name filter (exact match, case-sensitive).</param>
        public static DataTable BuildSchemas(CalciteConnection connection, string? schemaFilter)
        {
            var t = new DataTable(Schemas);
            t.Columns.Add("CatalogName", typeof(string));
            t.Columns.Add("SchemaName", typeof(string));
            t.Columns.Add("IsDefault", typeof(bool));

            var defaultSchema = connection.Database;
            var root = connection.RootSchema;

            // The root schema itself has no name; only enumerate sub-schemas.
            foreach (var name in EnumerateNames(root.getSubSchemaNames()))
            {
                if (schemaFilter is not null && schemaFilter != name)
                    continue;

                t.Rows.Add(string.Empty, name, string.Equals(defaultSchema, name, StringComparison.Ordinal));
            }

            return t;
        }

        /// <summary>
        /// Returns the tables visible in the engine.
        /// </summary>
        /// <param name="connection">The open connection.</param>
        /// <param name="catalog">Catalog filter (Calcite has a single anonymous catalog; pass <see langword="null"/>).</param>
        /// <param name="schema">Optional schema-name filter (exact match, case-sensitive).</param>
        /// <param name="table">Optional table-name filter (exact match, case-sensitive).</param>
        public static DataTable BuildTables(CalciteConnection connection, string? catalog, string? schema, string? table)
        {
            var t = new DataTable(Tables);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));
            t.Columns.Add("TableType", typeof(string));

            foreach (var (schemaName, schemaPlus) in EnumerateSchemas(connection, schema))
            {
                foreach (var (name, tbl) in EnumerateTables(schemaPlus))
                {
                    if (table is not null && table != name)
                        continue;

                    var jdbcType = tbl.getJdbcTableType();
                    var typeName = jdbcType is null ? "TABLE" : jdbcType.jdbcName;

                    t.Rows.Add(catalog ?? string.Empty, schemaName, name, typeName);
                }
            }

            return t;
        }

        /// <summary>
        /// Returns the views visible in the engine.
        /// </summary>
        public static DataTable BuildViews(CalciteConnection connection, string? catalog, string? schema, string? view)
        {
            var t = new DataTable(Views);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));

            foreach (var (schemaName, schemaPlus) in EnumerateSchemas(connection, schema))
            {
                foreach (var (name, tbl) in EnumerateTables(schemaPlus))
                {
                    if (view is not null && view != name)
                        continue;

                    var jdbcType = tbl.getJdbcTableType();
                    if (jdbcType is null || string.Equals(jdbcType.jdbcName, "VIEW", StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    t.Rows.Add(catalog ?? string.Empty, schemaName, name);
                }
            }

            return t;
        }

        /// <summary>
        /// Returns the columns visible in the engine.
        /// </summary>
        public static DataTable BuildColumns(CalciteConnection connection, string? catalog, string? schema, string? table, string? column)
        {
            var t = new DataTable(Columns);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));
            t.Columns.Add("ColumnName", typeof(string));
            t.Columns.Add("OrdinalPosition", typeof(int));
            t.Columns.Add("DataType", typeof(string));
            t.Columns.Add("IsNullable", typeof(bool));
            t.Columns.Add("CharacterMaximumLength", typeof(int));
            t.Columns.Add("NumericPrecision", typeof(int));
            t.Columns.Add("NumericScale", typeof(int));

            var typeFactory = connection.TypeFactory;

            foreach (var (schemaName, schemaPlus) in EnumerateSchemas(connection, schema))
            {
                foreach (var (tableName, tbl) in EnumerateTables(schemaPlus))
                {
                    if (table is not null && table != tableName)
                        continue;

                    RelDataType rowType;
                    try
                    {
                        rowType = tbl.getRowType(typeFactory);
                    }
                    catch
                    {
                        continue;
                    }

                    var fields = rowType.getFieldList();
                    var count = fields.size();
                    for (var i = 0; i < count; i++)
                    {
                        var field = (RelDataTypeField)fields.get(i);
                        var name = field.getName();
                        if (column is not null && column != name)
                            continue;

                        var type = field.getType();
                        var sqlTypeName = type.getSqlTypeName();

                        var row = t.NewRow();
                        row["TableCatalog"] = catalog ?? string.Empty;
                        row["TableSchema"] = schemaName;
                        row["TableName"] = tableName;
                        row["ColumnName"] = name;
                        row["OrdinalPosition"] = field.getIndex() + 1;
                        row["DataType"] = sqlTypeName?.getName() ?? "ANY";
                        row["IsNullable"] = type.isNullable();

                        if (IsCharacterType(sqlTypeName) && type.getPrecision() != int.MinValue)
                            row["CharacterMaximumLength"] = type.getPrecision();
                        else
                            row["CharacterMaximumLength"] = DBNull.Value;

                        if (IsNumericType(sqlTypeName))
                        {
                            row["NumericPrecision"] = type.getPrecision();
                            row["NumericScale"] = type.getScale();
                        }
                        else
                        {
                            row["NumericPrecision"] = DBNull.Value;
                            row["NumericScale"] = DBNull.Value;
                        }

                        t.Rows.Add(row);
                    }
                }
            }

            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>ViewColumns</c> collection.
        /// Calcite's metamodel does not expose dedicated view-column metadata, so the table is shaped but unpopulated.
        /// </summary>
        public static DataTable BuildViewColumns(CalciteConnection connection, string? catalog, string? schema, string? view, string? column)
        {
            var t = new DataTable(ViewColumns);
            t.Columns.Add("ViewCatalog", typeof(string));
            t.Columns.Add("ViewSchema", typeof(string));
            t.Columns.Add("ViewName", typeof(string));
            t.Columns.Add("ColumnName", typeof(string));
            t.Columns.Add("OrdinalPosition", typeof(int));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>Indexes</c> collection.
        /// Calcite has no native index concept, so the table is shaped but unpopulated.
        /// </summary>
        public static DataTable BuildIndexes(CalciteConnection connection, string? catalog, string? schema, string? table, string? index)
        {
            var t = new DataTable(Indexes);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));
            t.Columns.Add("IndexName", typeof(string));
            t.Columns.Add("IsUnique", typeof(bool));
            t.Columns.Add("IsPrimary", typeof(bool));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>IndexColumns</c> collection.
        /// </summary>
        public static DataTable BuildIndexColumns(CalciteConnection connection, string? catalog, string? schema, string? table, string? index, string? column)
        {
            var t = new DataTable(IndexColumns);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));
            t.Columns.Add("IndexName", typeof(string));
            t.Columns.Add("ColumnName", typeof(string));
            t.Columns.Add("OrdinalPosition", typeof(int));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>PrimaryKeys</c> collection.
        /// Calcite does not surface key constraints through its catalog; the table is shaped but unpopulated.
        /// </summary>
        public static DataTable BuildPrimaryKeys(CalciteConnection connection, string? catalog, string? schema, string? table, string? constraint)
        {
            var t = new DataTable(PrimaryKeys);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));
            t.Columns.Add("ConstraintName", typeof(string));
            t.Columns.Add("ColumnName", typeof(string));
            t.Columns.Add("OrdinalPosition", typeof(int));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>ForeignKeys</c> collection.
        /// </summary>
        public static DataTable BuildForeignKeys(CalciteConnection connection, string? catalog, string? schema, string? table, string? constraint)
        {
            var t = new DataTable(ForeignKeys);
            t.Columns.Add("TableCatalog", typeof(string));
            t.Columns.Add("TableSchema", typeof(string));
            t.Columns.Add("TableName", typeof(string));
            t.Columns.Add("ConstraintName", typeof(string));
            t.Columns.Add("ReferencedTableCatalog", typeof(string));
            t.Columns.Add("ReferencedTableSchema", typeof(string));
            t.Columns.Add("ReferencedTableName", typeof(string));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>Procedures</c> collection.
        /// Calcite does not have stored procedures; the table is shaped but unpopulated.
        /// </summary>
        public static DataTable BuildProcedures(CalciteConnection connection, string? catalog, string? schema, string? procedure, string? procedureType)
        {
            var t = new DataTable(Procedures);
            t.Columns.Add("ProcedureCatalog", typeof(string));
            t.Columns.Add("ProcedureSchema", typeof(string));
            t.Columns.Add("ProcedureName", typeof(string));
            t.Columns.Add("ProcedureType", typeof(string));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>ProcedureParameters</c> collection.
        /// </summary>
        public static DataTable BuildProcedureParameters(CalciteConnection connection, string? catalog, string? schema, string? procedure, string? procedureType, string? parameter)
        {
            var t = new DataTable(ProcedureParameters);
            t.Columns.Add("ProcedureCatalog", typeof(string));
            t.Columns.Add("ProcedureSchema", typeof(string));
            t.Columns.Add("ProcedureName", typeof(string));
            t.Columns.Add("ParameterName", typeof(string));
            t.Columns.Add("OrdinalPosition", typeof(int));
            t.Columns.Add("ParameterMode", typeof(string));
            t.Columns.Add("DataType", typeof(string));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>Functions</c> collection.
        /// Per-schema function enumeration is not exposed; the table is shaped but unpopulated.
        /// </summary>
        public static DataTable BuildFunctions(CalciteConnection connection, string? catalog, string? schema, string? function, string? functionType)
        {
            var t = new DataTable(Functions);
            t.Columns.Add("FunctionCatalog", typeof(string));
            t.Columns.Add("FunctionSchema", typeof(string));
            t.Columns.Add("FunctionName", typeof(string));
            t.Columns.Add("FunctionType", typeof(string));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>FunctionParameters</c> collection.
        /// </summary>
        public static DataTable BuildFunctionParameters(CalciteConnection connection, string? catalog, string? schema, string? function, string? functionType, string? parameter)
        {
            var t = new DataTable(FunctionParameters);
            t.Columns.Add("FunctionCatalog", typeof(string));
            t.Columns.Add("FunctionSchema", typeof(string));
            t.Columns.Add("FunctionName", typeof(string));
            t.Columns.Add("ParameterName", typeof(string));
            t.Columns.Add("OrdinalPosition", typeof(int));
            t.Columns.Add("ParameterMode", typeof(string));
            t.Columns.Add("DataType", typeof(string));
            return t;
        }

        /// <summary>
        /// Returns an empty <see cref="DataTable"/> for the <c>UserDefinedTypes</c> collection.
        /// </summary>
        public static DataTable BuildUserDefinedTypes(CalciteConnection connection, string? catalog, string? type)
        {
            var t = new DataTable(UserDefinedTypes);
            t.Columns.Add("Catalog", typeof(string));
            t.Columns.Add("TypeName", typeof(string));
            t.Columns.Add("DataType", typeof(string));
            return t;
        }

        static System.Collections.Generic.IEnumerable<string> EnumerateNames(java.util.Set set)
        {
            var it = set.iterator();
            while (it.hasNext())
                yield return (string)it.next();
        }

        static System.Collections.Generic.IEnumerable<(string Name, SchemaPlus Schema)> EnumerateSchemas(CalciteConnection connection, string? schemaFilter)
        {
            var root = connection.RootSchema;
            foreach (var name in EnumerateNames(root.getSubSchemaNames()))
            {
                if (schemaFilter is not null && schemaFilter != name)
                    continue;

                var sub = root.getSubSchema(name);
                if (sub is not null)
                    yield return (name, sub);
            }
        }

        static System.Collections.Generic.IEnumerable<(string Name, Table Table)> EnumerateTables(SchemaPlus schemaPlus)
        {
            foreach (var name in EnumerateNames(schemaPlus.getTableNames()))
            {
                var tbl = schemaPlus.getTable(name);
                if (tbl is null)
                    continue;

                yield return (name, tbl);
            }
        }

        static bool IsCharacterType(SqlTypeName? name)
        {
            if (name is null)
                return false;

            return name == SqlTypeName.CHAR || name == SqlTypeName.VARCHAR;
        }

        static bool IsNumericType(SqlTypeName? name)
        {
            if (name is null)
                return false;

            return name == SqlTypeName.TINYINT
                || name == SqlTypeName.SMALLINT
                || name == SqlTypeName.INTEGER
                || name == SqlTypeName.BIGINT
                || name == SqlTypeName.DECIMAL
                || name == SqlTypeName.FLOAT
                || name == SqlTypeName.REAL
                || name == SqlTypeName.DOUBLE;
        }

        static IdentifierCase ToIdentifierCase(Casing casing)
        {
            // UNCHANGED preserves the user's casing, so identifiers are matched case-sensitively.
            // TO_UPPER and TO_LOWER normalize identifiers, so they compare case-insensitively.
            if (casing == Casing.UNCHANGED)
                return IdentifierCase.Sensitive;

            return IdentifierCase.Insensitive;
        }

        static string QuotedIdentifierPattern(Quoting quoting)
        {
            if (quoting == Quoting.BACK_TICK)
                return "`((?:[^`]|``)*)`";

            if (quoting == Quoting.BACK_TICK_BACKSLASH)
                return "`((?:[^`\\\\]|\\\\.)*)`";

            if (quoting == Quoting.BRACKET)
                return @"\[((?:[^\]]|\]\])*)\]";

            // Default and Quoting.DOUBLE_QUOTE.
            return "\"((?:[^\"]|\"\")*)\"";
        }

        static short ClampToShort(int value)
        {
            if (value > short.MaxValue)
                return short.MaxValue;

            if (value < short.MinValue)
                return short.MinValue;

            return (short)value;
        }

    }

}
