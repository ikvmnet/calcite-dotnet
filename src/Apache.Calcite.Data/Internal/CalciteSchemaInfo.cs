using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using org.apache.calcite.avatica.util;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;


namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Builds the standard ADO.NET metadata <see cref="DataTable"/>s served by
    /// <see cref="CalciteConnection.GetSchema()"/> overloads.
    /// </summary>
    /// <remarks>
    /// Only the metadata collections defined by ADO.NET (<see cref="DbMetaDataCollectionNames"/>)
    /// are exposed at this time. Provider-specific collections may be reintroduced once
    /// standardized names and shapes have been agreed upon.
    /// </remarks>
    internal static class CalciteSchemaInfo
    {

        public static readonly string MetaDataCollections = DbMetaDataCollectionNames.MetaDataCollections;
        public static readonly string Restrictions = DbMetaDataCollectionNames.Restrictions;
        public static readonly string DataSourceInformation = DbMetaDataCollectionNames.DataSourceInformation;
        public static readonly string DataTypes = DbMetaDataCollectionNames.DataTypes;
        public static readonly string ReservedWords = DbMetaDataCollectionNames.ReservedWords;
        public static readonly string Tables = "Tables";
        public static readonly string Columns = "Columns";

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
            t.Rows.Add(Tables, 4, 3);
            t.Rows.Add(Columns, 4, 4);

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
            t.Columns.Add("ParameterName", typeof(string));
            t.Columns.Add("RestrictionDefault", typeof(string));
            t.Columns.Add("RestrictionNumber", typeof(int));

            t.Rows.Add(Tables, "Catalog", "@Catalog", null, 1);
            t.Rows.Add(Tables, "Schema", "@Schema", null, 2);
            t.Rows.Add(Tables, "Table", "@Table", null, 3);
            t.Rows.Add(Tables, "TableType", "@TableType", null, 4);

            t.Rows.Add(Columns, "Catalog", "@Catalog", null, 1);
            t.Rows.Add(Columns, "Schema", "@Schema", null, 2);
            t.Rows.Add(Columns, "Table", "@Table", null, 3);
            t.Rows.Add(Columns, "Column", "@Column", null, 4);

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
        /// Returns the tables visible through the root schema of the connection.
        /// </summary>
        /// <param name="connection">The open connection whose root schema is enumerated.</param>
        /// <param name="restrictionValues">
        /// Optional restrictions in ADO.NET order: [0] catalog (ignored), [1] schema name, [2] table name, [3] table type.
        /// </param>
        public static DataTable BuildTables(CalciteConnection connection, string?[]? restrictionValues)
        {
            var t = new DataTable(Tables);
            t.Columns.Add("TABLE_CATALOG", typeof(string));
            t.Columns.Add("TABLE_SCHEMA", typeof(string));
            t.Columns.Add("TABLE_NAME", typeof(string));
            t.Columns.Add("TABLE_TYPE", typeof(string));

            var schemaFilter    = restrictionValues?.Length > 1 ? restrictionValues[1] : null;
            var tableFilter     = restrictionValues?.Length > 2 ? restrictionValues[2] : null;
            var tableTypeFilter = restrictionValues?.Length > 3 ? restrictionValues[3] : null;

            var root = connection.RootSchema;
            var schemaNames = root.getSubSchemaNames().iterator();
            while (schemaNames.hasNext())
            {
                var schemaName = (string)schemaNames.next();
                if (schemaFilter is not null && !string.Equals(schemaName, schemaFilter, StringComparison.OrdinalIgnoreCase))
                    continue;

                var subSchema = root.getSubSchema(schemaName);
                if (subSchema is null)
                    continue;

                foreach (var (tableName, table) in TablesOf(subSchema))
                {
                    if (tableFilter is not null && !string.Equals(tableName, tableFilter, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var tableType = table?.getJdbcTableType()?.jdbcName ?? "TABLE";

                    if (tableTypeFilter is not null && !string.Equals(tableType, tableTypeFilter, StringComparison.OrdinalIgnoreCase))
                        continue;

                    t.Rows.Add(DBNull.Value, schemaName, tableName, tableType);
                }
            }

            return t;
        }

        /// <summary>
        /// Returns every table of <paramref name="subSchema"/>, views included.
        /// </summary>
        /// <remarks>
        /// <c>CalciteMetaImpl.tables(MetaSchema, LikePattern)</c>, which is two sequences concatenated
        /// rather than one, and has to be: <b>a view is not a table.</b> Both routes to one register it as
        /// a <c>TableMacro</c> taking no arguments — <c>ModelHandler.visit(JsonView)</c> calls
        /// <c>schema.add(name, ViewTable.viewMacro(...))</c> and so does
        /// <c>ServerDdlExecutor.execute(SqlCreateView, ...)</c> — and a macro goes in the schema's function
        /// map. <c>getTableNames()</c> reads the table map and the underlying schema, so it never sees one,
        /// whatever its own javadoc says.
        ///
        /// <para><c>getTablesBasedOnNullaryFunctions</c> is the other half, and it <i>expands</i> each view
        /// to answer — <c>apply(ImmutableList.of())</c> per macro, which parses and validates the view's
        /// SQL. So this is not a cheap enumeration and a view whose definition no longer resolves throws
        /// here rather than returning nothing. Calcite's own metadata has both properties.</para>
        /// </remarks>
        static IEnumerable<(string Name, Table? Table)> TablesOf(SchemaPlus subSchema)
        {
            var tableNames = subSchema.getTableNames().iterator();
            while (tableNames.hasNext())
            {
                var tableName = (string)tableNames.next();
                yield return (tableName, subSchema.getTable(tableName));
            }

            var views = CalciteSchema.from(subSchema).getTablesBasedOnNullaryFunctions().entrySet().iterator();
            while (views.hasNext())
            {
                var entry = (java.util.Map.Entry)views.next();
                yield return ((string)entry.getKey(), (Table)entry.getValue());
            }
        }

        /// <summary>
        /// Returns the columns of all tables visible through the root schema of the connection.
        /// </summary>
        /// <param name="connection">The open connection whose root schema is enumerated.</param>
        /// <param name="restrictionValues">
        /// Optional restrictions in ADO.NET order: [0] catalog (ignored), [1] schema name, [2] table name, [3] column name.
        /// </param>
        public static DataTable BuildColumns(CalciteConnection connection, string?[]? restrictionValues)
        {
            var t = new DataTable(Columns);
            t.Columns.Add("TABLE_CATALOG", typeof(string));
            t.Columns.Add("TABLE_SCHEMA", typeof(string));
            t.Columns.Add("TABLE_NAME", typeof(string));
            t.Columns.Add("COLUMN_NAME", typeof(string));
            t.Columns.Add("ORDINAL_POSITION", typeof(int));
            t.Columns.Add("COLUMN_DEFAULT", typeof(string));
            t.Columns.Add("IS_NULLABLE", typeof(string));
            t.Columns.Add("DATA_TYPE", typeof(string));
            t.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int));
            t.Columns.Add("NUMERIC_PRECISION", typeof(int));
            t.Columns.Add("NUMERIC_SCALE", typeof(int));

            var schemaFilter = restrictionValues?.Length > 1 ? restrictionValues[1] : null;
            var tableFilter  = restrictionValues?.Length > 2 ? restrictionValues[2] : null;
            var columnFilter = restrictionValues?.Length > 3 ? restrictionValues[3] : null;

            var typeFactory = connection.TypeFactory;
            var root = connection.RootSchema;
            var schemaNames = root.getSubSchemaNames().iterator();
            while (schemaNames.hasNext())
            {
                var schemaName = (string)schemaNames.next();
                if (schemaFilter is not null && !string.Equals(schemaName, schemaFilter, StringComparison.OrdinalIgnoreCase))
                    continue;

                var subSchema = root.getSubSchema(schemaName);
                if (subSchema is null)
                    continue;

                foreach (var (tableName, table) in TablesOf(subSchema))
                {
                    if (tableFilter is not null && !string.Equals(tableName, tableFilter, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (table is null)
                        continue;

                    var rowType = table.getRowType(typeFactory);
                    var fields = rowType.getFieldList();
                    for (int i = 0; i < fields.size(); i++)
                    {
                        var field = (org.apache.calcite.rel.type.RelDataTypeField)fields.get(i);
                        var columnName = field.getName();

                        if (columnFilter is not null && !string.Equals(columnName, columnFilter, StringComparison.OrdinalIgnoreCase))
                            continue;

                        var fieldType = field.getType();
                        var sqlTypeName = fieldType.getSqlTypeName().getName();
                        var isNullable = fieldType.isNullable();
                        var precision = fieldType.getPrecision();
                        var scale = fieldType.getScale();

                        var isCharType = fieldType.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR
                                      || fieldType.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

                        t.Rows.Add(
                            DBNull.Value,
                            schemaName,
                            tableName,
                            columnName,
                            i + 1,
                            DBNull.Value,
                            isNullable ? "YES" : "NO",
                            sqlTypeName,
                            isCharType && precision >= 0 ? (object)precision : DBNull.Value,
                            !isCharType && precision >= 0 ? (object)precision : DBNull.Value,
                            !isCharType && scale >= 0 ? (object)scale : DBNull.Value
                        );
                    }
                }
            }

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
