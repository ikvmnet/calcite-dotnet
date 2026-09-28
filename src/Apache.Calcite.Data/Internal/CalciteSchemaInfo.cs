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
    /// Builds the metadata <see cref="DataTable"/>s returned by <see cref="CalciteConnection.GetSchema()"/>.
    /// </summary>
    /// <remarks>
    /// The collections are the five common ones named by <see cref="DbMetaDataCollectionNames"/>, plus
    /// <c>Tables</c> and <c>Columns</c>. <c>Tables</c> and <c>Columns</c> list the tables and views of the
    /// root schema's immediate sub-schemas; tables on the root itself and in nested sub-schemas are not
    /// listed. Restrictions match names exactly, ignoring case, and the catalog restriction is ignored.
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
        /// Returns the <c>MetaDataCollections</c> collection: every collection's name, number of restrictions
        /// and number of identifier parts.
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
        /// Returns the <c>Restrictions</c> collection: the restrictions <c>Tables</c> and <c>Columns</c> accept,
        /// in order.
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
        /// Returns the <c>DataSourceInformation</c> collection, with the identifier casing and quoting taken
        /// from the connection's configuration.
        /// </summary>
        /// <param name="connection">The open connection.</param>
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
        /// Returns the <c>DataTypes</c> collection: Calcite's scalar SQL types, with precision and scale
        /// limits from the connection's type system.
        /// </summary>
        /// <param name="connection">The open connection whose <see cref="RelDataTypeSystem"/> gives the limits.</param>
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
        /// Returns the <c>Tables</c> collection: the tables and views of the root schema's immediate
        /// sub-schemas, under the root's read lock.
        /// </summary>
        /// <param name="connection">The open connection whose root schema is enumerated.</param>
        /// <param name="restrictionValues">
        /// Optional restrictions in ADO.NET order: [0] catalog (ignored), [1] schema name, [2] table name, [3] table type.
        /// </param>
        /// <remarks>
        /// A table whose type Calcite does not report is listed as <c>TABLE</c>.
        /// </remarks>
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

            var session = connection.RequireSession();
            using var _ = session.ReadRoot();
            var root = session.RootSchema;
            var schemaNames = root.getSubSchemaNames().iterator();
            while (schemaNames.hasNext())
            {
                var schemaName = (string)schemaNames.next();
                if (schemaFilter is not null && !string.Equals(schemaName, schemaFilter, StringComparison.OrdinalIgnoreCase))
                    continue;

                var subSchema = root.getSubSchema(schemaName);
                if (subSchema is null)
                    continue;

                foreach (var (tableName, table) in TablesOf(subSchema, tableFilter))
                {
                    var tableType = table?.getJdbcTableType()?.jdbcName ?? "TABLE";

                    if (tableTypeFilter is not null && !string.Equals(tableType, tableTypeFilter, StringComparison.OrdinalIgnoreCase))
                        continue;

                    t.Rows.Add(DBNull.Value, schemaName, tableName, tableType);
                }
            }

            return t;
        }

        /// <summary>
        /// Returns the tables of <paramref name="subSchema"/> whose name passes
        /// <paramref name="nameFilter"/>, views included.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Tables and views are read separately. Calcite registers a view, from a model or from
        /// <c>CREATE VIEW</c>, as a <c>TableMacro</c> of no arguments in the schema's function map
        /// (<c>ModelHandler.visit(JsonView)</c> and <c>ServerDdlExecutor.execute(SqlCreateView, ...)</c> both
        /// call <c>schema.add(name, ViewTable.viewMacro(...))</c>), so <c>getTableNames()</c> does not return
        /// it.
        /// </para>
        /// <para>
        /// Describing a view means expanding it with <c>ViewTableMacro.apply</c>, which parses, validates and
        /// converts the view's SQL. The name restriction is therefore applied before expansion, and only the
        /// views that pass it are expanded, through <c>getTableBasedOnNullaryFunction</c>. Calcite's own JDBC
        /// metadata (<c>CalciteMetaImpl.tables</c>) expands every view in the schema; this is not a port of it,
        /// and restricting first keeps one unresolvable view from breaking metadata calls about other tables.
        /// </para>
        /// <para>
        /// The table-type restriction cannot be applied before expansion, because <c>TABLE_TYPE</c> comes from
        /// <c>Table.getJdbcTableType()</c> and a macro's class does not reliably say what it produces
        /// (<c>MaterializedViewTable.MaterializedViewTableMacro</c> overrides <c>apply</c>). A listing without a
        /// name restriction therefore expands every view, and fails if one no longer resolves.
        /// </para>
        /// </remarks>
        static IEnumerable<(string Name, Table? Table)> TablesOf(SchemaPlus subSchema, string? nameFilter)
        {
            var tableNames = subSchema.getTableNames().iterator();
            while (tableNames.hasNext())
            {
                var tableName = (string)tableNames.next();
                if (!MatchesName(tableName, nameFilter))
                    continue;

                yield return (tableName, subSchema.getTable(tableName));
            }

            var schema = CalciteSchema.from(subSchema);
            foreach (var viewName in ViewNamesOf(schema))
            {
                if (!MatchesName(viewName, nameFilter))
                    continue;

                // the name came from this schema's own function names, so the lookup is exact
                var entry = schema.getTableBasedOnNullaryFunction(viewName, true);
                if (entry is not null)
                    yield return (viewName, entry.getTable());
            }
        }

        /// <summary>
        /// Returns the names in <paramref name="schema"/> that resolve to a table macro of no arguments.
        /// </summary>
        /// <remarks>
        /// The names <c>getTablesBasedOnNullaryFunctions</c> would key its map by, found with the same two
        /// tests that method applies, without expanding any macro. <c>getFunctionNames</c> includes both
        /// explicit and implicit functions.
        /// </remarks>
        static IEnumerable<string> ViewNamesOf(CalciteSchema schema)
        {
            var names = schema.getFunctionNames().iterator();
            while (names.hasNext())
            {
                var name = (string)names.next();

                var functions = schema.getFunctions(name, true).iterator();
                while (functions.hasNext())
                {
                    if (functions.next() is TableMacro macro && macro.getParameters().isEmpty())
                    {
                        yield return name;
                        break;
                    }
                }
            }
        }

        /// <summary>Applies a name restriction: an exact match ignoring case, or none where <see langword="null"/>.</summary>
        static bool MatchesName(string name, string? filter)
        {
            return filter is null || string.Equals(name, filter, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns the <c>Columns</c> collection: the columns of the tables and views <see cref="BuildTables"/>
        /// would list, under the root's read lock.
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
            var session = connection.RequireSession();
            using var _ = session.ReadRoot();
            var root = session.RootSchema;
            var schemaNames = root.getSubSchemaNames().iterator();
            while (schemaNames.hasNext())
            {
                var schemaName = (string)schemaNames.next();
                if (schemaFilter is not null && !string.Equals(schemaName, schemaFilter, StringComparison.OrdinalIgnoreCase))
                    continue;

                var subSchema = root.getSubSchema(schemaName);
                if (subSchema is null)
                    continue;

                foreach (var (tableName, table) in TablesOf(subSchema, tableFilter))
                {
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
        /// Returns the <c>ReservedWords</c> collection: the words reserved by the parser the connection is
        /// configured with.
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
