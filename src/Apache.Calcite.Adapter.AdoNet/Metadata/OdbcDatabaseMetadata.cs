using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// The metadata for any <see cref="OdbcConnection"/>, read from the ODBC catalog.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The <c>Tables</c> and <c>Columns</c> schema collections come from ODBC's <c>SQLTables</c> and
    /// <c>SQLColumns</c>. They name the catalog and schema <c>TABLE_CAT</c> and <c>TABLE_SCHEM</c>, give the type
    /// as a numeric <c>DATA_TYPE</c> code, and have no <c>NUMERIC_PRECISION</c>: <c>COLUMN_SIZE</c> is the length
    /// of a character column and the precision of a numeric one.
    /// </para>
    /// <para>
    /// The database behind the driver is unknown, so there is no default schema: a null database or schema means
    /// every one, and a caller who wants one schema names it.
    /// </para>
    /// </remarks>
    class OdbcDatabaseMetadata : AdoDatabaseMetadata
    {

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource">The data source to read metadata from.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dbDataSource"/> is <see langword="null"/>.</exception>
        public OdbcDatabaseMetadata(DbDataSource dbDataSource)
        {
            _dbDataSource = dbDataSource ?? throw new ArgumentNullException(nameof(dbDataSource));
        }

        /// <summary>
        /// Gets the data source this metadata describes.
        /// </summary>
        public DbDataSource DbDataSource => _dbDataSource;

        /// <summary>
        /// Returns the catalog a new connection is in, or <see langword="null"/> where the driver reports none.
        /// </summary>
        /// <returns>The catalog, or <see langword="null"/>.</returns>
        public override string? GetDefaultDatabase()
        {
            using var cnn = _dbDataSource.OpenConnection();

            // a driver with no notion of a catalog reports an empty one
            return string.IsNullOrEmpty(cnn.Database) ? null : cnn.Database;
        }

        /// <summary>
        /// Returns <see langword="null"/>, meaning every schema: ODBC has no portable way to ask which schema an
        /// unqualified name resolves in.
        /// </summary>
        /// <returns><see langword="null"/>.</returns>
        public override string? GetDefaultSchema()
        {
            return null;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Built on first use from the product name and version the driver reports (see
        /// <see cref="AdoSqlDialects.ForConnection"/>), which opens a connection, and then kept.
        /// </remarks>
        public override SqlDialect Dialect => _dialect ??= CreateDialect();

        SqlDialect? _dialect;

        /// <summary>
        /// Opens a connection and chooses the dialect for the product behind the driver.
        /// </summary>
        /// <returns>The dialect.</returns>
        SqlDialect CreateDialect()
        {
            using var cnn = _dbDataSource.OpenConnection();

            return AdoSqlDialects.ForConnection(cnn);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Every parameter is written <c>?</c>: <see cref="OdbcCommand"/> binds by position and ignores the name.
        /// </remarks>
        public override IAdoSqlSyntax Syntax { get; } = new OdbcSqlSyntax();

        /// <summary>
        /// Writes every parameter as <c>?</c>.
        /// </summary>
        sealed class OdbcSqlSyntax : IAdoSqlSyntax
        {

            /// <inheritdoc />
            public string GetParameterName(int index) => "?";

        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            var set = new HashSet<AdoSchemaMetadata>();

            foreach (var row in Rows("Tables", databaseName, null, null))
                if (SchemaRow.String(row, "TABLE_SCHEM") is string schemaName)
                    set.Add(new AdoSchemaMetadata(schemaName));

            return set;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName)
        {
            var set = new HashSet<AdoTableMetadata>();

            foreach (var row in Rows("Tables", databaseName, schemaName, null))
                if (SchemaRow.String(row, "TABLE_NAME") is string tableName)
                    set.Add(new AdoTableMetadata(SchemaRow.String(row, "TABLE_CAT"), SchemaRow.String(row, "TABLE_SCHEM"), tableName));

            return set;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoFieldMetadata> GetFields(string? databaseName, string? schemaName, string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);

            var set = new HashSet<AdoFieldMetadata>();

            foreach (var row in Rows("Columns", databaseName, schemaName, tableName))
            {
                var name = SchemaRow.String(row, "COLUMN_NAME");
                if (name is null)
                    continue;

                // COLUMN_SIZE is a character column's length and a numeric column's precision; an unbounded
                // column (SQL Server's varchar(max), xml) reports zero
                var size = SchemaRow.Int32(row, "COLUMN_SIZE") is int columnSize && columnSize > 0 ? columnSize : (int?)null;

                set.Add(new AdoFieldMetadata(
                    name,
                    ParseDbType(SchemaRow.Int32(row, "DATA_TYPE") ?? SqlUnknownType),
                    size,
                    size,
                    SchemaRow.Int32(row, "DECIMAL_DIGITS"),
                    // SQL_NO_NULLS is 0 and SQL_NULLABLE_UNKNOWN 2: only a stated no is not nullable
                    SchemaRow.Int32(row, "NULLABLE") != 0));
            }

            return set;
        }

        /// <summary>
        /// Returns the rows of a schema collection for a catalog, schema and table, a null for any of them matching
        /// every one. A named catalog is switched to with <see cref="DbConnection.ChangeDatabase"/> first.
        /// </summary>
        /// <param name="collectionName">The schema collection.</param>
        /// <param name="databaseName">The catalog, or <see langword="null"/>.</param>
        /// <param name="schemaName">The schema, or <see langword="null"/>.</param>
        /// <param name="tableName">The table, or <see langword="null"/>.</param>
        /// <returns>The matching rows.</returns>
        IEnumerable<DataRow> Rows(string collectionName, string? databaseName, string? schemaName, string? tableName)
        {
            using var cnn = _dbDataSource.OpenConnection();

            if (databaseName is not null)
                cnn.ChangeDatabase(databaseName);

            using var result = cnn.GetSchema(collectionName);

            var rows = new List<DataRow>();
            foreach (DataRow row in result.Rows)
            {
                if (databaseName is not null && SchemaRow.String(row, "TABLE_CAT") != databaseName)
                    continue;
                if (schemaName is not null && SchemaRow.String(row, "TABLE_SCHEM") != schemaName)
                    continue;
                if (tableName is not null && SchemaRow.String(row, "TABLE_NAME") != tableName)
                    continue;

                rows.Add(row);
            }

            return rows;
        }

        #region Type codes

        // the concise type codes SQLColumns reports, from ODBC's sql.h and sqlext.h
        const int SqlUnknownType = 0;
        const int SqlChar = 1;
        const int SqlNumeric = 2;
        const int SqlDecimal = 3;
        const int SqlInteger = 4;
        const int SqlSmallint = 5;
        const int SqlFloat = 6;
        const int SqlReal = 7;
        const int SqlDouble = 8;
        const int SqlVarchar = 12;
        const int SqlLongVarchar = -1;
        const int SqlBinary = -2;
        const int SqlVarbinary = -3;
        const int SqlLongVarbinary = -4;
        const int SqlBigint = -5;
        const int SqlTinyint = -6;
        const int SqlBit = -7;
        const int SqlWchar = -8;
        const int SqlWvarchar = -9;
        const int SqlWlongVarchar = -10;
        const int SqlGuid = -11;

        // ODBC 2 codes the datetime types 9, 10 and 11 and ODBC 3 codes them 91, 92 and 93; a driver answers in
        // the version the application asked for
        const int SqlDateV2 = 9;
        const int SqlTimeV2 = 10;
        const int SqlTimestampV2 = 11;
        const int SqlTypeDate = 91;
        const int SqlTypeTime = 92;
        const int SqlTypeTimestamp = 93;

        // SQL Server's driver-specific codes. System.Data.Odbc cannot read SQL_SS_TIME2 or SQL_SS_TIMESTAMPOFFSET
        // (it throws ArgumentException), but mapping them gives the columns their proper types
        const int SqlSsVariant = -150;
        const int SqlSsUdt = -151;
        const int SqlSsXml = -152;
        const int SqlSsTime2 = -154;
        const int SqlSsTimestampOffset = -155;

        #endregion

        /// <summary>
        /// Returns the <see cref="DbType"/> for an ODBC type code.
        /// </summary>
        /// <param name="dataType">The code from <c>DATA_TYPE</c>.</param>
        /// <returns>The type.</returns>
        /// <remarks>
        /// A code with no mapping, the interval types included, is <see cref="DbType.Object"/>, which the adapter
        /// reads as <c>OTHER</c> and passes through unchanged.
        /// </remarks>
        static DbType ParseDbType(int dataType)
        {
            return dataType switch
            {
                SqlBit => DbType.Boolean,
                // ODBC does not say whether a tiny integer is signed. SQL Server's is not, so this is the
                // unsigned DbType.Byte (UTINYINT); a driver whose tiny integer is signed loses the negative half
                SqlTinyint => DbType.Byte,
                SqlSmallint => DbType.Int16,
                SqlInteger => DbType.Int32,
                SqlBigint => DbType.Int64,
                SqlDecimal or SqlNumeric => DbType.Decimal,
                SqlFloat or SqlDouble => DbType.Double,
                SqlReal => DbType.Single,
                SqlChar => DbType.AnsiStringFixedLength,
                SqlWchar => DbType.StringFixedLength,
                SqlVarchar or SqlLongVarchar => DbType.AnsiString,
                SqlWvarchar or SqlWlongVarchar => DbType.String,
                SqlSsXml => DbType.Xml,
                SqlBinary or SqlVarbinary or SqlLongVarbinary => DbType.Binary,
                SqlGuid => DbType.Guid,
                SqlTypeDate or SqlDateV2 => DbType.Date,
                SqlTypeTime or SqlTimeV2 or SqlSsTime2 => DbType.Time,
                SqlTypeTimestamp or SqlTimestampV2 => DbType.DateTime,
                SqlSsTimestampOffset => DbType.DateTimeOffset,
                SqlSsVariant or SqlSsUdt => DbType.Object,
                _ => DbType.Object,
            };
        }

    }

}
