using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoDatabaseMetadata"/> for any <see cref="OdbcConnection"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ODBC's catalog is not the SQL <c>INFORMATION_SCHEMA</c> and does not have its shape: the schema
    /// collections come from <c>SQLTables</c> and <c>SQLColumns</c>, which name the catalog and schema
    /// <c>TABLE_CAT</c> and <c>TABLE_SCHEM</c>, state the type as the numeric <c>DATA_TYPE</c> code rather
    /// than a name, and carry no <c>NUMERIC_PRECISION</c> at all — <c>COLUMN_SIZE</c> is the length of a
    /// character column and the precision of a numeric one.
    /// </para>
    /// <para>
    /// What sits behind the driver is unknown, so there is no default schema to speak of: a null database or
    /// schema means every one rather than a particular one, and a caller who wants a single schema names it.
    /// </para>
    /// </remarks>
    class OdbcDatabaseMetadata : AdoDatabaseMetadata
    {

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource"></param>
        public OdbcDatabaseMetadata(DbDataSource dbDataSource)
        {
            _dbDataSource = dbDataSource ?? throw new ArgumentNullException(nameof(dbDataSource));
        }

        /// <summary>
        /// Gets the data source this metadata describes.
        /// </summary>
        public DbDataSource DbDataSource => _dbDataSource;

        /// <inheritdoc />
        public override string? GetDefaultDatabase()
        {
            using var cnn = _dbDataSource.OpenConnection();

            // a driver that has no notion of a catalog reports an empty one
            return string.IsNullOrEmpty(cnn.Database) ? null : cnn.Database;
        }

        /// <inheritdoc />
        /// <remarks>
        /// ODBC has no portable way to ask which schema an identifier resolves in, so there is no answer
        /// but "all of them".
        /// </remarks>
        public override string? GetDefaultSchema()
        {
            return null;
        }

        /// <inheritdoc />
        public override SqlDialect Dialect => _dialect ??= CreateDialect();

        SqlDialect? _dialect;

        /// <summary>
        /// Asks the driver what is behind it, once: the convention reads the dialect for every rule that
        /// matches while planning.
        /// </summary>
        /// <returns></returns>
        SqlDialect CreateDialect()
        {
            using var cnn = _dbDataSource.OpenConnection();

            return AdoSqlDialects.ForConnection(cnn);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The parameter marker is <c>?</c> and positional: <see cref="OdbcCommand"/> binds by ordinal and
        /// ignores the name.
        /// </remarks>
        public override IAdoSqlSyntax Syntax { get; } = new OdbcSqlSyntax();

        /// <summary>
        /// ODBC names no parameter; it counts them.
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

                // COLUMN_SIZE is the character count of a character column and the precision of a numeric
                // one, and a driver reports zero for a column with no bound — SQL Server's varchar(max) and
                // xml both arrive as zero rather than as the -1 the information schema gives
                var size = SchemaRow.Int32(row, "COLUMN_SIZE") is int columnSize && columnSize > 0 ? columnSize : (int?)null;

                set.Add(new AdoFieldMetadata(
                    name,
                    ParseDbType(SchemaRow.Int32(row, "DATA_TYPE") ?? SqlUnknownType),
                    size,
                    size,
                    SchemaRow.Int32(row, "DECIMAL_DIGITS"),
                    // SQL_NO_NULLS is zero and SQL_NULLABLE_UNKNOWN is two: only a stated no is a no
                    SchemaRow.Int32(row, "NULLABLE") != 0));
            }

            return set;
        }

        /// <summary>
        /// Returns the rows of a schema collection that describe one catalog, schema and table, taking a
        /// null for any of them as every one.
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
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

        // ODBC 2 spelled the datetime types 9, 10 and 11; ODBC 3 spells them 91, 92 and 93, and a driver
        // answers in whichever version the application asked for
        const int SqlDateV2 = 9;
        const int SqlTimeV2 = 10;
        const int SqlTimestampV2 = 11;
        const int SqlTypeDate = 91;
        const int SqlTypeTime = 92;
        const int SqlTypeTimestamp = 93;

        // SQL Server's driver-specific codes. System.Data.Odbc has no mapping for SQL_SS_TIME2 or
        // SQL_SS_TIMESTAMPOFFSET and throws ArgumentException on reading either, whatever they are typed as
        // here; they are named so that the column at least appears with the type it has
        const int SqlSsVariant = -150;
        const int SqlSsUdt = -151;
        const int SqlSsXml = -152;
        const int SqlSsTime2 = -154;
        const int SqlSsTimestampOffset = -155;

        #endregion

        /// <summary>
        /// Returns the <see cref="DbType"/> for an ODBC type code.
        /// </summary>
        /// <param name="dataType"></param>
        /// <returns></returns>
        /// <remarks>
        /// A code with no mapping goes to <see cref="DbType.Object"/>, which <c>AdoTable</c> maps to
        /// <c>OTHER</c> and the reader passes through: an unrecognised column costs that column rather than
        /// the table. The interval types are among them, Calcite's own interval being a different thing.
        /// </remarks>
        static DbType ParseDbType(int dataType)
        {
            return dataType switch
            {
                SqlBit => DbType.Boolean,
                // ODBC does not say whether the driver's tiny integer is signed, and SQL Server's is not:
                // DbType.Byte is the unsigned one, which AdoTable holds as a UTINYINT; a driver whose tiny
                // integer really is signed loses the negative half, and ODBC gives no way to tell
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
