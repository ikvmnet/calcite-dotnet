using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoDatabaseMetadata"/> for any <see cref="OleDbConnection"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// OLE DB's schema rowsets borrow the information schema's column names and not its types: the type is
    /// the numeric <c>DBTYPE</c> in <c>DATA_TYPE</c> rather than a name, <c>IS_NULLABLE</c> is a
    /// <see cref="bool"/>, <c>CHARACTER_MAXIMUM_LENGTH</c> is a <see cref="decimal"/> and
    /// <c>NUMERIC_SCALE</c> a <see cref="short"/>. A <c>DBTYPE</c> says nothing about whether a character or
    /// binary column is fixed or varying, so that comes from <c>COLUMN_FLAGS</c>.
    /// </para>
    /// <para>
    /// What sits behind the provider is unknown, so a null database or schema means every one rather than a
    /// particular one, and a caller who wants a single schema names it.
    /// </para>
    /// </remarks>
    class OleDbDatabaseMetadata : AdoDatabaseMetadata
    {

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource"></param>
        public OleDbDatabaseMetadata(DbDataSource dbDataSource)
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

            // a provider over something with no catalogs reports an empty one
            return string.IsNullOrEmpty(cnn.Database) ? null : cnn.Database;
        }

        /// <inheritdoc />
        /// <remarks>
        /// OLE DB has no portable way to ask which schema an identifier resolves in, so there is no answer
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
        /// Asks the provider what is behind it, once: the convention reads the dialect for every rule that
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
        /// The parameter marker is <c>?</c> and positional: <see cref="OleDbCommand"/> binds by ordinal and
        /// ignores the name.
        /// </remarks>
        public override IAdoSqlSyntax Syntax { get; } = new OleDbSqlSyntax();

        /// <summary>
        /// OLE DB names no parameter; it counts them.
        /// </summary>
        sealed class OleDbSqlSyntax : IAdoSqlSyntax
        {

            /// <inheritdoc />
            public string GetParameterName(int index) => "?";

        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            var set = new HashSet<AdoSchemaMetadata>();

            foreach (var row in Rows("Tables", databaseName, null, null))
                if (SchemaRow.String(row, "TABLE_SCHEMA") is string schemaName)
                    set.Add(new AdoSchemaMetadata(schemaName));

            return set;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName)
        {
            var set = new HashSet<AdoTableMetadata>();

            foreach (var row in Rows("Tables", databaseName, schemaName, null))
                if (SchemaRow.String(row, "TABLE_NAME") is string tableName)
                    set.Add(new AdoTableMetadata(SchemaRow.String(row, "TABLE_CATALOG"), SchemaRow.String(row, "TABLE_SCHEMA"), tableName));

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

                // a provider reports zero for a column with no bound: SQL Server's varchar(max) and xml both
                // arrive as zero rather than as the -1 the information schema gives
                var size = SchemaRow.Int32(row, "CHARACTER_MAXIMUM_LENGTH") is int length && length > 0 ? length : (int?)null;
                var flags = SchemaRow.Int64(row, "COLUMN_FLAGS") ?? 0;

                set.Add(new AdoFieldMetadata(
                    name,
                    ParseDbType(SchemaRow.Int32(row, "DATA_TYPE") ?? DbTypeEmpty, (flags & DbColumnFlagsIsFixedLength) != 0),
                    size,
                    SchemaRow.Int32(row, "NUMERIC_PRECISION"),
                    SchemaRow.Int32(row, "NUMERIC_SCALE"),
                    SchemaRow.Boolean(row, "IS_NULLABLE") ?? true));
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
                if (databaseName is not null && SchemaRow.String(row, "TABLE_CATALOG") != databaseName)
                    continue;
                if (schemaName is not null && SchemaRow.String(row, "TABLE_SCHEMA") != schemaName)
                    continue;
                if (tableName is not null && SchemaRow.String(row, "TABLE_NAME") != tableName)
                    continue;

                rows.Add(row);
            }

            return rows;
        }

        #region Type codes

        // the DBTYPE enumeration from OLE DB's oledb.h, and the three SQL Server adds to it
        const int DbTypeEmpty = 0;
        const int DbTypeNull = 1;
        const int DbTypeI2 = 2;
        const int DbTypeI4 = 3;
        const int DbTypeR4 = 4;
        const int DbTypeR8 = 5;
        const int DbTypeCy = 6;
        const int DbTypeDate = 7;
        const int DbTypeBstr = 8;
        const int DbTypeBool = 11;
        const int DbTypeVariant = 12;
        const int DbTypeDecimal = 14;
        const int DbTypeI1 = 16;
        const int DbTypeUi1 = 17;
        const int DbTypeUi2 = 18;
        const int DbTypeUi4 = 19;
        const int DbTypeI8 = 20;
        const int DbTypeUi8 = 21;
        const int DbTypeFileTime = 64;
        const int DbTypeGuid = 72;
        const int DbTypeBytes = 128;
        const int DbTypeStr = 129;
        const int DbTypeWstr = 130;
        const int DbTypeNumeric = 131;
        const int DbTypeUdt = 132;
        const int DbTypeDbDate = 133;
        const int DbTypeDbTime = 134;
        const int DbTypeDbTimestamp = 135;
        const int DbTypeVarNumeric = 139;
        const int DbTypeXml = 141;
        const int DbTypeDbTime2 = 145;
        const int DbTypeDbTimestampOffset = 146;

        /// <summary>
        /// The modifier bits a <c>DBTYPE</c> can carry — <c>DBTYPE_VECTOR</c>, <c>DBTYPE_ARRAY</c>,
        /// <c>DBTYPE_BYREF</c> and <c>DBTYPE_RESERVED</c> — which say how the value is handed over rather
        /// than what it is.
        /// </summary>
        const int DbTypeModifierMask = 0x1000 | 0x2000 | 0x4000 | 0x8000;

        /// <summary>
        /// <c>DBCOLUMNFLAGS_ISFIXEDLENGTH</c>: set for <c>char</c> and clear for <c>varchar</c>, which the
        /// <c>DBTYPE</c> alone does not distinguish.
        /// </summary>
        const long DbColumnFlagsIsFixedLength = 0x10;

        #endregion

        /// <summary>
        /// Returns the <see cref="DbType"/> for a <c>DBTYPE</c> code.
        /// </summary>
        /// <param name="dataType"></param>
        /// <param name="fixedLength"></param>
        /// <returns></returns>
        /// <remarks>
        /// A code with no mapping goes to <see cref="DbType.Object"/>, which <c>AdoTable</c> maps to
        /// <c>OTHER</c> and the reader passes through: an unrecognised column costs that column rather than
        /// the table.
        /// </remarks>
        static DbType ParseDbType(int dataType, bool fixedLength)
        {
            return (dataType & ~DbTypeModifierMask) switch
            {
                DbTypeBool => DbType.Boolean,
                // DBTYPE_I1 is the signed one and DBTYPE_UI1 the unsigned; AdoTable holds the unsigned in a
                // UTINYINT, TINYINT being signed
                DbTypeI1 => DbType.SByte,
                DbTypeUi1 => DbType.Byte,
                DbTypeI2 => DbType.Int16,
                DbTypeUi2 => DbType.UInt16,
                DbTypeI4 => DbType.Int32,
                DbTypeUi4 => DbType.UInt32,
                DbTypeI8 => DbType.Int64,
                DbTypeUi8 => DbType.UInt64,
                DbTypeNumeric or DbTypeDecimal or DbTypeVarNumeric => DbType.Decimal,
                DbTypeCy => DbType.Currency,
                DbTypeR4 => DbType.Single,
                DbTypeR8 => DbType.Double,
                DbTypeStr => fixedLength ? DbType.AnsiStringFixedLength : DbType.AnsiString,
                DbTypeWstr or DbTypeBstr => fixedLength ? DbType.StringFixedLength : DbType.String,
                DbTypeXml => DbType.Xml,
                DbTypeBytes => DbType.Binary,
                DbTypeGuid => DbType.Guid,
                DbTypeDbDate => DbType.Date,
                DbTypeDbTime or DbTypeDbTime2 => DbType.Time,
                // DBTYPE_DATE is an OLE automation double and DBTYPE_FILETIME a 64 bit tick count, but a
                // provider hands either over as a DateTime and that is what the reader asks for
                DbTypeDbTimestamp or DbTypeDate or DbTypeFileTime => DbType.DateTime,
                DbTypeDbTimestampOffset => DbType.DateTimeOffset,
                DbTypeNull or DbTypeEmpty or DbTypeVariant or DbTypeUdt => DbType.Object,
                _ => DbType.Object,
            };
        }

    }

}
