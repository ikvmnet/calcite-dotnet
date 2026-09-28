using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// The metadata for any <see cref="OleDbConnection"/>, read from the OLE DB schema rowsets.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The rowsets use the information schema's column names but not its types: <c>DATA_TYPE</c> is a numeric
    /// <c>DBTYPE</c>, <c>IS_NULLABLE</c> a <see cref="bool"/>, <c>CHARACTER_MAXIMUM_LENGTH</c> a
    /// <see cref="decimal"/> and <c>NUMERIC_SCALE</c> a <see cref="short"/>. Whether a character column is fixed
    /// or varying comes from <c>COLUMN_FLAGS</c>, since the <c>DBTYPE</c> does not say.
    /// </para>
    /// <para>
    /// The database behind the provider is unknown, so a null database or schema means every one, and a caller
    /// who wants one schema names it.
    /// </para>
    /// </remarks>
    class OleDbDatabaseMetadata : AdoDatabaseMetadata
    {

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource">The data source to read metadata from.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dbDataSource"/> is <see langword="null"/>.</exception>
        public OleDbDatabaseMetadata(DbDataSource dbDataSource)
        {
            _dbDataSource = dbDataSource ?? throw new ArgumentNullException(nameof(dbDataSource));
        }

        /// <summary>
        /// Gets the data source this metadata describes.
        /// </summary>
        public DbDataSource DbDataSource => _dbDataSource;

        /// <summary>
        /// Returns the catalog a new connection is in, or <see langword="null"/> where the provider reports none.
        /// </summary>
        /// <returns>The catalog, or <see langword="null"/>.</returns>
        public override string? GetDefaultDatabase()
        {
            using var cnn = _dbDataSource.OpenConnection();

            // a provider with no catalogs reports an empty one
            return string.IsNullOrEmpty(cnn.Database) ? null : cnn.Database;
        }

        /// <summary>
        /// Returns <see langword="null"/>, meaning every schema: OLE DB has no portable way to ask which schema an
        /// unqualified name resolves in.
        /// </summary>
        /// <returns><see langword="null"/>.</returns>
        public override string? GetDefaultSchema()
        {
            return null;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Built on first use from the product name and version the provider reports (see
        /// <see cref="AdoSqlDialects.ForConnection"/>), which opens a connection, and then kept.
        /// </remarks>
        public override SqlDialect Dialect => _dialect ??= CreateDialect();

        SqlDialect? _dialect;

        /// <summary>
        /// Opens a connection and chooses the dialect for the product behind the provider.
        /// </summary>
        /// <returns>The dialect.</returns>
        SqlDialect CreateDialect()
        {
            using var cnn = _dbDataSource.OpenConnection();

            return AdoSqlDialects.ForConnection(cnn);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Every parameter is written <c>?</c>: <see cref="OleDbCommand"/> binds by position and ignores the name.
        /// </remarks>
        public override IAdoSqlSyntax Syntax { get; } = new OleDbSqlSyntax();

        /// <summary>
        /// Writes every parameter as <c>?</c>.
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

                // an unbounded column (SQL Server's varchar(max), xml) reports zero
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

        // the DBTYPE enumeration from OLE DB's oledb.h, with SQL Server's additions
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
        /// The modifier bits of a <c>DBTYPE</c> (<c>DBTYPE_VECTOR</c>, <c>DBTYPE_ARRAY</c>, <c>DBTYPE_BYREF</c> and
        /// <c>DBTYPE_RESERVED</c>), which say how a value is passed rather than what it is.
        /// </summary>
        const int DbTypeModifierMask = 0x1000 | 0x2000 | 0x4000 | 0x8000;

        /// <summary>
        /// <c>DBCOLUMNFLAGS_ISFIXEDLENGTH</c>: set for <c>char</c> and clear for <c>varchar</c>.
        /// </summary>
        const long DbColumnFlagsIsFixedLength = 0x10;

        #endregion

        /// <summary>
        /// Returns the <see cref="DbType"/> for a <c>DBTYPE</c> code.
        /// </summary>
        /// <param name="dataType">The code from <c>DATA_TYPE</c>. Modifier bits are ignored.</param>
        /// <param name="fixedLength">Whether a character column is fixed-length.</param>
        /// <returns>The type.</returns>
        /// <remarks>
        /// A code with no mapping is <see cref="DbType.Object"/>, which the adapter reads as <c>OTHER</c> and passes
        /// through unchanged.
        /// </remarks>
        static DbType ParseDbType(int dataType, bool fixedLength)
        {
            return (dataType & ~DbTypeModifierMask) switch
            {
                DbTypeBool => DbType.Boolean,
                // DBTYPE_I1 is signed (TINYINT) and DBTYPE_UI1 unsigned (UTINYINT)
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
                // DBTYPE_DATE is an OLE automation double and DBTYPE_FILETIME a 64-bit tick count, but a provider
                // returns either as a DateTime
                DbTypeDbTimestamp or DbTypeDate or DbTypeFileTime => DbType.DateTime,
                DbTypeDbTimestampOffset => DbType.DateTimeOffset,
                DbTypeNull or DbTypeEmpty or DbTypeVariant or DbTypeUdt => DbType.Object,
                _ => DbType.Object,
            };
        }

    }

}
