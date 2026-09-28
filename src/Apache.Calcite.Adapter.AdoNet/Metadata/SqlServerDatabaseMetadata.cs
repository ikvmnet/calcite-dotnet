using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// The metadata for SQL Server, through <c>Microsoft.Data.SqlClient</c> or <c>System.Data.SqlClient</c>.
    /// </summary>
    /// <remarks>
    /// Tables and columns come from the <c>INFORMATION_SCHEMA</c>-shaped schema collections, the default schema is
    /// <c>dbo</c>, and the dialect is built for the version the server reports.
    /// </remarks>
    class SqlServerDatabaseMetadata : AdoInformationSchemaDatabaseMetadata
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource">The data source to read metadata from.</param>
        public SqlServerDatabaseMetadata(DbDataSource dbDataSource) :
            base(dbDataSource)
        {

        }

        /// <summary>
        /// Returns the connection string's <c>Initial Catalog</c> or <c>Database</c>, and otherwise the database a new
        /// connection is in.
        /// </summary>
        /// <returns>The database name.</returns>
        public override string? GetDefaultDatabase()
        {
            var connectionString = new DbConnectionStringBuilder();
            connectionString.ConnectionString = DbDataSource.ConnectionString;

            connectionString.TryGetValue("Initial Catalog", out object? initialCatalog);
            if (initialCatalog is string initialCatalogStr)
                if (string.IsNullOrWhiteSpace(initialCatalogStr) == false)
                    return initialCatalogStr;

            connectionString.TryGetValue("Database", out object? database);
            if (database is string databaseStr)
                if (string.IsNullOrWhiteSpace(databaseStr) == false)
                    return databaseStr;

            return base.GetDefaultDatabase();
        }

        /// <summary>
        /// Returns <c>dbo</c>.
        /// </summary>
        /// <returns><c>dbo</c>.</returns>
        public override string GetDefaultSchema()
        {
            return "dbo";
        }

        /// <inheritdoc />
        /// <remarks>
        /// Built on first use, which opens a connection to read the server's version, and then kept.
        /// </remarks>
        public override SqlDialect Dialect => _dialect ??= CreateDialect();

        SqlDialect? _dialect;

        /// <inheritdoc />
        /// <remarks>
        /// Parameters are named <c>@P0</c>, <c>@P1</c> and so on. Every <c>UUID</c> literal is rewritten to
        /// <c>CAST('…' AS UNIQUEIDENTIFIER)</c>: Calcite unparses it as <c>UUID '…'</c> without consulting the
        /// dialect (<c>SqlUuidLiteral.unparse</c>), and SQL Server has neither that literal syntax nor a <c>UUID</c>
        /// type. The rewrite can go once Calcite lets a dialect write the literal.
        /// </remarks>
        public override IAdoSqlSyntax Syntax => _syntax ??= new SqlServerSqlSyntax();

        IAdoSqlSyntax? _syntax;

        /// <summary>
        /// SQL Server's syntax: the default parameter names, and every <c>UUID</c> literal rewritten as a cast of its
        /// text.
        /// </summary>
        sealed class SqlServerSqlSyntax : IAdoSqlSyntax
        {

            /// <inheritdoc />
            public SqlNode Rewrite(SqlNode statement, SqlDialect dialect, RelDataTypeFactory typeFactory)
            {
                return (SqlNode)statement.accept(new UuidLiteralShuttle(dialect, typeFactory));
            }

            /// <summary>
            /// Rewrites every <c>UUID</c> literal as a cast of its text to the type the dialect's cast spec names.
            /// </summary>
            sealed class UuidLiteralShuttle(SqlDialect dialect, RelDataTypeFactory typeFactory) : SqlShuttle
            {

                /// <inheritdoc />
                public override SqlNode visit(SqlLiteral literal)
                {
                    if (literal.getTypeName()?.name() != nameof(SqlTypeName.UUID))
                        return base.visit(literal);

                    var value = (java.util.UUID)literal.getValueAs((java.lang.Class)typeof(java.util.UUID));
                    var text = SqlLiteral.createCharString(value.toString(), SqlParserPos.ZERO);
                    var spec = dialect.getCastSpec(typeFactory.createSqlType(SqlTypeName.UUID));
                    return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, text, spec);
                }

            }

        }

        /// <summary>
        /// Opens a connection and builds the dialect for the server's version.
        /// </summary>
        /// <returns>The dialect.</returns>
        /// <remarks>
        /// The version matters: below major version 11 (SQL Server 2012), <see cref="MssqlSqlDialect"/> writes
        /// <c>TOP(n)</c> and drops the offset, so every page of a paged query would be the first. The rest is
        /// <see cref="MssqlSqlDialect.DEFAULT_CONTEXT"/>.
        /// </remarks>
        SqlDialect CreateDialect()
        {
            using var cnn = DbDataSource.OpenConnection();

            return AdoSqlDialects.CreateMssql("Microsoft SQL Server", cnn.ServerVersion);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Any name not listed, including the spatial and hierarchy types and <c>sql_variant</c>, maps to
        /// <see cref="DbType.Object"/>, which the adapter reads as <c>OTHER</c> and passes through unchanged.
        /// </remarks>
        protected override DbType ParseDbType(string typeName)
        {
            return typeName.ToLowerInvariant() switch
            {
                "bit" => DbType.Boolean,
                "tinyint" => DbType.Byte,
                "smallint" => DbType.Int16,
                "int" => DbType.Int32,
                "bigint" => DbType.Int64,
                "decimal" or "numeric" => DbType.Decimal,
                // money and smallmoney have a fixed scale of four
                "money" or "smallmoney" => DbType.Currency,
                // the server reports float(1..24) as 'real', so 'float' is always the eight-byte type
                "float" => DbType.Double,
                "real" => DbType.Single,
                "char" => DbType.AnsiStringFixedLength,
                "varchar" or "text" => DbType.AnsiString,
                "nchar" => DbType.StringFixedLength,
                "nvarchar" or "ntext" => DbType.String,
                "xml" => DbType.Xml,
                "uniqueidentifier" => DbType.Guid,
                "date" => DbType.Date,
                "time" => DbType.Time,
                "datetime" or "smalldatetime" => DbType.DateTime,
                "datetime2" => DbType.DateTime2,
                "datetimeoffset" => DbType.DateTimeOffset,
                // 'timestamp' is rowversion: eight opaque bytes, not a time
                "binary" or "varbinary" or "image" or "timestamp" or "rowversion" => DbType.Binary,
                _ => DbType.Object,
            };
        }


    }

}
