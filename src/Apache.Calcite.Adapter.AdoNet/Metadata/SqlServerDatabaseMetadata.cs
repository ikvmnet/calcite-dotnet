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
    /// Implements the <see cref="AdoDatabaseMetadata"/> for Microsoft SQL Server.
    /// </summary>
    class SqlServerDatabaseMetadata : AdoInformationSchemaDatabaseMetadata
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource"></param>
        public SqlServerDatabaseMetadata(DbDataSource dbDataSource) :
            base(dbDataSource)
        {

        }

        /// <inheritdoc />
        public override string? GetDefaultDatabase()
        {
            // use the generic datastring builder to parse
            var connectionString = new DbConnectionStringBuilder();
            connectionString.ConnectionString = DbDataSource.ConnectionString;

            // check for Initial Catalog
            connectionString.TryGetValue("Initial Catalog", out object? initialCatalog);
            if (initialCatalog is string initialCatalogStr)
                if (string.IsNullOrWhiteSpace(initialCatalogStr) == false)
                    return initialCatalogStr;

            // check for Database
            connectionString.TryGetValue("Database", out object? database);
            if (database is string databaseStr)
                if (string.IsNullOrWhiteSpace(databaseStr) == false)
                    return databaseStr;

            return base.GetDefaultDatabase();
        }

        /// <inheritdoc />
        public override string GetDefaultSchema()
        {
            return "dbo";
        }

        /// <inheritdoc />
        /// <remarks>
        /// Worked out once and kept: deriving it asks the server for its version, and the convention reads
        /// it for every rule that matches while planning.
        /// </remarks>
        public override SqlDialect Dialect => _dialect ??= CreateDialect();

        SqlDialect? _dialect;

        /// <inheritdoc />
        /// <remarks>
        /// SqlClient binds the default <c>@P</c> parameter form, so the naming is the interface's own. What this states
        /// is a rewrite: a <c>UUID</c> literal unparses as the standard typed literal <c>UUID '…'</c> —
        /// <c>SqlUuidLiteral.unparse</c> consults no dialect — and SQL Server has neither that literal syntax nor the
        /// <c>UUID</c> type name, so it answers "Incorrect syntax" on the string. Until Calcite lets the dialect render
        /// the literal, the syntax turns each one into <c>CAST('…' AS uniqueidentifier)</c>, naming the type the way the
        /// dialect's own cast spec does. It is a dialect concern done from the driver's side because that is the seam
        /// there is.
        /// </remarks>
        public override IAdoSqlSyntax Syntax => _syntax ??= new SqlServerSqlSyntax();

        IAdoSqlSyntax? _syntax;

        /// <summary>
        /// The SQL Server driver's syntax: the default parameter naming, and a rewrite of every <c>UUID</c> literal
        /// into a cast a server with no <c>UUID</c> literal can parse.
        /// </summary>
        sealed class SqlServerSqlSyntax : IAdoSqlSyntax
        {

            /// <inheritdoc />
            public SqlNode Rewrite(SqlNode statement, SqlDialect dialect, RelDataTypeFactory typeFactory)
            {
                return (SqlNode)statement.accept(new UuidLiteralShuttle(dialect, typeFactory));
            }

            /// <summary>
            /// Rewrites every <c>UUID</c> literal into an explicit cast of its text, so the dialect names the type.
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
        /// Asks the server what it is, and describes it to Calcite.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// The version is the part that has to be asked for: under major version 11
        /// <see cref="MssqlSqlDialect"/> writes <c>TOP(n)</c> and discards the offset, so a paged query
        /// silently returns the first page for every page. The rest of the context is
        /// <see cref="MssqlSqlDialect.DEFAULT_CONTEXT"/>'s, which already states the bracket quoting, the
        /// type system and the low null collation.
        /// </remarks>
        SqlDialect CreateDialect()
        {
            using var cnn = DbDataSource.OpenConnection();

            return AdoSqlDialects.CreateMssql("Microsoft SQL Server", cnn.ServerVersion);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Every type the server names in <c>INFORMATION_SCHEMA.COLUMNS.DATA_TYPE</c>, because a name that
        /// is missing does not cost that column — it throws, and takes the whole table with it. The spatial
        /// and hierarchy types, and <c>sql_variant</c>, go to <see cref="DbType.Object"/>, which
        /// <c>AdoTable</c> maps to <c>OTHER</c> and the reader passes through untouched.
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
                // money is a decimal of a fixed scale of its own, which is what DbType.Currency states
                "money" or "smallmoney" => DbType.Currency,
                // float is the eight byte one whatever its declared mantissa: the server reports a
                // float(1..24) as 'real', so this name is only ever the wide type
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
                // rowversion is spelled 'timestamp' here and is eight opaque bytes, not a time
                "binary" or "varbinary" or "image" or "timestamp" or "rowversion" => DbType.Binary,
                _ => DbType.Object,
            };
        }


    }

}
