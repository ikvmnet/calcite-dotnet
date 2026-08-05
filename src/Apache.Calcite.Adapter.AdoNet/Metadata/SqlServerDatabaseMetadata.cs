using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoDatabaseMetadata"/> for Microsoft SQL Server.
    /// </summary>
    class SqlServerDatabaseMetadata : AdoInformationSchemaDatabaseMetadata<DbConnection>
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
        public override SqlDialect GetDialect()
        {
            using var cnn = DbDataSource.OpenConnection();

            return new MssqlSqlDialect(SqlDialect.EMPTY_CONTEXT
                .withDatabaseProductName("Microsoft SQL Server")
                .withDatabaseMajorVersion(ParseMajorVersion(cnn.ServerVersion))
                .withDatabaseMinorVersion(ParseMinorVersion(cnn.ServerVersion))
                .withDatabaseVersion(cnn.ServerVersion)
                .withIdentifierQuoteString("\"")
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(true)
                .withNullCollation(NullCollation.LOW));
        }

        /// <summary>
        /// Parses the major SQL Server version.
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        int ParseMajorVersion(string v)
        {
            int p = v.IndexOf('.');
            if (p > 0)
                v = v.Substring(0, p);

            return int.TryParse(v, out int r) ? r : 0;
        }

        /// <summary>
        /// Parses the minor SQL Server version.
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        int ParseMinorVersion(string v)
        {
            int p = v.IndexOf('.');
            int q = v.IndexOf('.', p + 1);
            if (p > 0 && q > 0)
                v = v.Substring(p + 1, q);

            return int.TryParse(v, out int r) ? r : 0;
        }

        /// <inheritdoc />
        protected override DbType ParseDbType(string typeName)
        {
            return typeName switch
            {
                "int" => DbType.Int32,
                "char" => DbType.AnsiStringFixedLength,
                "varchar" => DbType.AnsiString,
                "nchar" => DbType.StringFixedLength,
                "nvarchar" => DbType.String,
                "uniqueidentifier" => DbType.Guid,
                "datetime" => DbType.DateTime,
                "datetime2" => DbType.DateTime2,
                _ => throw new NotImplementedException($"Unsupported field type: {typeName}"),
            };
        }

        /// <inheritdoc />
        public override string GetParameterName(int index)
        {
            return $"@P{index}";
        }

    }

}
