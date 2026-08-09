using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Works out which <see cref="SqlDialect"/> a provider is speaking to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A driver that fronts one database knows what it is; ODBC and OLE DB do not, and the only thing they
    /// can be asked is the <c>DataSourceInformation</c> collection, whose <c>DataSourceProductName</c> is
    /// the same string JDBC's <c>getDatabaseProductName</c> returns. So the name is matched with the tests
    /// <c>SqlDialectFactoryImpl.create</c> applies to it, and the answer is that product's own dialect.
    /// </para>
    /// <para>
    /// Calcite calls what a product name alone can give you "a dummy dialect ... at best an approximation",
    /// and it is: <see cref="SqlDialect.DatabaseProduct.getDialect"/> carries no version, and a dialect can
    /// turn on one. <see cref="MssqlSqlDialect"/> does — under version 11 it writes <c>TOP(n)</c> and
    /// <em>discards the offset</em>, so a paged query returns the first page for every page. That one is
    /// therefore built from the version, which both providers do report. A provider that needs more than
    /// this derives from <see cref="AdoDatabaseMetadata"/> and answers <c>Dialect</c> itself, which is what
    /// <see cref="SqlServerDatabaseMetadata"/> does.
    /// </para>
    /// </remarks>
    static class AdoSqlDialects
    {

        /// <summary>
        /// Asks a connection what it is talking to and returns the dialect for it.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public static SqlDialect ForConnection(DbConnection connection)
        {
            string? productName = null;
            string? productVersion = null;

            try
            {
                using var information = connection.GetSchema(DbMetaDataCollectionNames.DataSourceInformation);
                if (information.Rows.Count > 0)
                {
                    productName = SchemaRow.String(information.Rows[0], DbMetaDataColumnNames.DataSourceProductName);
                    productVersion = SchemaRow.String(information.Rows[0], DbMetaDataColumnNames.DataSourceProductVersion);
                }
            }
            catch (Exception)
            {
                // a driver need not offer the collection at all, and an unknown product is a supported answer
            }

            // the connection's own version is the better one where the collection did not carry it: ODBC and
            // OLE DB both fill ServerVersion from the same place
            if (string.IsNullOrWhiteSpace(productVersion))
                productVersion = TryGetServerVersion(connection);

            return For(productName, productVersion);
        }

        /// <summary>
        /// Reads <see cref="DbConnection.ServerVersion"/>, which a driver is entitled to refuse.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        static string? TryGetServerVersion(DbConnection connection)
        {
            try
            {
                return connection.ServerVersion;
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// Returns the dialect for a product name, as Calcite matches one.
        /// </summary>
        /// <param name="productName"></param>
        /// <param name="productVersion"></param>
        /// <returns></returns>
        public static SqlDialect For(string? productName, string? productVersion)
        {
            var product = ProductFor(productName);

            // the one product whose dialect answers differently for different versions, and answers wrongly
            // rather than conservatively when it guesses low
            if (product == SqlDialect.DatabaseProduct.MSSQL)
                return CreateMssql(productName, productVersion);

            // UNKNOWN's own dialect is a bare SqlDialect over the enum's quote character; the generic
            // dialect Calcite's create ends at is the ANSI one, and that is the answer being reproduced
            if (product == SqlDialect.DatabaseProduct.UNKNOWN)
                return AnsiSqlDialect.DEFAULT;

            return product.getDialect();
        }

        /// <summary>
        /// Builds the SQL Server dialect against the version the server reported.
        /// </summary>
        /// <param name="productName"></param>
        /// <param name="productVersion"></param>
        /// <returns></returns>
        public static SqlDialect CreateMssql(string? productName, string? productVersion)
        {
            var context = MssqlSqlDialect.DEFAULT_CONTEXT
                .withDatabaseProductName(productName ?? "Microsoft SQL Server")
                .withDatabaseMajorVersion(MajorVersion(productVersion))
                .withDatabaseMinorVersion(MinorVersion(productVersion));

            if (productVersion is not null)
                context = context.withDatabaseVersion(productVersion);

            return new MssqlSqlDialect(context);
        }

        /// <summary>
        /// Matches a product name to a product, with the exact names and then the fuzzy tests
        /// <c>SqlDialectFactoryImpl.create</c> applies in that order.
        /// </summary>
        /// <param name="productName"></param>
        /// <returns></returns>
        /// <remarks>
        /// Ported rather than called: <c>create</c> takes a JDBC <c>DatabaseMetaData</c>, and there is none
        /// here. Where Calcite dispatches to a dialect this dispatches to the product whose
        /// <c>getDialect</c> is that dialect; the products Calcite has a name test for but no product
        /// constant — Firebolt, Paraccel, Doris — fall where their names put them or to
        /// <see cref="SqlDialect.DatabaseProduct.UNKNOWN"/>, whose dialect is the generic one
        /// <c>create</c> ends at.
        /// </remarks>
        public static SqlDialect.DatabaseProduct ProductFor(string? productName)
        {
            var name = (productName ?? "").ToUpperInvariant().Trim();

            switch (name)
            {
                case "ACCESS":
                    return SqlDialect.DatabaseProduct.ACCESS;
                case "APACHE DERBY":
                case "DBMS:CLOUDSCAPE":
                    return SqlDialect.DatabaseProduct.DERBY;
                case "CLICKHOUSE":
                    return SqlDialect.DatabaseProduct.CLICKHOUSE;
                case "EXASOL":
                    return SqlDialect.DatabaseProduct.EXASOL;
                case "FIREBOLT":
                    return SqlDialect.DatabaseProduct.FIREBOLT;
                case "HIVE":
                    return SqlDialect.DatabaseProduct.HIVE;
                case "INGRES":
                    return SqlDialect.DatabaseProduct.INGRES;
                case "INTERBASE":
                    return SqlDialect.DatabaseProduct.INTERBASE;
                case "JETHRODATA":
                    return SqlDialect.DatabaseProduct.JETHRO;
                case "LUCIDDB":
                    return SqlDialect.DatabaseProduct.LUCIDDB;
                case "ORACLE":
                    return SqlDialect.DatabaseProduct.ORACLE;
                case "PHOENIX":
                    return SqlDialect.DatabaseProduct.PHOENIX;
                case "PRESTO":
                case "AWS.ATHENA":
                    return SqlDialect.DatabaseProduct.PRESTO;
                case "MYSQL (INFOBRIGHT)":
                    return SqlDialect.DatabaseProduct.INFOBRIGHT;
                case "MYSQL":
                    return SqlDialect.DatabaseProduct.MYSQL;
                case "REDSHIFT":
                    return SqlDialect.DatabaseProduct.REDSHIFT;
                case "SNOWFLAKE":
                    return SqlDialect.DatabaseProduct.SNOWFLAKE;
                case "SPARK":
                    return SqlDialect.DatabaseProduct.SPARK;
                default:
                    break;
            }

            // now the fuzzy matches, in Calcite's order: an earlier test wins where two would both hit
            if (name.StartsWith("DB2", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.DB2;
            if (name.Contains("FIREBIRD", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.FIREBIRD;
            if (name.Contains("FIREBOLT", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.FIREBOLT;
            if (name.Contains("GOOGLE BIGQUERY", StringComparison.Ordinal) || name.Contains("GOOGLE BIG QUERY", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.BIG_QUERY;
            if (name.StartsWith("INFORMIX", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.INFORMIX;
            if (name.Contains("NETEZZA", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.NETEZZA;
            if (name.Contains("PARACCEL", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.PARACCEL;
            if (name.StartsWith("HP NEOVIEW", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.NEOVIEW;
            if (name.Contains("POSTGRE", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.POSTGRESQL;
            if (name.Contains("SQL SERVER", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.MSSQL;
            if (name.Contains("SYBASE", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.SYBASE;
            if (name.Contains("TERADATA", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.TERADATA;
            if (name.Contains("HSQL", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.HSQLDB;
            if (name.Contains("H2", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.H2;
            if (name.Contains("VERTICA", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.VERTICA;
            if (name.Contains("SNOWFLAKE", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.SNOWFLAKE;
            if (name.Contains("SPARK", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.SPARK;

            // an addition rather than a port: Calcite's create has no SQLite branch, its JDBC driver being
            // one nobody had put through this, and a bridged driver over SQLite says exactly this
            if (name.Contains("SQLITE", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.SQLITE;

            return SqlDialect.DatabaseProduct.UNKNOWN;
        }

        /// <summary>
        /// Returns the leading component of a dotted version string, or zero.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        public static int MajorVersion(string? version)
        {
            return Component(version, 0);
        }

        /// <summary>
        /// Returns the second component of a dotted version string, or zero.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        public static int MinorVersion(string? version)
        {
            return Component(version, 1);
        }

        /// <summary>
        /// Returns one component of a dotted version string.
        /// </summary>
        /// <param name="version"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        static int Component(string? version, int index)
        {
            if (version is null)
                return 0;

            var parts = version.Split('.');
            return parts.Length > index && int.TryParse(parts[index], out var value) ? value : 0;
        }

    }

}
