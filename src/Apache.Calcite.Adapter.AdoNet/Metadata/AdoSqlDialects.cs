using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;

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

            return new Mssql(context);
        }

        /// <summary>
        /// <see cref="MssqlSqlDialect"/>, and the two things it does not say about SQL Server.
        /// </summary>
        /// <param name="context"></param>
        /// <remarks>
        /// <para>
        /// SQL Server cannot group by a constant, and <see cref="MssqlSqlDialect"/> does not declare it:
        /// <c>SqlDialect.supportsGroupByLiteral</c> defaults to true and Postgres, Redshift and Informix
        /// each override it while SQL Server does not. Measured — <c>GROUP BY (1 = 1)</c> is "Incorrect
        /// syntax near '='" and <c>GROUP BY 1</c> is "Each GROUP BY expression must contain at least one
        /// column that is not an outer reference".
        /// </para>
        /// <para>
        /// It costs every correlated sub-query. <c>EXISTS</c> becomes an aggregate over a constant true, and
        /// <c>SqlImplementor.visitRoot</c> only runs <c>AggregateProjectConstantToDummyJoinRule</c> — which
        /// exists for exactly this — when the dialect has said it is needed. Saying so is a correction to
        /// Calcite rather than a reproduction of it, which the adapter is entitled to make: it generates SQL
        /// for a server to run, and the server is the authority on what it accepts.
        /// </para>
        /// <para>
        /// The second is what an unbounded string casts to — see <see cref="Mssql.getCastSpec"/>, which is
        /// the same kind of correction and made for the same reason.
        /// </para>
        /// </remarks>
        sealed class Mssql(SqlDialect.Context context) : MssqlSqlDialect(context)
        {

            /// <inheritdoc />
            public override bool supportsGroupByLiteral()
            {
                return false;
            }

            /// <inheritdoc />
            /// <remarks>
            /// <para>
            /// A Calcite <c>VARCHAR</c> with no precision is unbounded, and <c>SqlDialect.getCastSpec</c>
            /// writes it as the bare keyword, its precision being the type system's
            /// <c>PRECISION_NOT_SPECIFIED</c>. A bare <c>varchar</c> in T-SQL is not unbounded: it is one
            /// character in a declaration and <em>thirty</em> in a <c>CAST</c> or <c>CONVERT</c>. So the
            /// cast that meant "no limit" silently becomes a thirty character one.
            /// </para>
            /// <para>
            /// Where the conversion cannot fit it raises rather than truncates and reads as a type problem
            /// in the caller's data — <c>CAST(&lt;uniqueidentifier&gt; AS VARCHAR)</c> is "Insufficient
            /// result space to convert uniqueidentifier value to char", a GUID being thirty-six. Where it
            /// fits it truncates: the same cast over a long <c>nvarchar</c> returns the first thirty
            /// characters and raises nothing. And it is not contained to a query that writes the cast, since
            /// comparing an unbounded string against a bounded column makes Calcite's coercion widen the
            /// column back to unbounded, so a caller who stated a length in a view still gets it.
            /// </para>
            /// <para>
            /// <c>varchar(max)</c> is SQL Server's own unbounded form and is what the type means.
            /// <c>CHAR</c> goes to the same place rather than to a <c>char(max)</c>, there being no such
            /// thing in T-SQL and nothing for a fixed length with no length to pad to; it is reachable
            /// because a type system may leave <c>CHAR</c>'s precision unspecified, which
            /// <c>MssqlSqlDialect.MSSQL_TYPE_SYSTEM</c> is itself one that does — CALCITE-6565 made bare
            /// <c>CHAR</c> the intended rendering, and thirty is what the server reads it as.
            /// <c>VARBINARY</c> and <c>BINARY</c> are the same rule over bytes.
            /// </para>
            /// <para>
            /// <see cref="SqlAlienSystemTypeNameSpec"/> is how a dialect states a type name of the product
            /// rather than of Calcite — Postgres writes <c>double precision</c> through it — and it unparses
            /// the alias alone, which is what puts the <c>(MAX)</c> where a precision would otherwise go.
            /// </para>
            /// <para>
            /// <c>UUID</c> is the other name Calcite writes that T-SQL has never heard: the server answers
            /// "Type UUID is not a defined system type" and the statement never runs. <c>uniqueidentifier</c>
            /// is what SQL Server calls the same sixteen bytes. A schema reaches this by stating GUID
            /// semantics for a key its source spells as text — which is what a view over a document store
            /// does — and then every comparison against that key is a cast.
            /// </para>
            /// </remarks>
            public override SqlNode getCastSpec(RelDataType type)
            {
                if (UnboundedTypeName(type) is string unbounded)
                    return AlienSpec(unbounded, type);

                if (type.getSqlTypeName()?.name() == nameof(SqlTypeName.UUID))
                    return AlienSpec("UNIQUEIDENTIFIER", type);

                return base.getCastSpec(type);
            }

            /// <summary>
            /// Writes a cast to a type named as SQL Server names it rather than as Calcite does.
            /// </summary>
            /// <param name="typeAlias"></param>
            /// <param name="type"></param>
            /// <returns></returns>
            static SqlDataTypeSpec AlienSpec(string typeAlias, RelDataType type)
            {
                return new SqlDataTypeSpec(
                    new SqlAlienSystemTypeNameSpec(typeAlias, type.getSqlTypeName(), SqlParserPos.ZERO),
                    SqlParserPos.ZERO);
            }

            /// <summary>
            /// Returns the T-SQL type an unbounded <paramref name="type"/> has to be written as, or
            /// <see langword="null"/> where Calcite's own answer stands.
            /// </summary>
            /// <param name="type"></param>
            /// <returns></returns>
            /// <remarks>
            /// The <c>AbstractSqlType</c> test is <c>SqlDialect.getCastSpec</c>'s own: it is the branch that
            /// reads a precision at all, and anything else goes to <c>SqlTypeUtil.convertTypeToSpec</c>
            /// whole.
            /// </remarks>
            static string? UnboundedTypeName(RelDataType type)
            {
                if (type is not AbstractSqlType)
                    return null;

                if (type.getSqlTypeName() is not SqlTypeName typeName)
                    return null;

                var typeAlias = typeName.name() switch
                {
                    nameof(SqlTypeName.CHAR) or nameof(SqlTypeName.VARCHAR) => "VARCHAR(MAX)",
                    nameof(SqlTypeName.BINARY) or nameof(SqlTypeName.VARBINARY) => "VARBINARY(MAX)",
                    _ => null,
                };

                if (typeAlias is null)
                    return null;

                return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED ? typeAlias : null;
            }

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
