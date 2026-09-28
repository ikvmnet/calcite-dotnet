using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Chooses the <see cref="SqlDialect"/> for the database behind an ODBC or OLE DB connection, and builds the
    /// adapter's SQL Server dialect.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The only description ODBC and OLE DB give is the <c>DataSourceInformation</c> collection, whose
    /// <c>DataSourceProductName</c> is the string JDBC's <c>getDatabaseProductName</c> returns. The name is matched
    /// with the tests of Calcite's <c>SqlDialectFactoryImpl.create</c>, and the answer is that product's dialect.
    /// </para>
    /// <para>
    /// <see cref="SqlDialect.DatabaseProduct.getDialect"/> carries no version, which is enough for every product
    /// but SQL Server: below major version 11 <see cref="MssqlSqlDialect"/> writes <c>TOP(n)</c> and drops the
    /// offset. SQL Server's dialect is therefore built from the reported version.
    /// </para>
    /// </remarks>
    static class AdoSqlDialects
    {

        /// <summary>
        /// Reads a connection's product name and version and returns the dialect for them.
        /// </summary>
        /// <param name="connection">An open connection.</param>
        /// <returns>The dialect; the ANSI dialect where the product is not recognised or not reported.</returns>
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
                // a driver need not offer the collection; an unknown product gets the ANSI dialect
            }

            if (string.IsNullOrWhiteSpace(productVersion))
                productVersion = TryGetServerVersion(connection);

            return For(productName, productVersion);
        }

        /// <summary>
        /// Reads <see cref="DbConnection.ServerVersion"/>, which a driver may refuse.
        /// </summary>
        /// <param name="connection">An open connection.</param>
        /// <returns>The version, or <see langword="null"/> where the driver throws.</returns>
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
        /// Returns the dialect for a product name and version.
        /// </summary>
        /// <param name="productName">The product name, or <see langword="null"/>.</param>
        /// <param name="productVersion">The version, or <see langword="null"/>. Only SQL Server's dialect uses it.</param>
        /// <returns>The dialect.</returns>
        public static SqlDialect For(string? productName, string? productVersion)
        {
            var product = ProductFor(productName);

            if (product == SqlDialect.DatabaseProduct.MSSQL)
                return CreateMssql(productName, productVersion);

            // SqlDialectFactoryImpl.create ends at the ANSI dialect, not UNKNOWN's own bare SqlDialect
            if (product == SqlDialect.DatabaseProduct.UNKNOWN)
                return AnsiSqlDialect.DEFAULT;

            return product.getDialect();
        }

        /// <summary>
        /// Builds the adapter's SQL Server dialect for a reported version.
        /// </summary>
        /// <param name="productName">The product name, or <see langword="null"/> for <c>Microsoft SQL Server</c>.</param>
        /// <param name="productVersion">The dotted version, or <see langword="null"/>, which is read as version 0.</param>
        /// <returns>The dialect.</returns>
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
        /// <see cref="MssqlSqlDialect"/> with three corrections for what SQL Server accepts.
        /// </summary>
        /// <param name="context">The dialect's context.</param>
        /// <remarks>
        /// <para>
        /// These correct Calcite rather than reproduce it, because a dialect's output is judged by the server that
        /// runs it. <see cref="getCastSpec"/> writes an unbounded string or binary type as <c>(MAX)</c> and
        /// <c>UUID</c> as <c>UNIQUEIDENTIFIER</c>; <see cref="unparseCall"/> parenthesises <c>MOD</c> for the
        /// <c>%</c> it is written as; and <see cref="unparseTopN"/> and <see cref="unparseOffsetFetch"/> write row
        /// counts as integers.
        /// </para>
        /// <para>
        /// <see cref="MssqlSqlDialect"/> itself writes <c>+</c> for concatenation and reports
        /// <c>supportsGroupByLiteral</c> as false, so neither needs correcting here.
        /// </para>
        /// </remarks>
        sealed class Mssql(SqlDialect.Context context) : MssqlSqlDialect(context)
        {

            /// <inheritdoc />
            /// <remarks>
            /// <para>
            /// <see cref="MssqlSqlDialect"/> writes <c>MOD(a, b)</c> as <c>a % b</c>, but by the time the dialect is
            /// asked, <c>SqlCall.unparse</c> has decided the parentheses from <c>MOD</c>'s own precedence (a
            /// function's, 100) rather than <c>%</c>'s (60). As the right operand of <c>/</c>, <c>*</c> or
            /// <c>%</c> the result is misgrouped: <c>n / MOD(a, b)</c> becomes <c>n / a % b</c>, which the server
            /// evaluates as <c>(n / a) % b</c>. This writes <c>MOD</c> with the parentheses <c>%</c> needs
            /// (<see cref="UnparseAsBinary"/>). Calcite has the same defect; it is corrected here rather than
            /// reproduced because the SQL is wrong.
            /// </para>
            /// <para>
            /// Under a prefix operator this adds parentheses Calcite would not (<c>-(a % b)</c> for <c>-a % b</c>);
            /// SQL Server's <c>%</c> takes the sign of its dividend, so the two agree.
            /// </para>
            /// <para>
            /// Calcite's substitution of <c>+</c> for <c>||</c> has the same grouping problem
            /// (<c>(a || b) * n</c> becomes <c>a + b * n</c>). It is not corrected, because a string operand of
            /// <c>*</c> does not validate.
            /// </para>
            /// </remarks>
            public override void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
            {
                // the operator is the one Calcite's MssqlSqlDialect substitutes; only the parentheses differ
                if (call.getKind().name() == nameof(SqlKind.MOD))
                {
                    UnparseAsBinary(writer, SqlStdOperatorTable.PERCENT_REMAINDER, call, leftPrec, rightPrec);
                    return;
                }

                base.unparseCall(writer, call, leftPrec, rightPrec);
            }

            /// <summary>
            /// Writes a call's operands under another binary operator, parenthesised as that operator needs.
            /// </summary>
            /// <param name="writer">The writer.</param>
            /// <param name="op">The operator to write.</param>
            /// <param name="call">The call whose operands are written.</param>
            /// <param name="leftPrec">The precedence of the operator to the left.</param>
            /// <param name="rightPrec">The precedence of the operator to the right.</param>
            /// <remarks>
            /// The test is the two precedence clauses of <c>SqlCall.needsParentheses</c>, applied to
            /// <paramref name="op"/>. Where the caller has already parenthesised the call, both precedences are
            /// zero and nothing more is added.
            /// </remarks>
            static void UnparseAsBinary(SqlWriter writer, SqlOperator op, SqlCall call, int leftPrec, int rightPrec)
            {
                if (leftPrec > op.getLeftPrec() || (op.getRightPrec() <= rightPrec && rightPrec != 0))
                {
                    var frame = writer.startList("(", ")");
                    SqlSyntax.BINARY.unparse(writer, op, call, 0, 0);
                    writer.endList(frame);
                }
                else
                {
                    SqlSyntax.BINARY.unparse(writer, op, call, leftPrec, rightPrec);
                }
            }

            /// <inheritdoc />
            /// <remarks>
            /// SQL Server accepts a <c>TOP</c>, <c>OFFSET</c> or <c>FETCH</c> row count only as an integer, and
            /// Calcite's row counts are decimals. <see cref="AsRowCount"/> writes the count as an integer.
            /// </remarks>
            public override void unparseTopN(SqlWriter writer, SqlNode offset, SqlNode fetch)
            {
                // MssqlSqlDialect reads the offset only to test it for null, so it is passed unchanged
                base.unparseTopN(writer, offset, AsRowCount(fetch, Fetch));
            }

            /// <inheritdoc cref="unparseTopN"/>
            public override void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch)
            {
                base.unparseOffsetFetch(writer, AsRowCount(offset, Offset), AsRowCount(fetch, Fetch));
            }

            /// <summary>
            /// The clause's name in an error message, as <c>EnumUtils.numberToBigDecimal</c> names it.
            /// </summary>
            const string Fetch = "FETCH";

            /// <inheritdoc cref="Fetch"/>
            const string Offset = "OFFSET";

            /// <summary>
            /// The type <c>INT</c>, spelled as SQL Server spells it.
            /// </summary>
            static readonly SqlDataTypeSpec Integer = new(
                new SqlAlienSystemTypeNameSpec("INT", SqlTypeName.INTEGER, SqlParserPos.ZERO),
                SqlParserPos.ZERO);

            /// <summary>
            /// Returns the row count to write for a <c>TOP</c>, an <c>OFFSET</c> or a <c>FETCH</c>.
            /// </summary>
            /// <param name="node">The count Calcite produced, or <see langword="null"/> where there is none.</param>
            /// <param name="kind"><see cref="Fetch"/> or <see cref="Offset"/>, for the error message.</param>
            /// <returns>An integer literal, a <c>CAST(CEILING(…) AS INT)</c> of any other expression, or
            /// <see langword="null"/>.</returns>
            /// <exception cref="AdoCalciteException">A literal count's ceiling does not fit in an <c>int</c>.</exception>
            /// <remarks>
            /// <para>
            /// Calcite types offset and fetch counts as <c>DECIMAL</c>, literals and parameters alike, and SQL Server
            /// rejects a decimal row count. A literal is written as its ceiling, the number of rows
            /// <c>EnumerableDefaults.take</c> and <c>skip</c> count for the same value. Any other expression, such as
            /// a parameter, is wrapped so that the server takes the ceiling and converts it to <c>int</c>; T-SQL
            /// spells the function <c>CEILING</c>, and <see cref="MssqlSqlDialect"/> writes <c>CEIL</c> that way.
            /// </para>
            /// <para>
            /// The dialect cannot see a <c>FetchOffsetRoundingPolicy</c> on the planner's context, so a policy
            /// that rounds fractional counts differently applies in process but not here. A whole count is
            /// unaffected.
            /// </para>
            /// </remarks>
            static SqlNode? AsRowCount(SqlNode? node, string kind)
            {
                if (node is null)
                    return null;

                if (node is SqlNumericLiteral literal && literal.bigDecimalValue() is java.math.BigDecimal value)
                    return WholeRows(value, kind, literal.getParserPosition());

                return SqlStdOperatorTable.CAST.createCall(
                    SqlParserPos.ZERO,
                    SqlStdOperatorTable.CEIL.createCall(SqlParserPos.ZERO, node),
                    Integer);
            }

            /// <summary>
            /// Returns the ceiling of a literal count as an exact numeric literal.
            /// </summary>
            /// <param name="value">The count.</param>
            /// <param name="kind"><see cref="Fetch"/> or <see cref="Offset"/>, for the error message.</param>
            /// <param name="pos">The literal's position.</param>
            /// <returns>The literal.</returns>
            /// <exception cref="AdoCalciteException">The ceiling does not fit in an <c>int</c>.</exception>
            static SqlNode WholeRows(java.math.BigDecimal value, string kind, SqlParserPos pos)
            {
                var rows = value.setScale(0, java.math.RoundingMode.CEILING);

                try
                {
                    rows.intValueExact();
                }
                catch (java.lang.ArithmeticException e)
                {
                    throw new AdoCalciteException($"A {kind} row count reaches SQL Server as an int, and {value} has none.", e);
                }

                return SqlLiteral.createExactNumeric(rows.toString(), pos);
            }

            /// <inheritdoc />
            /// <remarks>
            /// <para>
            /// A <c>CHAR</c>, <c>VARCHAR</c>, <c>BINARY</c> or <c>VARBINARY</c> with no precision is unbounded in
            /// Calcite, and would otherwise be written as the bare type name. In a T-SQL <c>CAST</c> a bare
            /// <c>varchar</c> means thirty characters, so a long value would be truncated, or the cast would fail
            /// (a GUID is thirty-six). The character types are written <c>VARCHAR(MAX)</c> and the binary types
            /// <c>VARBINARY(MAX)</c>; there is no <c>char(max)</c>.
            /// </para>
            /// <para>
            /// <c>UUID</c> is written <c>UNIQUEIDENTIFIER</c>, SQL Server's name for the type; it has no type named
            /// <c>UUID</c>.
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
            /// Returns a cast target spelled as SQL Server spells it. <see cref="SqlAlienSystemTypeNameSpec"/>
            /// unparses the alias alone, so <c>(MAX)</c> can be part of it.
            /// </summary>
            /// <param name="typeAlias">The type as SQL Server spells it.</param>
            /// <param name="type">The Calcite type.</param>
            /// <returns>The type spec.</returns>
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
            /// <param name="type">The type.</param>
            /// <returns>The type as SQL Server spells it, or <see langword="null"/>.</returns>
            /// <remarks>
            /// Only an <c>AbstractSqlType</c> is considered, as in <c>SqlDialect.getCastSpec</c>, which writes a
            /// precision only for those.
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
        /// Matches a product name to a product: the exact names first, then the substring tests, in the order
        /// <c>SqlDialectFactoryImpl.create</c> applies them.
        /// </summary>
        /// <param name="productName">The product name, or <see langword="null"/>.</param>
        /// <returns>The product, or <see cref="SqlDialect.DatabaseProduct.UNKNOWN"/>.</returns>
        /// <remarks>
        /// A port of <c>create</c>'s matching, since <c>create</c> takes a JDBC <c>DatabaseMetaData</c>. Where Calcite
        /// returns a dialect this returns the product whose <c>getDialect</c> is that dialect.
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

            // in Calcite's order: where two tests match, the earlier wins
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

            // not in Calcite's create, which has no SQLite test
            if (name.Contains("SQLITE", StringComparison.Ordinal))
                return SqlDialect.DatabaseProduct.SQLITE;

            return SqlDialect.DatabaseProduct.UNKNOWN;
        }

        /// <summary>
        /// Returns the leading component of a dotted version string, or zero.
        /// </summary>
        /// <param name="version">The version, or <see langword="null"/>.</param>
        /// <returns>The component, or zero where it is missing or not a number.</returns>
        public static int MajorVersion(string? version)
        {
            return Component(version, 0);
        }

        /// <summary>
        /// Returns the second component of a dotted version string, or zero.
        /// </summary>
        /// <param name="version">The version, or <see langword="null"/>.</param>
        /// <returns>The component, or zero where it is missing or not a number.</returns>
        public static int MinorVersion(string? version)
        {
            return Component(version, 1);
        }

        /// <summary>
        /// Returns one component of a dotted version string.
        /// </summary>
        /// <param name="version">The version, or <see langword="null"/>.</param>
        /// <param name="index">The zero-based component.</param>
        /// <returns>The component, or zero where it is missing or not a number.</returns>
        static int Component(string? version, int index)
        {
            if (version is null)
                return 0;

            var parts = version.Split('.');
            return parts.Length > index && int.TryParse(parts[index], out var value) ? value : 0;
        }

    }

}
