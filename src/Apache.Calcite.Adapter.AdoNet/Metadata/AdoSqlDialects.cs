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
        /// <see cref="MssqlSqlDialect"/>, and the three things it does not say about SQL Server.
        /// </summary>
        /// <param name="context"></param>
        /// <remarks>
        /// <para>
        /// Each is a correction to Calcite rather than a reproduction of it, which the adapter is entitled
        /// to make: a dialect generates SQL for a server to run, and the server is the authority on what it
        /// accepts.
        /// </para>
        /// <para>
        /// The first is what an unbounded string casts to — see <see cref="Mssql.getCastSpec"/>. The second
        /// is in <see cref="Mssql.unparseCall"/>: the modulo Calcite already writes for T-SQL is grouped
        /// wrongly. The third is the row count of a <c>TOP</c>, an <c>OFFSET</c> or a <c>FETCH</c>, which
        /// SQL Server takes only as an integer — see <see cref="Mssql.unparseTopN"/>.
        /// </para>
        /// <para>
        /// Two others were here and are not. T-SQL has no concatenation operator and Calcite wrote
        /// <c>||</c> anyway, so every statement that concatenated reached the server as <c>[A] || [B]</c>
        /// and answered "Incorrect syntax near '|'"; and SQL Server cannot group by a constant, which cost
        /// every correlated sub-query, <c>EXISTS</c> becoming an aggregate over a constant true that
        /// <c>SqlImplementor.visitRoot</c> only rewrites when the dialect has said
        /// <c>supportsGroupByLiteral</c> is false. <see cref="MssqlSqlDialect"/> says both itself now, and
        /// each override went when it did. <c>AdoSqlDialectsTests</c> still reads the answers off the
        /// dialect, which is what says they are still there.
        /// </para>
        /// </remarks>
        sealed class Mssql(SqlDialect.Context context) : MssqlSqlDialect(context)
        {

            /// <inheritdoc />
            /// <remarks>
            /// <para>
            /// <b>The modulo Calcite writes for T-SQL is grouped wrongly, and this is where that is put
            /// right.</b> CALCITE-6726 put the substitution in <see cref="MssqlSqlDialect"/>'s own
            /// <c>unparseCall</c>: <see cref="SqlSyntax"/>'s <c>BINARY</c> unparses the call under
            /// <c>PERCENT_REMAINDER</c>. What that shape does not carry across is precedence. By the time a
            /// dialect is asked, <c>SqlCall.unparse</c> has already decided the parentheses around the call
            /// from the call's <em>own</em> operator, so the substitution writes the new operator inside the
            /// old one's parenthesisation, and where the two differ the grouping is the server's to get
            /// wrong. <see cref="UnparseAsBinary"/> is where that is put right.
            /// </para>
            /// <para>
            /// The gap is reachable here: <c>MOD</c> is a function and carries a function's precedence of
            /// 100, <c>PERCENT_REMAINDER</c> is 60, and the operands are numeric, so every context is a
            /// valid one. Measured on <c>MssqlSqlDialect.DEFAULT</c>, with the call as the right operand,
            /// <c>n / MOD(a, b)</c> is written <c>n / a % b</c>, <c>n * MOD(a, b)</c> is written
            /// <c>n * a % b</c>, and <c>MOD(n, MOD(a, b))</c> is written <c>n % a % b</c> — each grouped by
            /// the server from the left, so over 12, 7 and 4 they answer 1, 0 and 1 where the expressions
            /// mean 4, 36 and 0.
            /// </para>
            /// <para>
            /// Nothing else changes. As the left operand the rendering was already right, left associativity
            /// giving what the nesting meant, and so was <c>n - MOD(a, b)</c>, <c>%</c> binding tighter than
            /// <c>-</c>. The one place a parenthesis appears that Calcite would not have written is under a
            /// prefix operator, which hands its operand a left precedence of 80: <c>-MOD(a, b)</c> becomes
            /// <c>-(a % b)</c> where Calcite writes <c>-a % b</c>. Calcite is not wrong there — SQL Server's
            /// <c>%</c> takes the sign of its dividend, so the two agree, measured — but the rule does not
            /// know that and does not need to.
            /// </para>
            /// <para>
            /// Calcite does not close the gap. That is a defect to raise upstream rather than one to
            /// reproduce: a dialect exists to generate SQL a server will run, and a misgrouped expression is
            /// not that.
            /// </para>
            /// <para>
            /// Concatenation was the other half of this method and is not any more. Calcite wrote <c>||</c>
            /// for T-SQL, which the server refuses, so the adapter substituted the <c>+</c> and carried the
            /// parenthesisation across the same way. A 1.43 snapshot made the substitution upstream. It does
            /// not carry the grouping — <c>(a || b) * n</c> comes out <c>a + b * n</c> — and that is left
            /// alone, because reaching it takes a string as an operand of <c>*</c>, which does not validate.
            /// </para>
            /// </remarks>
            public override void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec)
            {
                // the interception is Calcite's own and the operator is the one it chooses; what is taken
                // over is where the parentheses go, which is why this is a case here rather than a call to
                // base with something rearranged
                if (call.getKind().name() == nameof(SqlKind.MOD))
                {
                    UnparseAsBinary(writer, SqlStdOperatorTable.PERCENT_REMAINDER, call, leftPrec, rightPrec);
                    return;
                }

                base.unparseCall(writer, call, leftPrec, rightPrec);
            }

            /// <summary>
            /// Writes a call under another operator, parenthesised as that operator would have been.
            /// </summary>
            /// <param name="writer"></param>
            /// <param name="op"></param>
            /// <param name="call"></param>
            /// <param name="leftPrec"></param>
            /// <param name="rightPrec"></param>
            /// <remarks>
            /// The two precedences the caller already spent on the call's own operator are spent again on
            /// the one being written, which is <c>SqlCall.needsParentheses</c>'s test — its two clauses that
            /// read a precedence, the third being the writer's own setting, already consulted, and the
            /// fourth a comparison, which none of these operators is. Where the call was parenthesised on
            /// the way in, both are zero and nothing more is written; where it was not, and the new
            /// operator binds too loosely for where it stands, the parentheses go on here.
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
            /// <b>SQL Server takes a row count only as an integer</b>, and since CALCITE-7624 a row count is
            /// neither. <see cref="AsRowCount"/> is where that is put right, for the <c>TOP</c> this writes
            /// and for the <c>OFFSET</c> and <c>FETCH</c> of <see cref="unparseOffsetFetch"/> alike.
            /// </remarks>
            public override void unparseTopN(SqlWriter writer, SqlNode offset, SqlNode fetch)
            {
                // the offset is read for its nullness alone here, so it goes down untouched: MssqlSqlDialect
                // writes TOP where there is no offset, and under version 11 writes it anyway and discards
                // the offset, which is the whole reason this dialect is built from a version
                base.unparseTopN(writer, offset, AsRowCount(fetch, Fetch));
            }

            /// <inheritdoc cref="unparseTopN"/>
            public override void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch)
            {
                base.unparseOffsetFetch(writer, AsRowCount(offset, Offset), AsRowCount(fetch, Fetch));
            }

            /// <summary>
            /// Names the two clauses in a refusal, as <c>EnumUtils.numberToBigDecimal</c> names them in the
            /// one the operator in process raises over the same value.
            /// </summary>
            const string Fetch = "FETCH";

            /// <inheritdoc cref="Fetch"/>
            const string Offset = "OFFSET";

            /// <summary>
            /// <c>INT</c>, named as SQL Server names it.
            /// </summary>
            static readonly SqlDataTypeSpec Integer = new(
                new SqlAlienSystemTypeNameSpec("INT", SqlTypeName.INTEGER, SqlParserPos.ZERO),
                SqlParserPos.ZERO);

            /// <summary>
            /// Returns the row count to write for a <c>TOP</c>, an <c>OFFSET</c> or a <c>FETCH</c>.
            /// </summary>
            /// <param name="node">The count Calcite produced, or <see langword="null"/> where there is none.</param>
            /// <param name="kind"><see cref="Fetch"/> or <see cref="Offset"/>, for the refusal.</param>
            /// <returns></returns>
            /// <exception cref="AdoCalciteException">Where a literal count has no <c>int</c>.</exception>
            /// <remarks>
            /// <para>
            /// CALCITE-7624 widened both counts to a <c>BigDecimal</c>: a literal may have a fractional part,
            /// and <c>SqlValidatorImpl.handleOffsetFetch</c> types a placeholder in either slot
            /// <c>DECIMAL</c>, so what a driver binds for one is a decimal too. SQL Server refuses both —
            /// "The number of rows provided for a TOP or FETCH clauses row count parameter must be an
            /// integer" — and measured, it takes a <c>tinyint</c>, a <c>smallint</c>, an <c>int</c> and a
            /// <c>bigint</c> there and refuses a <c>decimal</c> and a string.
            /// </para>
            /// <para>
            /// A literal is therefore written as the whole number of rows it stands for, which is its
            /// ceiling: that is how many rows <c>EnumerableDefaults.take</c> and <c>skip</c> return for the
            /// same <c>BigDecimal</c>, each counting while the zero-based index is below it, and
            /// <c>RexUtil.makeOffsetFetchSum</c> says the same — "Enumerable execution rounds OFFSET and
            /// FETCH independently to whole row counts".
            /// </para>
            /// <para>
            /// Anything else is a value this has never seen, a parameter above all, so the server is asked
            /// to take the same two steps on it. Measured, and each of the three refusals the value can earn
            /// is the one the operator in process raises over it: 2.9 fetches three rows, 3000000000 is
            /// "Arithmetic overflow error converting expression to data type int", and a negative is "A TOP
            /// N or FETCH rowcount value may not be negative". <c>CEILING</c> rather than <c>CEIL</c>
            /// because T-SQL has only the one spelling, which <see cref="MssqlSqlDialect"/>'s own
            /// <c>unparseCall</c> already knows.
            /// </para>
            /// <para>
            /// The one thing this cannot see is a <c>FetchOffsetRoundingPolicy</c> a caller put on the
            /// planner's context: a dialect is handed a node and a writer, and the policy is nowhere in
            /// either. So a caller who rounds fractional counts its own way gets its rounding in process and
            /// the ceiling here. Nothing rounds a count that is already whole, which is every count a caller
            /// that is not asking for this writes.
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
            /// Returns the literal to write for a count known here.
            /// </summary>
            /// <param name="value"></param>
            /// <param name="kind"></param>
            /// <param name="pos"></param>
            /// <returns></returns>
            /// <exception cref="AdoCalciteException"></exception>
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
            /// A Calcite <c>VARCHAR</c> with no precision is unbounded, and <c>SqlDialect.getCastSpec</c>
            /// writes it as the bare keyword, its precision being the type system's
            /// <c>PRECISION_NOT_SPECIFIED</c>. <c>MssqlSqlDialect</c> writes <c>VARCHAR(MAX)</c> for that
            /// one type itself from the 1.43 snapshots on (CALCITE-7756); the other three below are still
            /// written bare. A bare <c>varchar</c> in T-SQL is not unbounded: it is one
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
