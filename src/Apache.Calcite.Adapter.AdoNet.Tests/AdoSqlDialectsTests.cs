using System.Text.RegularExpressions;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using FluentAssertions;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;
using org.apache.calcite.sql.type;

using Xunit;
using Xunit.Sdk;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests choosing a dialect from the product name and version a driver reports, and the SQL Server
    /// dialect's corrections to Calcite's output.
    /// </summary>
    /// <remarks>
    /// Needs no database, so it runs on every platform; the ODBC and OLE DB suites that reach the same code
    /// end to end need Windows and LocalDB.
    /// </remarks>
    public class AdoSqlDialectsTests
    {

        /// <summary>
        /// Returns what a dialect writes for an <c>OFFSET</c> / <c>FETCH</c> pair.
        /// </summary>
        /// <param name="dialect">The dialect under test.</param>
        /// <returns>The SQL the dialect writes for an offset of 1 and a fetch of 2.</returns>
        static string OffsetFetch(SqlDialect dialect)
        {
            var writer = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(dialect));
            dialect.unparseOffsetFetch(
                writer,
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO));

            return writer.toSqlString().getSql();
        }

        /// <summary>
        /// Returns what a dialect writes for the target type of a cast.
        /// </summary>
        /// <param name="dialect">The dialect under test.</param>
        /// <param name="type">The type being cast to.</param>
        /// <returns>The dialect's spelling of that type, trimmed.</returns>
        static string CastSpec(SqlDialect dialect, RelDataType type)
        {
            var writer = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(dialect));
            dialect.getCastSpec(type).unparse(writer, 0, 0);

            return writer.toSqlString().getSql().Trim();
        }

        /// <summary>
        /// Returns what a dialect writes for a statement, by parsing it and unparsing it again.
        /// </summary>
        /// <remarks>
        /// The adapter goes through <c>RelToSqlConverter</c> instead, but both routes end at
        /// <c>SqlNode.unparse</c>, where an operator is written, so this needs no schema.
        /// </remarks>
        /// <param name="dialect">The dialect to write the statement in.</param>
        /// <param name="sql">A query in Calcite's default SQL syntax.</param>
        /// <returns>The query as the dialect writes it, with whitespace collapsed.</returns>
        static string Unparse(SqlDialect dialect, string sql)
        {
            return Unparse(dialect, SqlParser.create(sql).parseQuery());
        }

        /// <summary>
        /// Returns what a dialect writes for a node already built, with whitespace collapsed. Building the node
        /// reaches an operator the parser would leave unresolved.
        /// </summary>
        /// <param name="dialect">The dialect to write the node in.</param>
        /// <param name="node">The parse tree to write.</param>
        /// <returns>The SQL text, with each run of whitespace reduced to one space and the ends trimmed.</returns>
        static string Unparse(SqlDialect dialect, SqlNode node)
        {
            return Regex.Replace(node.toSqlString(dialect).getSql(), @"\s+", " ").Trim();
        }

        /// <summary>
        /// A column reference, for building a call the parser will not produce.
        /// </summary>
        /// <param name="name">The column name, used as a single-part identifier.</param>
        /// <returns>An unresolved identifier with no parser position.</returns>
        static SqlNode Column(string name)
        {
            return new SqlIdentifier(name, SqlParserPos.ZERO);
        }

        /// <summary>
        /// A call to an operator over the operands given.
        /// </summary>
        /// <param name="op">The operator to call.</param>
        /// <param name="operands">The call's operands, in order.</param>
        /// <returns>The call node, with no parser position.</returns>
        static SqlNode Call(SqlOperator op, params SqlNode[] operands)
        {
            return op.createCall(SqlParserPos.ZERO, operands);
        }

        /// <summary>
        /// <c>MOD(A, B)</c>, the call the modulo tests are written around.
        /// </summary>
        /// <returns>A new call to <c>MOD</c> over the columns <c>A</c> and <c>B</c>.</returns>
        static SqlNode Modulo()
        {
            return Call(SqlStdOperatorTable.MOD, Column("A"), Column("B"));
        }

        /// <summary>
        /// Builds types the way a connection does, from the default type system.
        /// </summary>
        static readonly RelDataTypeFactory Types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        /// <summary>
        /// Builds types from the type system <see cref="MssqlSqlDialect"/> carries, which leaves a <c>CHAR</c>
        /// with no precision.
        /// </summary>
        static readonly RelDataTypeFactory MssqlTypes = new SqlTypeFactoryImpl(MssqlSqlDialect.MSSQL_TYPE_SYSTEM);

        #region Product

        /// <remarks>
        /// SQL Server is absent because its dialect is a subclass of Calcite's rather than Calcite's own type;
        /// see <see cref="TheCorrectedDialectIsStillTheSqlServerOne"/>.
        /// </remarks>
        /// <param name="productName">The product name as a driver reports it.</param>
        /// <param name="expected">The simple class name of the dialect Calcite chooses.</param>
        [Theory]
        [InlineData("PostgreSQL", "PostgresqlSqlDialect")]
        [InlineData("Oracle", "OracleSqlDialect")]
        [InlineData("MySQL", "MysqlSqlDialect")]
        [InlineData("Apache Derby", "DerbySqlDialect")]
        [InlineData("ACCESS", "AccessSqlDialect")]
        // the DB2 driver reports its platform, and Calcite matches the prefix rather than the word
        [InlineData("DB2/LINUXX8664", "Db2SqlDialect")]
        [InlineData("Teradata Database", "TeradataSqlDialect")]
        [InlineData("SQLite", "SqliteSqlDialect")]
        public void AProductNameSelectsItsDialect(string productName, string expected)
        {
            Assert.Equal(expected, AdoSqlDialects.For(productName, "1.0").GetType().Name);
        }

        /// <summary>
        /// The name is matched case-insensitively, after trimming, and by containment, as Calcite matches it.
        /// </summary>
        /// <param name="productName">A variation on SQL Server's product name.</param>
        [Theory]
        [InlineData("microsoft sql server")]
        [InlineData("  Microsoft SQL Server  ")]
        [InlineData("Microsoft SQL Server Enterprise Edition")]
        public void TheProductNameIsMatchedLoosely(string productName)
        {
            Assert.IsAssignableFrom<MssqlSqlDialect>(AdoSqlDialects.For(productName, "15.0"));
        }

        /// <summary>
        /// An unknown or missing product name gets the ANSI dialect, as Calcite's own factory does.
        /// </summary>
        [Fact]
        public void AnUnknownProductGetsTheGenericDialect()
        {
            Assert.Equal("AnsiSqlDialect", AdoSqlDialects.For(null, null).GetType().Name);
            Assert.Equal("AnsiSqlDialect", AdoSqlDialects.For("Some Database Nobody Has Heard Of", "1.2.3").GetType().Name);
        }

        [Fact]
        public void AnUnknownProductIsTheUnknownProduct()
        {
            Assert.Equal(
                SqlDialect.DatabaseProduct.UNKNOWN,
                AdoSqlDialects.ProductFor("Some Database Nobody Has Heard Of"));
        }

        #endregion

        #region Version

        [Theory]
        [InlineData("15.00.4382", 15, 0)]
        [InlineData("10.50.1600.1", 10, 50)]
        [InlineData("9", 9, 0)]
        [InlineData("", 0, 0)]
        [InlineData(null, 0, 0)]
        public void AVersionIsSplitIntoItsComponents(string? version, int major, int minor)
        {
            Assert.Equal(major, AdoSqlDialects.MajorVersion(version));
            Assert.Equal(minor, AdoSqlDialects.MinorVersion(version));
        }

        /// <summary>
        /// SQL Server 2012 (version 11) and later get <c>OFFSET</c> / <c>FETCH</c>. Below version 11
        /// <c>MssqlSqlDialect</c> writes <c>TOP(n)</c> and discards the offset, so a dialect built without the
        /// version would return the first page for every page.
        /// </summary>
        [Fact]
        public void SqlServerPastTwentyTwelveGetsOffsetFetch()
        {
            Assert.Contains("OFFSET", OffsetFetch(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382")));
        }

        /// <summary>
        /// Below version 11 the dialect writes no <c>OFFSET</c> / <c>FETCH</c>, as Calcite's does.
        /// </summary>
        [Fact]
        public void SqlServerBeforeTwentyTwelveDoesNot()
        {
            Assert.Equal("", OffsetFetch(AdoSqlDialects.For("Microsoft SQL Server", "10.50.1600")).Trim());
        }

        /// <summary>
        /// A version that cannot be parsed is treated as no version rather than throwing.
        /// </summary>
        [Fact]
        public void AnUnreadableVersionIsNotAnError()
        {
            Assert.IsAssignableFrom<MssqlSqlDialect>(AdoSqlDialects.For("Microsoft SQL Server", "not a version"));
        }

        #endregion

        #region Group by a constant

        /// <summary>
        /// SQL Server cannot group by a constant. A correlated <c>EXISTS</c> becomes an aggregate over a
        /// constant true, and <c>SqlImplementor.visitRoot</c> rewrites that away only when
        /// <c>supportsGroupByLiteral</c> is false; otherwise the server is sent <c>GROUP BY (1 = 1)</c> and
        /// rejects it.
        /// </summary>
        [Fact]
        public void SqlServerSaysItCannotGroupByAConstant()
        {
            Assert.False(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382").supportsGroupByLiteral());
        }

        /// <summary>
        /// The adapter's SQL Server dialect is a <see cref="MssqlSqlDialect"/>.
        /// </summary>
        [Fact]
        public void TheCorrectedDialectIsStillTheSqlServerOne()
        {
            Assert.IsAssignableFrom<MssqlSqlDialect>(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"));
        }

        /// <summary>
        /// Other products keep Calcite's own answer.
        /// </summary>
        [Fact]
        public void AnotherProductKeepsCalcitesOwnAnswer()
        {
            Assert.False(AdoSqlDialects.For("PostgreSQL", "16.0").supportsGroupByLiteral(), "Postgres says so itself");
            Assert.True(AdoSqlDialects.For("MySQL", "8.0").supportsGroupByLiteral(), "MySQL can");
        }

        #endregion

        #region Unbounded strings

        /// <summary>
        /// A Calcite <c>VARCHAR</c> with no precision is unbounded, but SQL Server reads a bare <c>VARCHAR</c>
        /// in a cast as thirty characters: a <c>uniqueidentifier</c> fails to convert, and a longer string is
        /// silently truncated.
        /// </summary>
        [Fact]
        public void AnUnboundedVarcharBecomesVarcharMax()
        {
            Assert.Equal("VARCHAR(MAX)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Types.createSqlType(SqlTypeName.VARCHAR)));
        }

        /// <summary>
        /// Calcite's own dialect gives the same answer for <c>VARCHAR</c>.
        /// </summary>
        [Fact]
        public void CalcitesOwnAnswerIsVarcharMax()
        {
            Assert.Equal("VARCHAR(MAX)", CastSpec(MssqlSqlDialect.DEFAULT, Types.createSqlType(SqlTypeName.VARCHAR)));
        }

        /// <summary>
        /// A stated length is written as it stands.
        /// </summary>
        /// <param name="typeName">The name of the character or binary <see cref="SqlTypeName"/>.</param>
        /// <param name="precision">The stated length.</param>
        /// <param name="expected">The cast target the dialect is expected to write.</param>
        [Theory]
        [InlineData(nameof(SqlTypeName.VARCHAR), 36, "VARCHAR(36)")]
        [InlineData(nameof(SqlTypeName.CHAR), 36, "CHAR(36)")]
        [InlineData(nameof(SqlTypeName.VARBINARY), 16, "VARBINARY(16)")]
        [InlineData(nameof(SqlTypeName.BINARY), 4, "BINARY(4)")]
        public void AStatedLengthIsLeftAlone(string typeName, int precision, string expected)
        {
            var type = Types.createSqlType(SqlTypeName.valueOf(typeName), precision);
            Assert.Equal(expected, CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), type));
        }

        /// <summary>
        /// An unbounded <c>VARBINARY</c> is written as <c>VARBINARY(MAX)</c>, for the same reason.
        /// </summary>
        [Fact]
        public void AnUnboundedVarbinaryBecomesVarbinaryMax()
        {
            Assert.Equal("VARBINARY(MAX)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Types.createSqlType(SqlTypeName.VARBINARY)));
        }

        /// <summary>
        /// A <c>CHAR</c> whose precision the type system leaves unspecified is also written as
        /// <c>VARCHAR(MAX)</c>. Calcite writes the bare <c>CHAR</c>, which SQL Server reads as thirty
        /// characters, and T-SQL has no <c>char(max)</c>.
        /// </summary>
        [Fact]
        public void AnUnboundedCharBecomesVarcharMax()
        {
            var type = MssqlTypes.createSqlType(SqlTypeName.CHAR);
            CastSpec(MssqlSqlDialect.DEFAULT, type).Should().Be("CHAR", "the answer being corrected");
            Assert.Equal("VARCHAR(MAX)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), type));
        }

        /// <summary>
        /// Under the default type system a <c>CHAR</c> has a precision of one, so nothing changes for it.
        /// </summary>
        [Fact]
        public void ACharOfTheDefaultTypeSystemKeepsItsOne()
        {
            Assert.Equal("CHAR(1)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Types.createSqlType(SqlTypeName.CHAR)));
        }

        /// <summary>
        /// Other types keep Calcite's cast spec.
        /// </summary>
        [Fact]
        public void AnotherTypeKeepsCalcitesAnswer()
        {
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");

            Assert.Equal("INTEGER", CastSpec(dialect, Types.createSqlType(SqlTypeName.INTEGER)));
            Assert.Equal("DECIMAL(12, 3)", CastSpec(dialect, Types.createSqlType(SqlTypeName.DECIMAL, 12, 3)));
        }

        /// <summary>
        /// Other products keep the bare keyword: SQLite ignores a length, and PostgreSQL reads a bare
        /// <c>varchar</c> as unbounded. The assertion is only that no length is written, because Calcite
        /// appends a character set for SQLite.
        /// </summary>
        /// <param name="productName">A product other than SQL Server.</param>
        [Theory]
        [InlineData("SQLite")]
        [InlineData("PostgreSQL")]
        public void AnotherProductKeepsTheBareKeyword(string productName)
        {
            var spec = CastSpec(AdoSqlDialects.For(productName, "1.0"), Types.createSqlType(SqlTypeName.VARCHAR));

            Assert.StartsWith("VARCHAR", spec);
            Assert.False(spec.Contains('('), $"a length was written where the bare keyword is right: {spec}");
        }

        /// <summary>
        /// Every product name that selects SQL Server gets the corrected dialect, which is how ODBC and OLE DB
        /// over SQL Server get it.
        /// </summary>
        /// <param name="productName">A product name that selects SQL Server.</param>
        [Theory]
        [InlineData("Microsoft SQL Server")]
        [InlineData("microsoft sql server")]
        [InlineData("Microsoft SQL Server Enterprise Edition")]
        public void AnyNameThatSelectsSqlServerGetsTheCorrection(string productName)
        {
            Assert.Equal("VARCHAR(MAX)", CastSpec(AdoSqlDialects.For(productName, "10.50.1600"), Types.createSqlType(SqlTypeName.VARCHAR)));
        }

        #endregion

        #region Concatenation

        /// <summary>
        /// T-SQL has no <c>||</c>, so concatenation is written as <c>+</c> wherever it appears. Two literals are
        /// not folded away, so they are covered too.
        /// </summary>
        /// <param name="sql">A statement with a concatenation in one position.</param>
        /// <param name="shape">The position, named for the failure message.</param>
        [Theory]
        [InlineData("SELECT A || B FROM CAT WHERE ID = 1", "a projection")]
        [InlineData("SELECT ID FROM CAT WHERE A || B = 'aabb'", "a predicate")]
        [InlineData("SELECT ID FROM CAT ORDER BY A || B", "a sort key")]
        [InlineData("SELECT MAX(A || B) FROM CAT", "an aggregate argument")]
        [InlineData("SELECT A || B FROM CAT GROUP BY A || B", "a group key")]
        [InlineData("SELECT 'x' || 'y' FROM CAT WHERE ID = 1", "two literals")]
        public void ConcatenationIsWrittenAsPlus(string sql, string shape)
        {
            var written = Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql);

            Assert.False(written.Contains("||"), $"{shape} still carries the operator the server refuses: {written}");
            written.Should().Contain("+", $"{shape}: {written}");
        }

        /// <summary>
        /// Calcite's own <c>MssqlSqlDialect</c> writes <c>+</c> for <c>||</c>, so the adapter's dialect does not
        /// override concatenation.
        /// </summary>
        [Fact]
        public void CalciteWritesThePlusItself()
        {
            Assert.Equal(
                "SELECT [A] + [B] FROM [CAT] WHERE [ID] = 1",
                Unparse(MssqlSqlDialect.DEFAULT, "SELECT A || B FROM CAT WHERE ID = 1"));
        }

        /// <summary>
        /// The whole statement for each shape, so the operator is checked in place.
        /// </summary>
        [Theory]
        [InlineData(
            "SELECT A || B FROM CAT WHERE ID = 1",
            "SELECT [A] + [B] FROM [CAT] WHERE [ID] = 1")]
        [InlineData(
            "SELECT ID FROM CAT WHERE A || B = 'aabb'",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] = 'aabb'")]
        [InlineData(
            "SELECT MAX(A || B) FROM CAT",
            "SELECT MAX([A] + [B]) FROM [CAT]")]
        [InlineData(
            "SELECT A || B FROM CAT GROUP BY A || B",
            "SELECT [A] + [B] FROM [CAT] GROUP BY [A] + [B]")]
        public void TheStatementIsWrittenWhole(string sql, string expected)
        {
            Assert.Equal(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql));
        }

        /// <summary>
        /// <c>+</c> and not <c>CONCAT</c>: <c>||</c> yields null when either operand is null, as <c>+</c> does
        /// under the default <c>CONCAT_NULL_YIELDS_NULL</c>, while T-SQL's <c>CONCAT</c> reads a null operand as
        /// the empty string.
        /// </summary>
        [Fact]
        public void TheFunctionIsNotWhatIsWritten()
        {
            var written = Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), "SELECT A || B FROM CAT");

            Assert.False(written.Contains("CONCAT"), $"the function does not propagate null: {written}");
        }

        /// <summary>
        /// A product whose own operator is <c>||</c> keeps it.
        /// </summary>
        /// <param name="productName">A product other than SQL Server, including one no dialect recognises.</param>
        [Theory]
        [InlineData("PostgreSQL")]
        [InlineData("SQLite")]
        [InlineData("Oracle")]
        // an unknown product, which gets the ANSI dialect
        [InlineData("Some Database Nobody Has Heard Of")]
        public void AnotherProductKeepsTheOperator(string productName)
        {
            Assert.Contains("||", Unparse(AdoSqlDialects.For(productName, "1.0"), "SELECT A || B FROM CAT"));
        }

        /// <summary>
        /// ODBC and OLE DB reach SQL Server through this dialect by product name, so every name that selects
        /// SQL Server writes <c>+</c>.
        /// </summary>
        /// <param name="productName">A product name that selects SQL Server.</param>
        [Theory]
        [InlineData("Microsoft SQL Server")]
        [InlineData("microsoft sql server")]
        [InlineData("Microsoft SQL Server Enterprise Edition")]
        public void AnyNameThatSelectsSqlServerGetsTheOperator(string productName)
        {
            Assert.Equal(
                "SELECT [A] + [B] FROM [CAT]",
                Unparse(AdoSqlDialects.For(productName, "10.50.1600"), "SELECT A || B FROM CAT"));
        }

        /// <summary>
        /// The adapter's <c>unparseCall</c> handles <c>MOD</c> and defers everything else to
        /// <c>MssqlSqlDialect.unparseCall</c>, so that method's own rewrites still happen.
        /// </summary>
        /// <param name="sql">A statement with a call <c>MssqlSqlDialect</c> rewrites.</param>
        /// <param name="expected">Text the rewritten statement must contain.</param>
        [Theory]
        [InlineData("SELECT CEIL(SALARY) FROM CAT", "CEILING")]
        [InlineData("SELECT SUBSTRING(A FROM 1 FOR 2) FROM CAT", "SUBSTRING")]
        [InlineData("SELECT CAST(A AS INTEGER) FROM CAT", "CAST")]
        public void TheDialectsOwnInterceptionsStillHappen(string sql, string expected)
        {
            Assert.Contains(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql));
        }


        #endregion

        #region Concatenation and precedence

        /// <summary>
        /// How Calcite's substitution of <c>+</c> for <c>||</c> writes a nested concatenation.
        /// </summary>
        /// <remarks>
        /// <c>||</c> has precedence 60 and <c>+</c> has 40, and <c>SqlCall.unparse</c> decides the parentheses
        /// from the call's own operator before the dialect substitutes, so the grouping is written for
        /// <c>||</c>. The rows cover concatenation inside itself, a comparison, a postfix operator and calls
        /// that write their own parentheses; the last row is the one context where the precedences differ and
        /// the grouping is lost.
        /// </remarks>
        [Theory]
        // concatenation in concatenation, which associates the same either way
        [InlineData(
            "SELECT A || B || C FROM CAT",
            "SELECT [A] + [B] + [C] FROM [CAT]")]
        // the parentheses the caller wrote are dropped; concatenation is associative, so the meaning is kept
        [InlineData(
            "SELECT A || (B || C) FROM CAT",
            "SELECT [A] + [B] + [C] FROM [CAT]")]
        // against a comparison, which binds looser than either spelling
        [InlineData(
            "SELECT ID FROM CAT WHERE A || B = 'aabb'",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] = 'aabb'")]
        [InlineData(
            "SELECT ID FROM CAT WHERE A || B > 'aa' AND ID > 1",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] > 'aa' AND [ID] > 1")]
        // a postfix operator, which is what a sort key's null ordering is written with
        [InlineData(
            "SELECT ID FROM CAT WHERE A || B IS NULL",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] IS NULL")]
        // arithmetic reaches a string only through a cast, and a cast writes its own parentheses
        [InlineData(
            "SELECT A || CAST(ID + 1 AS VARCHAR(4)) FROM CAT",
            "SELECT [A] + CAST([ID] + 1 AS VARCHAR(4)) FROM [CAT]")]
        [InlineData(
            "SELECT CAST(A || B AS VARCHAR(4)) FROM CAT",
            "SELECT CAST([A] + [B] AS VARCHAR(4)) FROM [CAT]")]
        // and inside a function call, whose frame parenthesises whatever it holds
        [InlineData(
            "SELECT UPPER(A || B) FROM CAT",
            "SELECT UPPER([A] + [B]) FROM [CAT]")]
        // an operator binding between the two precedences, where the grouping is lost: this reads as
        // [A] + ([B] * 2). It needs a string as an operand of *, which does not validate, so no plan
        // produces it
        [InlineData(
            "SELECT (A || B) * 2 FROM CAT",
            "SELECT [A] + [B] * 2 FROM [CAT]")]
        public void ANestedConcatenationKeepsItsGrouping(string sql, string expected)
        {
            Assert.Equal(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql));
        }

        /// <summary>
        /// Calcite's substitution loses the grouping of a concatenation under <c>*</c>; the adapter leaves this
        /// as Calcite writes it.
        /// </summary>
        /// <remarks>
        /// The result reads as <c>[A] + ([B] * 2)</c>. Reaching it takes a string as an operand of <c>*</c>,
        /// which does not validate, so no plan produces it; this test constructs it by unparsing text.
        /// </remarks>
        [Fact]
        public void TheGroupingUpstreamDropsIsUnreachable()
        {
            Assert.Equal(
                "SELECT [A] + [B] * 2 FROM [CAT]",
                Unparse(MssqlSqlDialect.DEFAULT, "SELECT (A || B) * 2 FROM CAT"));
        }

        #endregion

        #region Modulo

        /// <summary>
        /// A modulo is written with T-SQL's <c>%</c> operator, as <c>MssqlSqlDialect</c> writes it.
        /// </summary>
        /// <remarks>
        /// The calls in this region are built rather than parsed. An unqualified function name is a
        /// <c>SqlUnresolvedFunction</c> until validation, and the dialect switches on <c>SqlKind.MOD</c>,
        /// which an unresolved call does not carry, so a parsed <c>MOD</c> would be written unchanged.
        /// </remarks>
        [Fact]
        public void ModuloIsWrittenAsThePercentOperator()
        {
            Assert.Equal("[A] % [B]", Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Modulo()));
        }

        /// <summary>
        /// As the right operand of an operator binding at 60, the modulo keeps its parentheses. <c>MOD</c> is a
        /// function with precedence 100 and <c>PERCENT_REMAINDER</c> is 60, so Calcite's substitution loses the
        /// grouping here, and with numeric operands a validated plan can produce every one of these shapes.
        /// </summary>
        /// <param name="operatorName">The outer operator, as <see cref="OperatorNamed"/> takes it.</param>
        /// <param name="expected">The SQL the adapter's dialect is expected to write.</param>
        [Theory]
        [InlineData(nameof(SqlStdOperatorTable.DIVIDE), "[N] / ([A] % [B])")]
        [InlineData(nameof(SqlStdOperatorTable.MULTIPLY), "[N] * ([A] % [B])")]
        [InlineData(nameof(SqlStdOperatorTable.MOD), "[N] % ([A] % [B])")]
        public void AModuloAsARightOperandKeepsItsGrouping(string operatorName, string expected)
        {
            var call = Call(OperatorNamed(operatorName), Column("N"), Modulo());

            Assert.Equal(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), call));
        }

        /// <summary>
        /// Calcite's own rendering of the same calls, which the server groups from the left: over 12, 7 and 4
        /// these give 1, 0 and 1 where the expressions mean 4, 36 and 0.
        /// </summary>
        /// <param name="operatorName">The outer operator, as <see cref="OperatorNamed"/> takes it.</param>
        /// <param name="expected">The SQL Calcite's <c>MssqlSqlDialect</c> writes.</param>
        [Theory]
        [InlineData(nameof(SqlStdOperatorTable.DIVIDE), "[N] / [A] % [B]")]
        [InlineData(nameof(SqlStdOperatorTable.MULTIPLY), "[N] * [A] % [B]")]
        [InlineData(nameof(SqlStdOperatorTable.MOD), "[N] % [A] % [B]")]
        public void CalcitesOwnAnswerLosesTheGrouping(string operatorName, string expected)
        {
            var call = Call(OperatorNamed(operatorName), Column("N"), Modulo());

            Assert.Equal(expected, Unparse(MssqlSqlDialect.DEFAULT, call));
        }

        /// <summary>
        /// As a left operand the rendering is unchanged and matches Calcite's: left associativity gives the
        /// intended nesting, and <c>%</c> binds tighter than <c>-</c> and <c>=</c>.
        /// </summary>
        /// <param name="operatorName">The outer operator, as <see cref="OperatorNamed"/> takes it.</param>
        /// <param name="expected">The SQL both dialects are expected to write.</param>
        [Theory]
        [InlineData(nameof(SqlStdOperatorTable.MULTIPLY), "[A] % [B] * [N]")]
        [InlineData(nameof(SqlStdOperatorTable.DIVIDE), "[A] % [B] / [N]")]
        [InlineData(nameof(SqlStdOperatorTable.MINUS), "[A] % [B] - [N]")]
        [InlineData(nameof(SqlStdOperatorTable.EQUALS), "[A] % [B] = [N]")]
        public void AModuloAsALeftOperandIsUnchanged(string operatorName, string expected)
        {
            var call = Call(OperatorNamed(operatorName), Modulo(), Column("N"));
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");

            Assert.Equal(expected, Unparse(dialect, call));
            Unparse(MssqlSqlDialect.DEFAULT, call).Should().Be(expected, "Calcite already writes this one");
        }

        /// <summary>
        /// As the right operand of <c>-</c> the rendering is unchanged, since <c>%</c> binds tighter.
        /// </summary>
        [Fact]
        public void AModuloUnderALooserOperatorIsUnchanged()
        {
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");
            var minus = Call(SqlStdOperatorTable.MINUS, Column("N"), Modulo());

            Assert.Equal("[N] - [A] % [B]", Unparse(dialect, minus));
            Assert.Equal(Unparse(MssqlSqlDialect.DEFAULT, minus), Unparse(dialect, minus));
        }

        /// <summary>
        /// A prefix operator hands its operand a left precedence of 80, above <c>PERCENT_REMAINDER</c>'s 60, so
        /// the adapter writes parentheses where Calcite writes none.
        /// </summary>
        /// <remarks>
        /// Both renderings compute the same value, because SQL Server's <c>%</c> takes the sign of its dividend
        /// and so <c>(-a) % b</c> equals <c>-(a % b)</c>; the adapter's rule writes the call's grouping without
        /// relying on that.
        /// </remarks>
        [Fact]
        public void AModuloUnderAPrefixOperatorIsParenthesised()
        {
            var negated = Call(SqlStdOperatorTable.UNARY_MINUS, Modulo());

            Assert.Equal("- ([A] % [B])", Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), negated));
            Unparse(MssqlSqlDialect.DEFAULT, negated).Should().Be("- [A] % [B]", "the answer this does not need to correct");
        }

        /// <summary>
        /// A postfix operator, which is what a sort key's null ordering is written with.
        /// </summary>
        [Fact]
        public void AModuloUnderAPostfixOperatorIsUnchanged()
        {
            var call = Call(SqlStdOperatorTable.IS_NULL, Modulo());

            Assert.Equal("[A] % [B] IS NULL", Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), call));
        }

        /// <summary>
        /// <c>PERCENT_REMAINDER</c>, which a query written with <c>%</c> produces under a conformance that allows
        /// it, also has <c>SqlKind.MOD</c>. The adapter then substitutes the operator for itself with the
        /// precedences <c>SqlCall.unparse</c> has already applied, so the output matches Calcite's and no
        /// parenthesis is written twice.
        /// </summary>
        [Fact]
        public void ThePercentOperatorItselfIsUnchanged()
        {
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");
            var percent = Call(SqlStdOperatorTable.PERCENT_REMAINDER, Column("A"), Column("B"));

            foreach (var call in new[]
            {
                percent,
                Call(SqlStdOperatorTable.DIVIDE, Column("N"), percent),
                Call(SqlStdOperatorTable.MULTIPLY, percent, Column("N")),
                Call(SqlStdOperatorTable.UNARY_MINUS, percent),
            })
            {
                Assert.Equal(Unparse(MssqlSqlDialect.DEFAULT, call), Unparse(dialect, call));
            }
        }

        /// <summary>
        /// Other products write the <c>MOD</c> function rather than <c>%</c>.
        /// </summary>
        /// <param name="productName">A product other than SQL Server.</param>
        [Theory]
        [InlineData("PostgreSQL")]
        [InlineData("Oracle")]
        public void AnotherProductKeepsTheFunction(string productName)
        {
            var call = Call(SqlStdOperatorTable.DIVIDE, Column("N"), Modulo());

            Assert.False(Unparse(AdoSqlDialects.For(productName, "1.0"), call).Contains('%'));
        }

        /// <summary>
        /// Resolves one of the operators the rows above name.
        /// </summary>
        /// <param name="name">The name of a <see cref="SqlStdOperatorTable"/> field, as the theory data gives
        /// it.</param>
        /// <returns>The standard operator of that name; the test fails for a name not handled here.</returns>
        static SqlOperator OperatorNamed(string name)
        {
            return name switch
            {
                nameof(SqlStdOperatorTable.DIVIDE) => SqlStdOperatorTable.DIVIDE,
                nameof(SqlStdOperatorTable.MULTIPLY) => SqlStdOperatorTable.MULTIPLY,
                nameof(SqlStdOperatorTable.MINUS) => SqlStdOperatorTable.MINUS,
                nameof(SqlStdOperatorTable.EQUALS) => SqlStdOperatorTable.EQUALS,
                nameof(SqlStdOperatorTable.MOD) => SqlStdOperatorTable.MOD,
                _ => throw new XunitException($"no operator {name}"),
            };
        }

        #endregion

    }

}
