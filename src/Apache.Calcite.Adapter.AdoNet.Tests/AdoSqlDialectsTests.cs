using Apache.Calcite.Adapter.AdoNet.Metadata;

using System.Text.RegularExpressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers working out a dialect from the only thing a generic driver can be asked: the name of the
    /// product behind it.
    /// </summary>
    /// <remarks>
    /// No database, so this runs everywhere, which matters: the ODBC and OLE DB suites that reach the same
    /// code end to end need a Windows machine with LocalDB and skip on the rest of the matrix.
    /// </remarks>
    [TestClass]
    public class AdoSqlDialectsTests
    {

        /// <summary>
        /// Returns what a dialect writes for an <c>OFFSET</c> / <c>FETCH</c> pair.
        /// </summary>
        /// <param name="dialect"></param>
        /// <returns></returns>
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
        /// <param name="dialect"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        static string CastSpec(SqlDialect dialect, RelDataType type)
        {
            var writer = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(dialect));
            dialect.getCastSpec(type).unparse(writer, 0, 0);

            return writer.toSqlString().getSql().Trim();
        }

        /// <summary>
        /// Returns what a dialect writes for a statement, by parsing it and unparsing it again.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        /// <remarks>
        /// The route the adapter takes is a rel through <c>RelToSqlConverter</c>, and this is not that; it
        /// is the same answer because both end at <c>SqlNode.unparse</c>, which is where an operator is
        /// written. Measured on stock Calcite over the shapes in the report — projection, predicate, sort
        /// key, aggregate argument — the two routes give the same string for this operator, so the shorter
        /// one is what the assertions are made against and no schema is needed to make them.
        /// </remarks>
        static string Unparse(SqlDialect dialect, string sql)
        {
            return Unparse(dialect, SqlParser.create(sql).parseQuery());
        }

        /// <summary>
        /// Returns what a dialect writes for a node already built, which is the way to reach an operator the
        /// parser leaves unresolved.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        static string Unparse(SqlDialect dialect, SqlNode node)
        {
            return Regex.Replace(node.toSqlString(dialect).getSql(), @"\s+", " ").Trim();
        }

        /// <summary>
        /// A column reference, for building a call the parser will not produce.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static SqlNode Column(string name)
        {
            return new SqlIdentifier(name, SqlParserPos.ZERO);
        }

        /// <summary>
        /// A call to an operator over the operands given.
        /// </summary>
        /// <param name="op"></param>
        /// <param name="operands"></param>
        /// <returns></returns>
        static SqlNode Call(SqlOperator op, params SqlNode[] operands)
        {
            return op.createCall(SqlParserPos.ZERO, operands);
        }

        /// <summary>
        /// <c>MOD(A, B)</c>, which is the call every modulo test is written around.
        /// </summary>
        /// <returns></returns>
        static SqlNode Modulo()
        {
            return Call(SqlStdOperatorTable.MOD, Column("A"), Column("B"));
        }

        /// <summary>
        /// Builds types the way a connection does, from the default type system.
        /// </summary>
        static readonly RelDataTypeFactory Types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        /// <summary>
        /// Builds types from the type system <see cref="MssqlSqlDialect"/> carries, which is the one that
        /// leaves a <c>CHAR</c> with no precision.
        /// </summary>
        static readonly RelDataTypeFactory MssqlTypes = new SqlTypeFactoryImpl(MssqlSqlDialect.MSSQL_TYPE_SYSTEM);

        #region Product

        /// <remarks>
        /// SQL Server is absent because its dialect is not Calcite's own instance — see
        /// <see cref="TheCorrectedDialectIsStillTheSqlServerOne"/>.
        /// </remarks>
        [TestMethod]
        [DataRow("PostgreSQL", "PostgresqlSqlDialect")]
        [DataRow("Oracle", "OracleSqlDialect")]
        [DataRow("MySQL", "MysqlSqlDialect")]
        [DataRow("Apache Derby", "DerbySqlDialect")]
        [DataRow("ACCESS", "AccessSqlDialect")]
        // the DB2 driver reports its platform, and Calcite matches the prefix rather than the word
        [DataRow("DB2/LINUXX8664", "Db2SqlDialect")]
        [DataRow("Teradata Database", "TeradataSqlDialect")]
        [DataRow("SQLite", "SqliteSqlDialect")]
        public void AProductNameSelectsItsDialect(string productName, string expected)
        {
            Assert.AreEqual(expected, AdoSqlDialects.For(productName, "1.0").GetType().Name);
        }

        /// <summary>
        /// Calcite matches the name case-insensitively and after trimming, so this does too.
        /// </summary>
        [TestMethod]
        [DataRow("microsoft sql server")]
        [DataRow("  Microsoft SQL Server  ")]
        [DataRow("Microsoft SQL Server Enterprise Edition")]
        public void TheProductNameIsMatchedLoosely(string productName)
        {
            Assert.IsInstanceOfType<MssqlSqlDialect>(AdoSqlDialects.For(productName, "15.0"));
        }

        /// <summary>
        /// A driver that will not say what is behind it still has to get a dialect, and the generic one is
        /// what Calcite's own factory ends at.
        /// </summary>
        [TestMethod]
        public void AnUnknownProductGetsTheGenericDialect()
        {
            Assert.AreEqual("AnsiSqlDialect", AdoSqlDialects.For(null, null).GetType().Name);
            Assert.AreEqual("AnsiSqlDialect", AdoSqlDialects.For("Some Database Nobody Has Heard Of", "1.2.3").GetType().Name);
        }

        [TestMethod]
        public void AnUnknownProductIsTheUnknownProduct()
        {
            Assert.AreEqual(
                SqlDialect.DatabaseProduct.UNKNOWN,
                AdoSqlDialects.ProductFor("Some Database Nobody Has Heard Of"));
        }

        #endregion

        #region Version

        [TestMethod]
        [DataRow("15.00.4382", 15, 0)]
        [DataRow("10.50.1600.1", 10, 50)]
        [DataRow("9", 9, 0)]
        [DataRow("", 0, 0)]
        [DataRow(null, 0, 0)]
        public void AVersionIsSplitIntoItsComponents(string? version, int major, int minor)
        {
            Assert.AreEqual(major, AdoSqlDialects.MajorVersion(version));
            Assert.AreEqual(minor, AdoSqlDialects.MinorVersion(version));
        }

        /// <summary>
        /// The reason the version is carried at all. <c>MssqlSqlDialect</c> writes <c>TOP(n)</c> below major
        /// version 11 and <em>discards the offset</em> — a paged query then returns the first page for every
        /// page — so a dialect built without a version is not merely conservative, it is wrong.
        /// </summary>
        [TestMethod]
        public void SqlServerPastTwentyTwelveGetsOffsetFetch()
        {
            StringAssert.Contains(OffsetFetch(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382")), "OFFSET");
        }

        /// <summary>
        /// And below it, Calcite's own answer, reproduced rather than corrected.
        /// </summary>
        [TestMethod]
        public void SqlServerBeforeTwentyTwelveDoesNot()
        {
            Assert.AreEqual("", OffsetFetch(AdoSqlDialects.For("Microsoft SQL Server", "10.50.1600")).Trim());
        }

        /// <summary>
        /// A version that could not be read is the same case as no version at all, and lands on the
        /// conservative side rather than throwing.
        /// </summary>
        [TestMethod]
        public void AnUnreadableVersionIsNotAnError()
        {
            Assert.IsInstanceOfType<MssqlSqlDialect>(AdoSqlDialects.For("Microsoft SQL Server", "not a version"));
        }

        #endregion

        #region Group by a constant

        /// <summary>
        /// SQL Server cannot group by a constant and <c>MssqlSqlDialect</c> does not say so, which costs
        /// every correlated sub-query: <c>EXISTS</c> becomes an aggregate over a constant true, and the
        /// statement generated for it is <c>SELECT 1 AS [i] GROUP BY (1 = 1)</c> — "Incorrect syntax near
        /// '='", measured. <c>SqlImplementor.visitRoot</c> only runs the rule that rewrites it away when
        /// the dialect has asked for it.
        /// </summary>
        [TestMethod]
        public void SqlServerSaysItCannotGroupByAConstant()
        {
            Assert.IsFalse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382").supportsGroupByLiteral());
        }

        /// <summary>
        /// And it is still the SQL Server dialect, rather than a generic one that happens to say the same.
        /// </summary>
        [TestMethod]
        public void TheCorrectedDialectIsStillTheSqlServerOne()
        {
            Assert.IsInstanceOfType<MssqlSqlDialect>(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"));
        }

        /// <summary>
        /// The correction is to SQL Server alone: a dialect Calcite already had right is left as it is.
        /// </summary>
        [TestMethod]
        public void AnotherProductKeepsCalcitesOwnAnswer()
        {
            Assert.IsFalse(AdoSqlDialects.For("PostgreSQL", "16.0").supportsGroupByLiteral(), "Postgres says so itself");
            Assert.IsTrue(AdoSqlDialects.For("MySQL", "8.0").supportsGroupByLiteral(), "MySQL can");
        }

        #endregion

        #region Unbounded strings

        /// <summary>
        /// A Calcite <c>VARCHAR</c> with no precision is unbounded, and the bare keyword SQL Server reads it
        /// as is thirty characters in a cast — so <c>CAST(&lt;uniqueidentifier&gt; AS VARCHAR)</c> is
        /// "Insufficient result space to convert uniqueidentifier value to char" and the same cast over a
        /// long string returns its first thirty characters with no error at all.
        /// </summary>
        [TestMethod]
        public void AnUnboundedVarcharBecomesVarcharMax()
        {
            Assert.AreEqual("VARCHAR(MAX)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Types.createSqlType(SqlTypeName.VARCHAR)));
        }

        /// <summary>
        /// And the answer this corrects, so that the test says what it is for: Calcite writes the keyword
        /// alone, which is a different type on the server.
        /// </summary>
        [TestMethod]
        public void CalcitesOwnAnswerIsTheBareKeyword()
        {
            Assert.AreEqual("VARCHAR", CastSpec(MssqlSqlDialect.DEFAULT, Types.createSqlType(SqlTypeName.VARCHAR)));
        }

        /// <summary>
        /// The correction is to the unbounded case alone: a stated length is what the caller asked for and
        /// is written as it stands.
        /// </summary>
        [TestMethod]
        [DataRow(nameof(SqlTypeName.VARCHAR), 36, "VARCHAR(36)")]
        [DataRow(nameof(SqlTypeName.CHAR), 36, "CHAR(36)")]
        [DataRow(nameof(SqlTypeName.VARBINARY), 16, "VARBINARY(16)")]
        [DataRow(nameof(SqlTypeName.BINARY), 4, "BINARY(4)")]
        public void AStatedLengthIsLeftAlone(string typeName, int precision, string expected)
        {
            var type = Types.createSqlType(SqlTypeName.valueOf(typeName), precision);
            Assert.AreEqual(expected, CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), type));
        }

        /// <summary>
        /// <c>varbinary</c> carries the same rule over bytes, and <c>VARBINARY</c>'s default precision is
        /// unspecified for the same reason <c>VARCHAR</c>'s is.
        /// </summary>
        [TestMethod]
        public void AnUnboundedVarbinaryBecomesVarbinaryMax()
        {
            Assert.AreEqual("VARBINARY(MAX)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Types.createSqlType(SqlTypeName.VARBINARY)));
        }

        /// <summary>
        /// A <c>CHAR</c> reaches the same rendering where the type system leaves its precision unspecified —
        /// CALCITE-6565 made the bare keyword the intended answer for SQL Server, and the server reads it as
        /// thirty. There is no <c>char(max)</c> in T-SQL, and a fixed length with no length has nothing to
        /// pad to.
        /// </summary>
        [TestMethod]
        public void AnUnboundedCharBecomesVarcharMax()
        {
            var type = MssqlTypes.createSqlType(SqlTypeName.CHAR);
            Assert.AreEqual("CHAR", CastSpec(MssqlSqlDialect.DEFAULT, type), "the answer being corrected");
            Assert.AreEqual("VARCHAR(MAX)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), type));
        }

        /// <summary>
        /// Under the default type system a <c>CHAR</c> has a precision of one, so nothing changes for it.
        /// </summary>
        [TestMethod]
        public void ACharOfTheDefaultTypeSystemKeepsItsOne()
        {
            Assert.AreEqual("CHAR(1)", CastSpec(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Types.createSqlType(SqlTypeName.CHAR)));
        }

        /// <summary>
        /// Nothing else is touched.
        /// </summary>
        [TestMethod]
        public void AnotherTypeKeepsCalcitesAnswer()
        {
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");

            Assert.AreEqual("INTEGER", CastSpec(dialect, Types.createSqlType(SqlTypeName.INTEGER)));
            Assert.AreEqual("DECIMAL(12, 3)", CastSpec(dialect, Types.createSqlType(SqlTypeName.DECIMAL, 12, 3)));
        }

        /// <summary>
        /// And the correction is SQL Server's alone. The bare keyword is a different default per product:
        /// SQLite ignores a length entirely, and Postgres reads a bare <c>varchar</c> as unbounded, which is
        /// what Calcite means. The claim is that no length was written, rather than that the whole spec is
        /// the keyword: SQLite says it supports a character set, so Calcite names one after it.
        /// </summary>
        [TestMethod]
        [DataRow("SQLite")]
        [DataRow("PostgreSQL")]
        public void AnotherProductKeepsTheBareKeyword(string productName)
        {
            var spec = CastSpec(AdoSqlDialects.For(productName, "1.0"), Types.createSqlType(SqlTypeName.VARCHAR));

            StringAssert.StartsWith(spec, "VARCHAR");
            Assert.IsFalse(spec.Contains('('), $"a length was written where the bare keyword is right: {spec}");
        }

        /// <summary>
        /// A driver that only says what is behind it reaches the same corrected dialect, which is what
        /// carries the fix to ODBC and OLE DB over SQL Server.
        /// </summary>
        [TestMethod]
        [DataRow("Microsoft SQL Server")]
        [DataRow("microsoft sql server")]
        [DataRow("Microsoft SQL Server Enterprise Edition")]
        public void AnyNameThatSelectsSqlServerGetsTheCorrection(string productName)
        {
            Assert.AreEqual("VARCHAR(MAX)", CastSpec(AdoSqlDialects.For(productName, "10.50.1600"), Types.createSqlType(SqlTypeName.VARCHAR)));
        }

        #endregion

        #region Concatenation

        /// <summary>
        /// T-SQL has no <c>||</c>, and every statement that concatenates reached the server carrying one.
        /// The four shapes are the ones measured in the report, and the fifth is two literals, which are
        /// not folded away — so there is no spelling of the expression that avoids the operator.
        /// </summary>
        [TestMethod]
        [DataRow("SELECT A || B FROM CAT WHERE ID = 1", "a projection")]
        [DataRow("SELECT ID FROM CAT WHERE A || B = 'aabb'", "a predicate")]
        [DataRow("SELECT ID FROM CAT ORDER BY A || B", "a sort key")]
        [DataRow("SELECT MAX(A || B) FROM CAT", "an aggregate argument")]
        [DataRow("SELECT A || B FROM CAT GROUP BY A || B", "a group key")]
        [DataRow("SELECT 'x' || 'y' FROM CAT WHERE ID = 1", "two literals")]
        public void ConcatenationIsWrittenAsPlus(string sql, string shape)
        {
            var written = Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql);

            Assert.IsFalse(written.Contains("||"), $"{shape} still carries the operator the server refuses: {written}");
            StringAssert.Contains(written, "+", $"{shape}: {written}");
        }

        /// <summary>
        /// And the answer this corrects, so that the test says what it is for. <c>MssqlSqlDialect</c>
        /// intercepts <c>SUBSTRING</c>, <c>CEIL</c>, <c>FLOOR</c>, <c>MOD</c> and <c>SAFE_CAST</c> and not
        /// this, so the operator goes down as it stands and the server answers "Incorrect syntax near '|'".
        /// </summary>
        [TestMethod]
        public void CalcitesOwnAnswerIsTheOperatorTheServerRefuses()
        {
            Assert.AreEqual(
                "SELECT [A] || [B] FROM [CAT] WHERE [ID] = 1",
                Unparse(MssqlSqlDialect.DEFAULT, "SELECT A || B FROM CAT WHERE ID = 1"));
        }

        /// <summary>
        /// The whole statement, rather than the operator alone, for each shape — a substitution that writes
        /// the right operator into the wrong place is still wrong.
        /// </summary>
        [TestMethod]
        [DataRow(
            "SELECT A || B FROM CAT WHERE ID = 1",
            "SELECT [A] + [B] FROM [CAT] WHERE [ID] = 1")]
        [DataRow(
            "SELECT ID FROM CAT WHERE A || B = 'aabb'",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] = 'aabb'")]
        [DataRow(
            "SELECT MAX(A || B) FROM CAT",
            "SELECT MAX([A] + [B]) FROM [CAT]")]
        [DataRow(
            "SELECT A || B FROM CAT GROUP BY A || B",
            "SELECT [A] + [B] FROM [CAT] GROUP BY [A] + [B]")]
        public void TheStatementIsWrittenWhole(string sql, string expected)
        {
            Assert.AreEqual(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql));
        }

        /// <summary>
        /// <c>+</c> and not <c>CONCAT</c>, and the difference is answers rather than taste: <c>||</c> yields
        /// null when either operand is null, <c>+</c> does the same under the default
        /// <c>CONCAT_NULL_YIELDS_NULL</c>, and T-SQL's <c>CONCAT</c> reads a null operand as the empty
        /// string. The function would turn a query that should return nothing into one that returns a row.
        /// </summary>
        [TestMethod]
        public void TheFunctionIsNotWhatIsWritten()
        {
            var written = Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), "SELECT A || B FROM CAT");

            Assert.IsFalse(written.Contains("CONCAT"), $"the function does not propagate null: {written}");
        }

        /// <summary>
        /// The correction is SQL Server's alone: a product whose own operator is <c>||</c> keeps it.
        /// </summary>
        [TestMethod]
        [DataRow("PostgreSQL")]
        [DataRow("SQLite")]
        [DataRow("Oracle")]
        // the generic dialect an unknown product gets, which is where a driver that will not say what it
        // fronts ends up
        [DataRow("Some Database Nobody Has Heard Of")]
        public void AnotherProductKeepsTheOperator(string productName)
        {
            StringAssert.Contains(Unparse(AdoSqlDialects.For(productName, "1.0"), "SELECT A || B FROM CAT"), "||");
        }

        /// <summary>
        /// ODBC and OLE DB reach SQL Server through this same dialect, and a name is all either can offer,
        /// so every name that selects SQL Server has to carry the correction with it.
        /// </summary>
        [TestMethod]
        [DataRow("Microsoft SQL Server")]
        [DataRow("microsoft sql server")]
        [DataRow("Microsoft SQL Server Enterprise Edition")]
        public void AnyNameThatSelectsSqlServerGetsTheOperator(string productName)
        {
            Assert.AreEqual(
                "SELECT [A] + [B] FROM [CAT]",
                Unparse(AdoSqlDialects.For(productName, "10.50.1600"), "SELECT A || B FROM CAT"));
        }

        /// <summary>
        /// The override is a case ahead of <c>MssqlSqlDialect.unparseCall</c> rather than a replacement of
        /// it, so what that method already intercepts still happens. <c>MOD</c> is the one this is modelled
        /// on — CALCITE-6726 swapped an operator in the same way — and <c>CEIL</c> is a rewrite of a
        /// different shape.
        /// </summary>
        [TestMethod]
        [DataRow("SELECT CEIL(SALARY) FROM CAT", "CEILING")]
        [DataRow("SELECT SUBSTRING(A FROM 1 FOR 2) FROM CAT", "SUBSTRING")]
        [DataRow("SELECT CAST(A AS INTEGER) FROM CAT", "CAST")]
        public void TheDialectsOwnInterceptionsStillHappen(string sql, string expected)
        {
            StringAssert.Contains(Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql), expected);
        }


        #endregion

        #region Concatenation and precedence

        /// <summary>
        /// <c>SqlSyntax.BINARY.unparse</c> is handed <c>PLUS</c>, whose precedence is not the one the call
        /// carries — <c>||</c> is 60 and <c>+</c> is 40 — and <c>SqlCall.unparse</c> has already decided the
        /// parentheses around the call from the call's own operator by the time the dialect is asked. So a
        /// nested expression is where a substitution of this shape goes wrong.
        /// </summary>
        /// <remarks>
        /// Concatenation nests inside itself, inside a comparison, inside a postfix operator and inside a
        /// call that writes its own parentheses; all four are here, and the last row is the one where the
        /// two precedences differ.
        /// </remarks>
        [TestMethod]
        // concatenation in concatenation, which associates the same either way
        [DataRow(
            "SELECT A || B || C FROM CAT",
            "SELECT [A] + [B] + [C] FROM [CAT]")]
        // the right operand is parenthesised under either operator, each being left associative, so the
        // grouping the caller wrote survives the substitution
        [DataRow(
            "SELECT A || (B || C) FROM CAT",
            "SELECT [A] + ([B] + [C]) FROM [CAT]")]
        // against a comparison, which binds looser than either spelling
        [DataRow(
            "SELECT ID FROM CAT WHERE A || B = 'aabb'",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] = 'aabb'")]
        [DataRow(
            "SELECT ID FROM CAT WHERE A || B > 'aa' AND ID > 1",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] > 'aa' AND [ID] > 1")]
        // a postfix operator, which is what a sort key's null ordering is written with
        [DataRow(
            "SELECT ID FROM CAT WHERE A || B IS NULL",
            "SELECT [ID] FROM [CAT] WHERE [A] + [B] IS NULL")]
        // arithmetic reaches a string only through a cast, and a cast writes its own parentheses
        [DataRow(
            "SELECT A || CAST(ID + 1 AS VARCHAR(4)) FROM CAT",
            "SELECT [A] + CAST([ID] + 1 AS VARCHAR(4)) FROM [CAT]")]
        [DataRow(
            "SELECT CAST(A || B AS VARCHAR(4)) FROM CAT",
            "SELECT CAST([A] + [B] AS VARCHAR(4)) FROM [CAT]")]
        // and inside a function call, whose frame parenthesises whatever it holds
        [DataRow(
            "SELECT UPPER(A || B) FROM CAT",
            "SELECT UPPER([A] + [B]) FROM [CAT]")]
        // the one context that binds between the two precedences, and the reason the override applies
        // PLUS's parenthesisation itself rather than inheriting the one computed for the operator it
        // replaces: without that, this is [A] + [B] * 2
        [DataRow(
            "SELECT (A || B) * 2 FROM CAT",
            "SELECT ([A] + [B]) * 2 FROM [CAT]")]
        public void ANestedConcatenationKeepsItsGrouping(string sql, string expected)
        {
            Assert.AreEqual(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), sql));
        }

        /// <summary>
        /// Calcite writes no parentheses there, and is right not to: <c>||</c> binds as tightly as <c>*</c>
        /// does. It is the substitution that makes them necessary, which is why closing the gap is this
        /// override's job and not something to leave to the shape of the expression.
        /// </summary>
        [TestMethod]
        public void TheGroupingIsOnlyAtRiskBecauseOfTheSubstitution()
        {
            Assert.AreEqual(
                "SELECT [A] || [B] * 2 FROM [CAT]",
                Unparse(MssqlSqlDialect.DEFAULT, "SELECT (A || B) * 2 FROM CAT"));
        }

        #endregion

        #region Modulo

        /// <summary>
        /// <c>MssqlSqlDialect</c> already writes a modulo as the operator T-SQL has, which CALCITE-6726
        /// added and nothing pinned. This is the answer that is kept.
        /// </summary>
        /// <remarks>
        /// Built rather than parsed throughout this region. An unqualified function name is a
        /// <c>SqlUnresolvedFunction</c> until the validator has run, and the interception switches on
        /// <c>SqlKind.MOD</c>, which an unresolved call does not carry — so parse-then-unparse writes
        /// <c>MOD([A], [B])</c> and says nothing about the interception either way.
        /// </remarks>
        [TestMethod]
        public void ModuloIsWrittenAsThePercentOperator()
        {
            Assert.AreEqual("[A] % [B]", Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), Modulo()));
        }

        /// <summary>
        /// What is corrected is where the parentheses go. <c>MOD</c> is a function and carries a function's
        /// precedence of 100; <c>PERCENT_REMAINDER</c> is 60. So as the right operand of an operator that
        /// binds at 60, the substitution loses the grouping the call had, and the operands being numeric
        /// there is no shape of the expression a validated plan cannot reach.
        /// </summary>
        [TestMethod]
        [DataRow(nameof(SqlStdOperatorTable.DIVIDE), "[N] / ([A] % [B])")]
        [DataRow(nameof(SqlStdOperatorTable.MULTIPLY), "[N] * ([A] % [B])")]
        [DataRow(nameof(SqlStdOperatorTable.MOD), "[N] % ([A] % [B])")]
        public void AModuloAsARightOperandKeepsItsGrouping(string operatorName, string expected)
        {
            var call = Call(OperatorNamed(operatorName), Column("N"), Modulo());

            Assert.AreEqual(expected, Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), call));
        }

        /// <summary>
        /// And the answers this corrects, so that the tests say what they are for. Each is grouped by the
        /// server from the left: over 12, 7 and 4 they answer 1, 0 and 1 where the expressions mean 4, 36
        /// and 0 — measured on the server, not derived from the precedence table.
        /// </summary>
        [TestMethod]
        [DataRow(nameof(SqlStdOperatorTable.DIVIDE), "[N] / [A] % [B]")]
        [DataRow(nameof(SqlStdOperatorTable.MULTIPLY), "[N] * [A] % [B]")]
        [DataRow(nameof(SqlStdOperatorTable.MOD), "[N] % [A] % [B]")]
        public void CalcitesOwnAnswerLosesTheGrouping(string operatorName, string expected)
        {
            var call = Call(OperatorNamed(operatorName), Column("N"), Modulo());

            Assert.AreEqual(expected, Unparse(MssqlSqlDialect.DEFAULT, call));
        }

        /// <summary>
        /// Everywhere else the rendering already meant what the call meant, and is left as it stands. As a
        /// left operand left associativity gives the nesting for nothing; <c>%</c> binds tighter than
        /// <c>-</c>; and SQL Server's <c>%</c> takes the sign of its dividend, so <c>(-a) % b</c> and
        /// <c>-(a % b)</c> agree.
        /// </summary>
        [TestMethod]
        [DataRow(nameof(SqlStdOperatorTable.MULTIPLY), "[A] % [B] * [N]")]
        [DataRow(nameof(SqlStdOperatorTable.DIVIDE), "[A] % [B] / [N]")]
        [DataRow(nameof(SqlStdOperatorTable.MINUS), "[A] % [B] - [N]")]
        [DataRow(nameof(SqlStdOperatorTable.EQUALS), "[A] % [B] = [N]")]
        public void AModuloAsALeftOperandIsUnchanged(string operatorName, string expected)
        {
            var call = Call(OperatorNamed(operatorName), Modulo(), Column("N"));
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");

            Assert.AreEqual(expected, Unparse(dialect, call));
            Assert.AreEqual(expected, Unparse(MssqlSqlDialect.DEFAULT, call), "Calcite already writes this one");
        }

        /// <summary>
        /// The right-operand context that was already right, for the same reason: a looser operator needs
        /// no parentheses around a tighter one, and <c>%</c> binds tighter than <c>-</c>.
        /// </summary>
        [TestMethod]
        public void AModuloUnderALooserOperatorIsUnchanged()
        {
            var dialect = AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382");
            var minus = Call(SqlStdOperatorTable.MINUS, Column("N"), Modulo());

            Assert.AreEqual("[N] - [A] % [B]", Unparse(dialect, minus));
            Assert.AreEqual(Unparse(MssqlSqlDialect.DEFAULT, minus), Unparse(dialect, minus));
        }

        /// <summary>
        /// A prefix operator hands its operand a left precedence of 80, which outranks
        /// <c>PERCENT_REMAINDER</c>'s 60, so the parentheses go on where Calcite writes none. This is the
        /// one place the correction writes a parenthesis Calcite would not have.
        /// </summary>
        /// <remarks>
        /// Both compute the same thing here, and Calcite is not wrong — but only because SQL Server's
        /// <c>%</c> takes the sign of its dividend, so <c>(-a) % b</c> and <c>-(a % b)</c> agree, measured
        /// at -3 over 7 and 4. The rule the override applies does not know that and does not need to: it
        /// writes the grouping the call had, and a parenthesis that was not needed costs nothing.
        /// </remarks>
        [TestMethod]
        public void AModuloUnderAPrefixOperatorIsParenthesised()
        {
            var negated = Call(SqlStdOperatorTable.UNARY_MINUS, Modulo());

            Assert.AreEqual("- ([A] % [B])", Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), negated));
            Assert.AreEqual("- [A] % [B]", Unparse(MssqlSqlDialect.DEFAULT, negated), "the answer this does not need to correct");
        }

        /// <summary>
        /// A postfix operator, which is what a sort key's null ordering is written with.
        /// </summary>
        [TestMethod]
        public void AModuloUnderAPostfixOperatorIsUnchanged()
        {
            var call = Call(SqlStdOperatorTable.IS_NULL, Modulo());

            Assert.AreEqual("[A] % [B] IS NULL", Unparse(AdoSqlDialects.For("Microsoft SQL Server", "15.00.4382"), call));
        }

        /// <summary>
        /// <c>SqlKind.MOD</c> is carried by two operators, and the other one is <c>PERCENT_REMAINDER</c>
        /// itself — what a query written with <c>%</c> produces, under a conformance level that allows one.
        /// The interception then substitutes an operator for itself, and the rule is applied to the same
        /// precedences <c>SqlCall.unparse</c> has just applied it to, so it answers the same and no
        /// parenthesis is written twice.
        /// </summary>
        [TestMethod]
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
                Assert.AreEqual(Unparse(MssqlSqlDialect.DEFAULT, call), Unparse(dialect, call));
            }
        }

        /// <summary>
        /// The correction is SQL Server's alone. Another product writes the function, and writes it whole,
        /// so there is no substitution to lose a grouping over.
        /// </summary>
        [TestMethod]
        [DataRow("PostgreSQL")]
        [DataRow("Oracle")]
        public void AnotherProductKeepsTheFunction(string productName)
        {
            var call = Call(SqlStdOperatorTable.DIVIDE, Column("N"), Modulo());

            Assert.IsFalse(Unparse(AdoSqlDialects.For(productName, "1.0"), call).Contains('%'));
        }

        /// <summary>
        /// Resolves one of the operators the rows above name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static SqlOperator OperatorNamed(string name)
        {
            return name switch
            {
                nameof(SqlStdOperatorTable.DIVIDE) => SqlStdOperatorTable.DIVIDE,
                nameof(SqlStdOperatorTable.MULTIPLY) => SqlStdOperatorTable.MULTIPLY,
                nameof(SqlStdOperatorTable.MINUS) => SqlStdOperatorTable.MINUS,
                nameof(SqlStdOperatorTable.EQUALS) => SqlStdOperatorTable.EQUALS,
                nameof(SqlStdOperatorTable.MOD) => SqlStdOperatorTable.MOD,
                _ => throw new AssertFailedException($"no operator {name}"),
            };
        }

        #endregion

    }

}
