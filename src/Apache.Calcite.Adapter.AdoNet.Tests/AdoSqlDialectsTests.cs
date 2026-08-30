using Apache.Calcite.Adapter.AdoNet.Metadata;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
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

    }

}
