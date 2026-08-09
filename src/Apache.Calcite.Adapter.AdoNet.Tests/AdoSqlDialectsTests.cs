using Apache.Calcite.Adapter.AdoNet.Metadata;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;

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

    }

}
