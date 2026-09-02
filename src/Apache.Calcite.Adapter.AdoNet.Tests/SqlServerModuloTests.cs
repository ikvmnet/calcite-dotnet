using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using System.Collections.Generic;
using System.Linq;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers a modulo against a real SQL Server, where the grouping of the expression that reached it is
    /// the thing in question.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>MssqlSqlDialect</c> writes <c>MOD(a, b)</c> as <c>a % b</c>, which is right, and hands
    /// <c>SqlSyntax.BINARY</c> an operator of a different precedence from the call's — a function's 100
    /// against <c>PERCENT_REMAINDER</c>'s 60 — while <c>SqlCall.unparse</c> has already chosen the
    /// parentheses from the call's own. So a modulo standing as the right operand of an operator binding at
    /// 60 lost its grouping: <c>n / MOD(a, b)</c> went down as <c>n / a % b</c>, which the server reads as
    /// <c>(n / a) % b</c>.
    /// </para>
    /// <para>
    /// Unlike the concatenation half of the same correction, this one is reachable from a validated plan —
    /// the operands are numeric, so <c>*</c>, <c>/</c> and <c>%</c> all take one. It is also silent: the
    /// statement parses and runs and answers a number, so nothing failed and the number was wrong.
    /// </para>
    /// <para>
    /// SqlClient alone. That all three drivers reach one dialect is a claim
    /// <see cref="SqlServerConcatenationTests"/> makes and holds, and repeating the matrix here would test
    /// the drivers again rather than the rendering.
    /// </para>
    /// </remarks>
    [TestClass]
    public class SqlServerModuloTests
    {

        static SqlServerModuloTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        [TestInitialize]
        public void Setup()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Inconclusive("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// What a query answered, and what was sent to the server to answer it.
        /// </summary>
        /// <param name="Rows"></param>
        /// <param name="Statements"></param>
        readonly record struct Answer(List<string> Rows, IReadOnlyList<string> Statements);

        /// <summary>
        /// Runs a query against the fixture's database.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static Answer Run(string sql)
        {
            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            var generated = new GeneratedSql();
            var handle = Hook.QUERY_PLAN.addThread(generated);

            try
            {
                using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);
                var root = ((CalciteConnection)connection).getRootSchema();
                root.add("ADO", AdoSchema.Create(root, "ADO", SqlServerFixture.Shared.DataSource, null, "dbo"));

                using var statement = connection.createStatement();
                var results = statement.executeQuery(sql);

                var rows = new List<string>();
                while (results.next())
                    rows.Add(results.getObject(1)?.ToString() ?? "NULL");

                return new Answer(rows, generated.Statements);
            }
            finally
            {
                handle.close();
            }
        }

        /// <summary>
        /// Runs a query over <c>DEPTS</c> ordered by its key, so that the answers line up with 10, 20 and 30
        /// whatever order the server would otherwise have returned them in.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        static Answer OverDepartments(string expression)
        {
            return Run($"SELECT {expression} FROM ADO.DEPTS ORDER BY DEPTNO");
        }

        #region The grouping

        /// <summary>
        /// A modulo as the right operand of a division, which is where the rendering lost the grouping.
        /// Over 10, 20 and 30 the moduli are 3, 6 and 2, so the expression means 20, 10 and 30; grouped from
        /// the left it is <c>(60 / DEPTNO) % 7</c>, which is 6, 3 and 2.
        /// </summary>
        [TestMethod]
        public void AModuloUnderADivisionIsGrouped()
        {
            CollectionAssert.AreEqual(
                new[] { "20", "10", "30" },
                OverDepartments("60 / MOD(DEPTNO, 7)").Rows);
        }

        /// <summary>
        /// The same under a multiplication: the expression means 18, 36 and 12, and grouped from the left
        /// <c>(6 * DEPTNO) % 7</c> is 4, 1 and 5.
        /// </summary>
        [TestMethod]
        public void AModuloUnderAMultiplicationIsGrouped()
        {
            CollectionAssert.AreEqual(
                new[] { "18", "36", "12" },
                OverDepartments("6 * MOD(DEPTNO, 7)").Rows);
        }

        /// <summary>
        /// And under another modulo, which is the case that reads worst as SQL: the expression means 2, 2
        /// and 0, and <c>(20 % DEPTNO) % 7</c> is 0, 0 and 6.
        /// </summary>
        [TestMethod]
        public void AModuloUnderAModuloIsGrouped()
        {
            CollectionAssert.AreEqual(
                new[] { "2", "2", "0" },
                OverDepartments("MOD(20, MOD(DEPTNO, 7))").Rows);
        }

        #endregion

        #region What was already right

        /// <summary>
        /// As a left operand the rendering already meant what the call meant, left associativity giving the
        /// nesting for nothing. Kept here so that the correction is known to be to the one case.
        /// </summary>
        [TestMethod]
        public void AModuloAsALeftOperandIsUnaffected()
        {
            CollectionAssert.AreEqual(
                new[] { "9", "18", "6" },
                OverDepartments("MOD(DEPTNO, 7) * 3").Rows);
        }

        /// <summary>
        /// And under an operator that binds looser than <c>%</c> does.
        /// </summary>
        [TestMethod]
        public void AModuloUnderASubtractionIsUnaffected()
        {
            CollectionAssert.AreEqual(
                new[] { "97", "94", "98" },
                OverDepartments("100 - MOD(DEPTNO, 7)").Rows);
        }

        /// <summary>
        /// The plain rendering, which is Calcite's and is kept: <c>%</c> is the operator T-SQL has, and
        /// <c>MOD</c> is not a function it knows.
        /// </summary>
        [TestMethod]
        public void APlainModuloStillRuns()
        {
            CollectionAssert.AreEqual(
                new[] { "3", "6", "2" },
                OverDepartments("MOD(DEPTNO, 7)").Rows);
        }

        #endregion

        #region What went to the server

        /// <summary>
        /// The claim that the numbers above were answered by the server rather than by Calcite: the
        /// statement carries the operator, and carries it parenthesised.
        /// </summary>
        [TestMethod]
        public void TheGroupingIsPushedDown()
        {
            var answer = OverDepartments("60 / MOD(DEPTNO, 7)");
            var generated = string.Join("\n", answer.Statements);

            Assert.AreNotEqual(0, answer.Statements.Count, "nothing was pushed down at all");
            Assert.IsTrue(answer.Statements.Any(s => s.Contains('%')), $"nothing computed a modulo on the server: {generated}");
            Assert.IsTrue(answer.Statements.Any(s => s.Contains("([DEPTNO] % 7)")), $"the grouping did not go down: {generated}");
        }

        #endregion

    }

}
