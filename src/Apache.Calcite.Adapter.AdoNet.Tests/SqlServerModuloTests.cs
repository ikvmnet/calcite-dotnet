using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests the grouping of a modulo pushed down to SQL Server.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>MssqlSqlDialect</c> writes <c>MOD(a, b)</c> as <c>a % b</c>, but <c>SqlCall.unparse</c> chooses the
    /// parentheses from the call's own precedence (a function's 100) rather than <c>PERCENT_REMAINDER</c>'s
    /// 60. Calcite therefore writes <c>n / MOD(a, b)</c> as <c>n / a % b</c>, which the server reads as
    /// <c>(n / a) % b</c> and silently returns a wrong number. The adapter's dialect writes the parentheses.
    /// </para>
    /// <para>
    /// Only SqlClient is used; <see cref="SqlServerConcatenationTests"/> covers all three drivers reaching the
    /// same dialect.
    /// </para>
    /// </remarks>
    public class SqlServerModuloTests
    {

        static SqlServerModuloTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public SqlServerModuloTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// What a query returned, and the statements sent to the server to answer it.
        /// </summary>
        /// <param name="Rows">The first column of each row, as a string.</param>
        /// <param name="Statements">The statements the adapter generated.</param>
        readonly record struct Answer(List<string> Rows, IReadOnlyList<string> Statements);

        /// <summary>
        /// Runs a query against the fixture's database, recording the generated SQL.
        /// </summary>
        /// <param name="sql">The statement to run through Calcite against the fixture's SQL Server
        /// database.</param>
        /// <returns>The first column of every row, and the SQL statements the adapter sent to the
        /// server.</returns>
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
        /// Evaluates an expression over <c>DEPTS</c> ordered by its key, so the results line up with
        /// <c>DEPTNO</c> 10, 20 and 30.
        /// </summary>
        /// <param name="expression">The select-list expression to evaluate for each department.</param>
        /// <returns>One value per department in <c>DEPTNO</c> order, and the SQL the adapter sent.</returns>
        static Answer OverDepartments(string expression)
        {
            return Run($"SELECT {expression} FROM ADO.DEPTS ORDER BY DEPTNO");
        }

        #region The grouping

        /// <summary>
        /// A modulo as the right operand of a division. Over 10, 20 and 30 the moduli are 3, 6 and 2, so the
        /// expression means 20, 10 and 30; grouped from the left it would be <c>(60 / DEPTNO) % 7</c>, which
        /// is 6, 3 and 2.
        /// </summary>
        [Fact]
        public void AModuloUnderADivisionIsGrouped()
        {
            Assert.Equal(
                new[] { "20", "10", "30" },
                OverDepartments("60 / MOD(DEPTNO, 7)").Rows);
        }

        /// <summary>
        /// The same under a multiplication: the expression means 18, 36 and 12, while grouped from the left
        /// <c>(6 * DEPTNO) % 7</c> would be 4, 1 and 5.
        /// </summary>
        [Fact]
        public void AModuloUnderAMultiplicationIsGrouped()
        {
            Assert.Equal(
                new[] { "18", "36", "12" },
                OverDepartments("6 * MOD(DEPTNO, 7)").Rows);
        }

        /// <summary>
        /// The same under another modulo: the expression means 2, 2 and 0, while <c>(20 % DEPTNO) % 7</c>
        /// would be 0, 0 and 6.
        /// </summary>
        [Fact]
        public void AModuloUnderAModuloIsGrouped()
        {
            Assert.Equal(
                new[] { "2", "2", "0" },
                OverDepartments("MOD(20, MOD(DEPTNO, 7))").Rows);
        }

        #endregion

        #region What was already right

        /// <summary>
        /// As a left operand the rendering is correct without parentheses, by left associativity.
        /// </summary>
        [Fact]
        public void AModuloAsALeftOperandIsUnaffected()
        {
            Assert.Equal(
                new[] { "9", "18", "6" },
                OverDepartments("MOD(DEPTNO, 7) * 3").Rows);
        }

        /// <summary>
        /// Under an operator that binds looser than <c>%</c>, no parentheses are needed.
        /// </summary>
        [Fact]
        public void AModuloUnderASubtractionIsUnaffected()
        {
            Assert.Equal(
                new[] { "97", "94", "98" },
                OverDepartments("100 - MOD(DEPTNO, 7)").Rows);
        }

        /// <summary>
        /// A plain modulo runs: it is written as <c>%</c>, since T-SQL has no <c>MOD</c> function.
        /// </summary>
        [Fact]
        public void APlainModuloStillRuns()
        {
            Assert.Equal(
                new[] { "3", "6", "2" },
                OverDepartments("MOD(DEPTNO, 7)").Rows);
        }

        #endregion

        #region What went to the server

        /// <summary>
        /// The modulo is evaluated on the server: the generated statement carries <c>%</c>, parenthesised.
        /// </summary>
        [Fact]
        public void TheGroupingIsPushedDown()
        {
            var answer = OverDepartments("60 / MOD(DEPTNO, 7)");
            var generated = string.Join("\n", answer.Statements);

            answer.Statements.Count.Should().NotBe(0, "nothing was pushed down at all");
            Assert.True(answer.Statements.Any(s => s.Contains('%')), $"nothing computed a modulo on the server: {generated}");
            Assert.True(answer.Statements.Any(s => s.Contains("([DEPTNO] % 7)")), $"the grouping did not go down: {generated}");
        }

        #endregion

    }

}
