using System;
using System.Collections.Generic;
using System.Linq;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the row count of a <c>TOP</c>, an <c>OFFSET</c> or a <c>FETCH</c> reaching a real SQL Server,
    /// where what the count is written as is the thing in question.
    /// </summary>
    /// <remarks>
    /// <para>
    /// CALCITE-7624 widened both counts to a <c>BigDecimal</c>: a literal may have a fractional part, and
    /// <c>SqlValidatorImpl.handleOffsetFetch</c> types a placeholder in either slot <c>DECIMAL</c>, so a
    /// caller's value arrives as a decimal too. SQL Server takes neither — "The number of rows provided for
    /// a TOP or FETCH clauses row count parameter must be an integer" — which the dialect puts right.
    /// </para>
    /// <para>
    /// The rows are half the claim and the statement is the other half: a query the adapter declined to push
    /// answers correctly while saying nothing about what was sent, which is exactly the case that hid this
    /// defect — a projection the source could not evaluate left the limit in process and it worked.
    /// </para>
    /// </remarks>
    public class SqlServerRowCountTests : IDisposable
    {

        static SqlServerRowCountTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        java.sql.Connection _connection = null!;
        GeneratedSql _generated = null!;
        Hook.Closeable _hook = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public SqlServerRowCountTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            _connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);

            var root = ((CalciteConnection)_connection).getRootSchema();
            root.add("ADO", AdoSchema.Create(root, "ADO", SqlServerFixture.Shared.DataSource, null, "dbo"));

            _generated = new GeneratedSql();
            _hook = Hook.QUERY_PLAN.addThread(_generated);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _hook?.close();
            _connection?.close();
        }

        /// <summary>
        /// Runs a query and returns its one column as strings.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        List<string> Rows(string sql)
        {
            using var statement = _connection.createStatement();
            return Read(statement.executeQuery(sql));
        }

        /// <summary>
        /// Runs a query whose row counts are parameters, binding the given values in order.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="counts"></param>
        /// <returns></returns>
        /// <remarks>
        /// As <c>BigDecimal</c>s, because that is what the placeholder's inferred <c>DECIMAL</c> makes of
        /// whatever an ADO.NET caller bound, and what the report is about. A caller's own naming of the
        /// parameter's type made no difference to it.
        /// </remarks>
        List<string> Rows(string sql, params string[] counts)
        {
            using var statement = _connection.prepareStatement(sql);

            for (int i = 0; i < counts.Length; i++)
                statement.setBigDecimal(i + 1, new java.math.BigDecimal(counts[i]));

            return Read(statement.executeQuery());
        }

        static List<string> Read(java.sql.ResultSet results)
        {
            var rows = new List<string>();
            while (results.next())
                rows.Add(results.getObject(1)?.ToString() ?? "NULL");

            return rows;
        }

        /// <summary>
        /// The one statement the adapter pushed down.
        /// </summary>
        string Statement => _generated.Statements.Single();

        /// <summary>
        /// The case from the report: a <c>FETCH</c> whose count is a parameter. The whole of the fix is that
        /// the marker no longer stands alone, so the count the server reads is not the decimal that was bound.
        /// </summary>
        [Fact]
        public void AParameterisedFetchAnswers()
        {
            Assert.Equal(
                new[] { "1", "2" },
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST ? ROWS ONLY", "2"));

            Assert.Contains("TOP (CAST(CEILING(@P0) AS INT))", Statement);
        }

        /// <summary>
        /// And an <c>OFFSET</c> whose count is a parameter, which failed the same way for the same reason.
        /// </summary>
        [Fact]
        public void AParameterisedOffsetAnswers()
        {
            Assert.Equal(
                new[] { "2", "3", "4" },
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO OFFSET ? ROWS", "1"));

            Assert.Contains("OFFSET CAST(CEILING(@P0) AS INT) ROWS", Statement);
        }

        /// <summary>
        /// Both at once, which is what paging actually asks for, and what <c>$top</c> and <c>$skip</c> send.
        /// </summary>
        [Fact]
        public void AParameterisedOffsetAndFetchAnswer()
        {
            Assert.Equal(
                new[] { "2", "3" },
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", "1", "2"));

            Assert.Contains("OFFSET CAST(CEILING(@P0) AS INT) ROWS", Statement);
            Assert.Contains("FETCH NEXT (CAST(CEILING(@P1) AS INT)) ROWS ONLY", Statement);
        }

        /// <summary>
        /// A count bound as an integer rather than as a decimal is the same statement and the same answer:
        /// what reads the count is the server, and the cast is a no-op over one that is already whole.
        /// </summary>
        [Fact]
        public void AnIntegerBoundFetchAnswers()
        {
            using var statement = _connection.prepareStatement("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST ? ROWS ONLY");
            statement.setInt(1, 2);

            Assert.Equal(new[] { "1", "2" }, Read(statement.executeQuery()));
        }

        /// <summary>
        /// A literal count with a fractional part is written as the whole number of rows it stands for, and
        /// the literal stays a literal — nothing is cast around a count this already knows.
        /// </summary>
        /// <remarks>
        /// <c>TOP (2.9)</c> is the same refusal a decimal parameter earns. Three is the count because
        /// <c>EnumerableDefaults.take</c> counts while the zero-based index is below the bound, which
        /// <c>RexUtil.makeOffsetFetchSum</c> states as rounding "to whole row counts".
        /// </remarks>
        [Fact]
        public void AFractionalFetchIsWrittenAsWholeRows()
        {
            Assert.Equal(
                new[] { "1", "2", "3" },
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST 2.9 ROWS ONLY"));

            Assert.Contains("TOP (3)", Statement);
        }

        /// <inheritdoc cref="AFractionalFetchIsWrittenAsWholeRows"/>
        [Fact]
        public void AFractionalOffsetIsWrittenAsWholeRows()
        {
            Assert.Equal(
                new[] { "3", "4" },
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO OFFSET 1.5 ROWS"));

            Assert.Contains("OFFSET 2 ROWS", Statement);
        }

        /// <summary>
        /// A whole count is written exactly as it was, which is what keeps <c>TOP (2)</c> a constant the
        /// server can plan a row goal against.
        /// </summary>
        [Fact]
        public void AWholeFetchIsWrittenUnchanged()
        {
            Assert.Equal(
                new[] { "1", "2" },
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST 2 ROWS ONLY"));

            Assert.Contains("TOP (2)", Statement);
        }

        /// <summary>
        /// And the rows a fractional count means are the rows the operator in process means by it, read off
        /// a source the adapter has nothing to do with. The literal arm is rounded here and the parameter
        /// arm by the server, and this holds both to the same answer.
        /// </summary>
        [Theory]
        [InlineData("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO OFFSET 1.5 ROWS FETCH NEXT 2.9 ROWS ONLY")]
        [InlineData("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST 2.9 ROWS ONLY")]
        [InlineData("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO OFFSET 1.5 ROWS")]
        public void AFractionalCountMeansTheSameRowsInProcess(string sql)
        {
            var inProcess = sql
                .Replace("EMPNO FROM ADO.EMPS", "x FROM (VALUES (1), (2), (3), (4)) AS t(x)")
                .Replace("ORDER BY EMPNO", "ORDER BY x");

            Assert.Equal(Rows(inProcess), Rows(sql));
        }

        /// <inheritdoc cref="AFractionalCountMeansTheSameRowsInProcess"/>
        [Fact]
        public void AFractionalParameterisedCountMeansTheSameRowsInProcess()
        {
            Assert.Equal(
                Rows("SELECT x FROM (VALUES (1), (2), (3), (4)) AS t(x) ORDER BY x OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", "1.5", "2.9"),
                Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", "1.5", "2.9"));
        }

        /// <summary>
        /// A literal count with no <c>int</c> is refused where it is written, naming the clause and the
        /// count.
        /// </summary>
        [Fact]
        public void AFetchWiderThanAnIntIsRefused()
        {
            var e = Assert.ThrowsAny<java.sql.SQLException>(
                () => Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST 3000000000 ROWS ONLY"));

            Assert.Contains("FETCH", Message(e));
            Assert.Contains("3000000000", Message(e));
        }

        /// <summary>
        /// The same count as a parameter is refused by the server instead, the cast being where it would
        /// have to fit: "Arithmetic overflow error converting expression to data type int".
        /// </summary>
        /// <remarks>
        /// As the driver's own exception rather than a <c>java.sql.SQLException</c>: the server raises this
        /// on the first read rather than on the command, which is past <c>AdoEnumerable.enumerator</c> and
        /// so past the one place a driver's exception is wrapped and given the statement that earned it.
        /// </remarks>
        [Fact]
        public void AParameterisedFetchWiderThanAnIntIsRefused()
        {
            var e = Assert.ThrowsAny<Microsoft.Data.SqlClient.SqlException>(
                () => Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST ? ROWS ONLY", "3000000000"));

            Assert.Contains("Arithmetic overflow", Message(e));
        }

        /// <summary>
        /// A negative count is refused too, and by both: <c>EnumUtils.numberToBigDecimal</c> reads it that
        /// way in process and SQL Server says the same of the count the cast handed it.
        /// </summary>
        [Fact]
        public void ANegativeParameterisedFetchIsRefusedEitherWay()
        {
            var pushed = Assert.ThrowsAny<Microsoft.Data.SqlClient.SqlException>(
                () => Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST ? ROWS ONLY", "-1"));

            var inProcess = Assert.ThrowsAny<java.sql.SQLException>(
                () => Rows("SELECT x FROM (VALUES (1), (2)) AS t(x) ORDER BY x FETCH FIRST ? ROWS ONLY", "-1"));

            Assert.Contains("may not be negative", Message(pushed));
            Assert.Contains("must not be negative", Message(inProcess));
        }

        /// <summary>
        /// The whole of an exception's chain, because what a driver or a runtime says is usually several
        /// wrappers down from what was thrown.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns></returns>
        static string Message(Exception exception)
        {
            var text = new System.Text.StringBuilder();
            for (Exception? e = exception; e is not null; e = e.InnerException)
                text.AppendLine(e.Message);

            return text.ToString();
        }

    }

}
