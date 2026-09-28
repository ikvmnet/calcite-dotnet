using System;
using System.Collections.Generic;
using System.Linq;

using org.apache.calcite.jdbc;
using org.apache.calcite.runtime;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests how the row count of a <c>TOP</c>, <c>OFFSET</c> or <c>FETCH</c> is written for SQL Server.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite holds both counts as a <c>BigDecimal</c>: a literal may have a fractional part, and
    /// <c>SqlValidatorImpl.handleOffsetFetch</c> types a placeholder in either position as <c>DECIMAL</c>.
    /// SQL Server requires an integer row count, so the adapter's dialect rounds a literal up and wraps a
    /// parameter in <c>CAST(CEILING(…) AS INT)</c>.
    /// </para>
    /// <para>
    /// Each test checks the generated statement as well as the rows, because a limit evaluated in process
    /// would give the same rows.
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
        /// Runs a query and returns its first column as strings.
        /// </summary>
        /// <param name="sql">The statement to run through Calcite.</param>
        /// <returns>The first column of every row, with <c>NULL</c> for a null.</returns>
        List<string> Rows(string sql)
        {
            using var statement = _connection.createStatement();
            return Read(statement.executeQuery(sql));
        }

        /// <summary>
        /// Runs a query whose row counts are parameters, binding the given values in order.
        /// </summary>
        /// <remarks>
        /// The values are bound as <c>BigDecimal</c>s, matching the <c>DECIMAL</c> type the validator infers
        /// for the placeholders.
        /// </remarks>
        /// <param name="sql">A statement whose row counts are <c>?</c> placeholders.</param>
        /// <param name="counts">The decimal text of each placeholder's value, in placeholder order.</param>
        /// <returns>The first column of every row, with <c>NULL</c> for a null.</returns>
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
        /// A <c>FETCH</c> whose count is a parameter: the marker is wrapped in a cast to <c>INT</c>, so the
        /// server does not read the bound decimal directly.
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
        /// An <c>OFFSET</c> whose count is a parameter is cast the same way.
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
        /// A parameterised <c>OFFSET</c> and <c>FETCH</c> together, as paging uses them.
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
        /// A count bound as an integer gives the same answer; the cast has no effect on a whole number.
        /// </summary>
        [Fact]
        public void AnIntegerBoundFetchAnswers()
        {
            using var statement = _connection.prepareStatement("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST ? ROWS ONLY");
            statement.setInt(1, 2);

            Assert.Equal(new[] { "1", "2" }, Read(statement.executeQuery()));
        }

        /// <summary>
        /// A literal count with a fractional part is rounded up to a whole literal, with no cast.
        /// </summary>
        /// <remarks>
        /// SQL Server rejects <c>TOP (2.9)</c>. Rounding up matches the rows the in-process operator returns
        /// for the same count; <see cref="AFractionalCountMeansTheSameRowsInProcess"/> checks that.
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
        /// A whole literal count is written unchanged, so <c>TOP (2)</c> stays a constant the server can plan
        /// a row goal against.
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
        /// A fractional count selects the same rows pushed down as it does in process over a <c>VALUES</c>
        /// source. A literal count is rounded by the dialect and a parameter by the server's
        /// <c>CEILING</c>.
        /// </summary>
        /// <param name="sql">A statement over <c>EMPS</c> with a fractional offset or fetch count.</param>
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
        /// A literal count too large for an <c>int</c> is refused when the statement is written, with a
        /// message naming the clause and the count.
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
        /// The same count as a parameter is refused by the server when it is cast to <c>INT</c>.
        /// </summary>
        /// <remarks>
        /// The exception is the driver's own rather than a <c>java.sql.SQLException</c>: the server raises it
        /// on the first read, after <c>AdoEnumerable.enumerator</c>, which is where a driver's exception is
        /// wrapped.
        /// </remarks>
        [Fact]
        public void AParameterisedFetchWiderThanAnIntIsRefused()
        {
            var e = Assert.ThrowsAny<Microsoft.Data.SqlClient.SqlException>(
                () => Rows("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO FETCH FIRST ? ROWS ONLY", "3000000000"));

            Assert.Contains("Arithmetic overflow", Message(e));
        }

        /// <summary>
        /// A negative parameterised count is refused both pushed down, by SQL Server, and in process, by
        /// Calcite.
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
        /// Returns the messages of an exception and all its inner exceptions, one per line, since the
        /// driver's message is usually several wrappers down.
        /// </summary>
        /// <param name="exception">The outermost exception.</param>
        /// <returns>Each message in the chain, outermost first, each followed by a line break.</returns>
        static string Message(Exception exception)
        {
            var text = new System.Text.StringBuilder();
            for (Exception? e = exception; e is not null; e = e.InnerException)
                text.AppendLine(e.Message);

            return text.ToString();
        }

    }

}
