using System;
using System.Collections.Generic;
using System.Data.Common;

using org.apache.calcite.jdbc;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests correlated sub-queries against SQL Server through SqlClient, ODBC and OLE DB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ODBC and OLE DB write every parameter marker as a bare <c>?</c>, so parameters are matched by the order
    /// they were added in. SqlClient names them <c>@P0</c>, <c>@P1</c> and so on, so it serves as a control:
    /// a failure on all three drivers is not a positional binding fault.
    /// </para>
    /// <para>
    /// The connections set <c>forceDecorrelate=false</c>, because otherwise Calcite rewrites the correlation
    /// into a join and no parameter is bound.
    /// </para>
    /// <para>
    /// All run against the shared LocalDB database, and skip where the driver is not installed.
    /// </para>
    /// </remarks>
    public class GenericProviderCorrelationTests
    {

        const string SqlClient = "sqlclient";
        const string Odbc = "odbc";
        const string OleDb = "oledb";

        static GenericProviderCorrelationTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public GenericProviderCorrelationTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// Returns the data source for a provider, skipping the test where it is not installed.
        /// </summary>
        /// <param name="provider">The provider name: one of the <c>SqlClient</c>, <c>Odbc</c> or <c>OleDb</c>
        /// constants.</param>
        /// <returns>The fixture's data source for that provider, connected to the shared test database.</returns>
        static DbDataSource DataSourceFor(string provider)
        {
            switch (provider)
            {
                case SqlClient:
                    return SqlServerFixture.Shared.DataSource;
                case Odbc:
                    if (SqlServerFixture.OdbcDriver is null)
                        Assert.Skip("No SQL Server ODBC driver is installed on this machine.");

                    return SqlServerFixture.Shared.OdbcDataSource;
                case OleDb:
                    if (SqlServerFixture.OleDbProvider is null)
                        Assert.Skip("No SQL Server OLE DB provider is registered for this process architecture.");

                    return SqlServerFixture.Shared.OleDbDataSource;
                default:
                    throw new ArgumentException($"unknown provider {provider}", nameof(provider));
            }
        }

        /// <summary>
        /// Runs a query on a connection that leaves the correlate in the plan, returning each row's values
        /// joined by a pipe.
        /// </summary>
        /// <param name="provider">The provider to reach SQL Server through, as <see cref="DataSourceFor"/>
        /// takes it.</param>
        /// <param name="sql">The statement to run, whose correlated sub-queries stay correlates in the
        /// plan.</param>
        /// <returns>One string per row, its values joined by a pipe with <c>NULL</c> for a null.</returns>
        static List<string> CorrelatedRows(string provider, string sql)
        {
            var dataSource = DataSourceFor(provider);

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");
            properties.setProperty("forceDecorrelate", "false");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);
            var root = ((CalciteConnection)connection).getRootSchema();
            root.add("ADO", AdoSchema.Create(root, "ADO", dataSource, null, "dbo"));

            using var statement = connection.createStatement();
            var results = statement.executeQuery(sql);

            var rows = new List<string>();
            var columns = results.getMetaData().getColumnCount();

            while (results.next())
            {
                var values = new string[columns];
                for (int i = 0; i < columns; i++)
                    values[i] = results.getObject(i + 1)?.ToString() ?? "NULL";

                rows.Add(string.Join("|", values));
            }

            return rows;
        }

        /// <summary>
        /// A correlated sub-query with one parameter.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ACorrelatedExistsBindsItsParameter(string provider)
        {
            Assert.Equivalent(
                new[] { "Sales", "Engineering" },
                CorrelatedRows(provider, "SELECT D.DNAME FROM ADO.DEPTS D WHERE EXISTS (SELECT 1 FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO)"), strict: true);
        }

        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ACorrelatedScalarSubQueryYieldsAValuePerRow(string provider)
        {
            Assert.Equivalent(
                new[] { "Sales|2", "Engineering|2", "Empty|0" },
                CorrelatedRows(provider, "SELECT D.DNAME, (SELECT COUNT(*) FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO) FROM ADO.DEPTS D"), strict: true);
        }

        /// <summary>
        /// Two parameters of different types in one statement, which positional markers must bind in order.
        /// Swapping them compares a department against a salary and gives a wrong answer rather than an error.
        /// Only Alice has a colleague in her own department earning more.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void TwoCorrelationVariablesAreBoundInOrder(string provider)
        {
            Assert.Equivalent(
                new[] { "Alice" },
                CorrelatedRows(provider, """
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO AND E2.SALARY > E.SALARY)
                    """), strict: true);
        }

        /// <summary>
        /// One variable read twice is two markers carrying one value, so there are more markers than
        /// variables.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void OneVariableReadTwiceFillsBothParameters(string provider)
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol" },
                CorrelatedRows(provider, """
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.EMPNO > E.EMPNO AND E2.EMPNO <= E.EMPNO + 2)
                    """), strict: true);
        }

        /// <summary>
        /// Correlating on a <c>DECIMAL</c>, which leaves the plan as a <c>java.math.BigDecimal</c> and is bound
        /// as a <see cref="decimal"/>.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void CorrelatingOnADecimal(string provider)
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob" },
                CorrelatedRows(provider, "SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.SALARY > E.SALARY)"), strict: true);
        }

        /// <summary>
        /// Correlating on a character column: everyone but the last name in order has one after them.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void CorrelatingOnAString(string provider)
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol" },
                CorrelatedRows(provider, "SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.NAME > E.NAME)"), strict: true);
        }

        /// <summary>
        /// A null correlation value is bound rather than skipped: Dave's salary is null, so his marker is bound
        /// to <see cref="DBNull"/>, the inner comparison is unknown for every row, and his count is zero.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void ANullCorrelationValueIsBound(string provider)
        {
            Assert.Equivalent(
                new[] { "Alice|1", "Bob|1", "Carol|1", "Dave|0" },
                CorrelatedRows(provider, "SELECT E.NAME, (SELECT COUNT(*) FROM ADO.EMPS E2 WHERE E2.SALARY = E.SALARY) FROM ADO.EMPS E"), strict: true);
        }

        /// <summary>
        /// The conversions <c>AdoEnumerable.ToProviderValue</c> performs from a plan value to a parameter, each
        /// reached by correlating on a column of that type.
        /// </summary>
        /// <remarks>
        /// A correlation value leaves the plan as a boxed Java type such as <c>java.lang.Boolean</c>,
        /// <c>java.lang.Double</c> or Avatica's <c>ByteString</c>. In <c>TYPES</c> the populated row matches
        /// itself and the row of nulls matches nothing, so every column gives the same answer.
        /// </remarks>
        /// <param name="provider">The provider the correlation value is bound through.</param>
        /// <param name="columnName">The column of <c>TYPES</c> correlated on, one per SQL Server type
        /// tested.</param>
        [Theory]
        [InlineData(SqlClient, "C_BIT")]
        [InlineData(Odbc, "C_BIT")]
        [InlineData(OleDb, "C_BIT")]
        [InlineData(SqlClient, "C_FLOAT")]
        [InlineData(Odbc, "C_FLOAT")]
        [InlineData(OleDb, "C_FLOAT")]
        [InlineData(SqlClient, "C_VARBINARY")]
        [InlineData(Odbc, "C_VARBINARY")]
        [InlineData(OleDb, "C_VARBINARY")]
        [InlineData(SqlClient, "C_BIGINT")]
        [InlineData(Odbc, "C_BIGINT")]
        [InlineData(OleDb, "C_BIGINT")]
        // SQL Server's tinyint is unsigned and leaves the plan as a joou UByte
        [InlineData(SqlClient, "C_TINYINT")]
        [InlineData(Odbc, "C_TINYINT")]
        [InlineData(OleDb, "C_TINYINT")]
        // the temporal types leave the plan as counts (a DATE as days since the epoch, a TIMESTAMP as
        // milliseconds), which must be converted back before binding against a typed column
        [InlineData(SqlClient, "C_DATE")]
        [InlineData(Odbc, "C_DATE")]
        [InlineData(OleDb, "C_DATE")]
        [InlineData(SqlClient, "C_DATETIME2")]
        [InlineData(Odbc, "C_DATETIME2")]
        [InlineData(OleDb, "C_DATETIME2")]
        // SqlClient only: System.Data.Odbc cannot read a time or datetimeoffset column
        // (TheDriverCannotReadSqlServersOwnTimeTypes), and OLE DB has the limitations the next two tests show
        [InlineData(SqlClient, "C_TIME")]
        [InlineData(SqlClient, "C_DATETIMEOFFSET")]
        public void CorrelatingOnAColumnConvertsItsValueForTheProvider(string provider, string columnName)
        {
            Assert.Equal(
                new[] { "1" },
                CorrelatedRows(provider, $"""
                    SELECT T.ID FROM ADO.TYPES T
                    WHERE EXISTS (SELECT 1 FROM ADO.TYPES T2 WHERE T2.{columnName} = T.{columnName})
                    """));
        }

        /// <summary>
        /// Correlating on a signed <c>TINYINT</c>, which carries its sign to the provider.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The outer row is a <c>VALUES</c> because SQL Server's <c>tinyint</c> is unsigned and maps to
        /// <c>UTINYINT</c>. Calcite's <c>TINYINT</c> is signed and held in a <c>java.lang.Byte</c>, whose
        /// <c>byteValue()</c> under IKVM returns an unsigned CLR <see cref="byte"/>, so -56 would bind as 200.
        /// </para>
        /// <para>
        /// The server compares against <c>ID</c>, whose values are 1 and 2: -56 is below both and 200 above
        /// both, so one row means the sign survived. <c>C_TINYINT</c> is not used as the other side because
        /// ordering a <c>TINYINT</c> against a <c>UTINYINT</c> casts to an unsigned type, which the SQL Server
        /// dialect writes as a bare <c>UNSIGNED</c> that T-SQL does not parse.
        /// </para>
        /// </remarks>
        /// <param name="provider">The provider the negative value is bound through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void CorrelatingOnASignedTinyIntKeepsItsSign(string provider)
        {
            Assert.Equal(
                new[] { "-56" },
                CorrelatedRows(provider, """
                    SELECT V.X FROM (VALUES (CAST(-56 AS TINYINT))) AS V(X)
                    WHERE EXISTS (SELECT 1 FROM ADO.TYPES T WHERE V.X < T.ID)
                    """));
        }

        /// <summary>
        /// Correlating across a cast to <c>UUID</c>, which puts a cast into the generated SQL as well as a UUID
        /// value into the parameter.
        /// </summary>
        /// <remarks>
        /// A <c>uniqueidentifier</c> reaches Calcite as a <c>CHAR(36)</c>, so GUID semantics need an explicit
        /// cast. T-SQL has no <c>UUID</c> type, so the SQL Server dialect writes the cast target as
        /// <c>UNIQUEIDENTIFIER</c>.
        /// </remarks>
        /// <param name="provider">The provider the UUID value is bound through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void CorrelatingAcrossAUuidCastRunsOnTheServer(string provider)
        {
            Assert.Equal(
                new[] { "1" },
                CorrelatedRows(provider, """
                    SELECT T.ID FROM ADO.TYPES T
                    WHERE EXISTS (SELECT 1 FROM ADO.TYPES T2 WHERE CAST(T2.C_GUID AS UUID) = CAST(T.C_GUID AS UUID))
                    """));
        }

        /// <summary>
        /// A driver limitation: <c>System.Data.OleDb</c> cannot marshal a <see cref="DateTimeOffset"/> to a
        /// Variant, so a zoned timestamp cannot be a parameter through OLE DB. It fails on the client.
        /// </summary>
        [Fact]
        public void TheOleDbDriverCannotBindAZonedTimestamp()
        {
            var thrown = Assert.ThrowsAny<NotSupportedException>(() => CorrelatedRows(OleDb, """
                SELECT T.ID FROM ADO.TYPES T
                WHERE EXISTS (SELECT 1 FROM ADO.TYPES T2 WHERE T2.C_DATETIMEOFFSET = T.C_DATETIMEOFFSET)
                """));

            Assert.Contains("Variant", thrown.Message);
        }

        /// <summary>
        /// A driver limitation that gives a wrong answer rather than an error: <c>System.Data.OleDb</c> binds a
        /// <see cref="TimeSpan"/> through OLE DB's <c>DBTIME</c> structure, which has no fractional seconds, so
        /// <c>01:02:03.500</c> reaches the server as <c>01:02:03</c> and an equality against the fractional
        /// <c>time</c> column matches nothing.
        /// </summary>
        [Fact]
        public void TheOleDbDriverTruncatesABoundTimeToWholeSeconds()
        {
            Assert.Empty(CorrelatedRows(OleDb, """
                SELECT T.ID FROM ADO.TYPES T
                WHERE EXISTS (SELECT 1 FROM ADO.TYPES T2 WHERE T2.C_TIME = T.C_TIME)
                """));
        }

        /// <summary>
        /// A correlated sub-query inside a correlated sub-query: two correlation contexts are live at once,
        /// each over a different outer row.
        /// </summary>
        /// <param name="provider">The provider the query reaches SQL Server through.</param>
        [Theory]
        [InlineData(SqlClient)]
        [InlineData(Odbc)]
        [InlineData(OleDb)]
        public void NestedCorrelation(string provider)
        {
            Assert.Equivalent(
                new[] { "Sales", "Engineering" },
                CorrelatedRows(provider, """
                    SELECT D.DNAME FROM ADO.DEPTS D
                    WHERE EXISTS (
                        SELECT 1 FROM ADO.EMPS E
                        WHERE E.DEPTNO = D.DEPTNO
                          AND EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO AND E2.EMPNO <> E.EMPNO))
                    """), strict: true);
        }

    }

}
