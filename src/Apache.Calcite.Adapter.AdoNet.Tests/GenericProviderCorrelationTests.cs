using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;

using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers correlated sub-queries against SQL Server through each of the three drivers that reach it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ODBC and OLE DB write the parameter marker as a bare <c>?</c>, so every parameter in a statement has
    /// the same name and the only thing distinguishing them is the order they were added in. SqlClient names
    /// them <c>@P0</c>, so a mistake in the ordering shows up there as an unfilled parameter and in the
    /// other two as the wrong answer — which is why SqlClient is here too, as the control.
    /// </para>
    /// <para>
    /// Nothing reaches a parameter but a correlated sub-query, and nothing reaches one of those but
    /// <c>forceDecorrelate=false</c>: Calcite rewrites a correlation into a join wherever it can, and by
    /// default it always tries.
    /// </para>
    /// <para>
    /// Every one of these failed identically on all three drivers before <c>AdoSqlDialects</c> told the
    /// dialect that SQL Server cannot group by a constant — not a parameter fault at all, and not one the
    /// generic providers introduced. Keeping SqlClient in the table is what said so.
    /// </para>
    /// <para>
    /// All three run against the shared LocalDB database, and skip where the driver is not installed.
    /// </para>
    /// </remarks>
    [TestClass]
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

        [TestInitialize]
        public void Setup()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Inconclusive("No SQL Server LocalDB instance is reachable on this machine.");
        }

        /// <summary>
        /// Returns the data source for a provider, skipping the test where it is not installed.
        /// </summary>
        /// <param name="provider"></param>
        /// <returns></returns>
        static DbDataSource DataSourceFor(string provider)
        {
            switch (provider)
            {
                case SqlClient:
                    return SqlServerFixture.Shared.DataSource;
                case Odbc:
                    if (SqlServerFixture.OdbcDriver is null)
                        Assert.Inconclusive("No SQL Server ODBC driver is installed on this machine.");

                    return SqlServerFixture.Shared.OdbcDataSource;
                case OleDb:
                    if (SqlServerFixture.OleDbProvider is null)
                        Assert.Inconclusive("No SQL Server OLE DB provider is registered for this process architecture.");

                    return SqlServerFixture.Shared.OleDbDataSource;
                default:
                    throw new ArgumentException($"unknown provider {provider}", nameof(provider));
            }
        }

        /// <summary>
        /// Runs a query on a connection that leaves the correlate in the plan.
        /// </summary>
        /// <param name="provider"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
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
        /// One parameter, which is the least that has to work before any of the rest can.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ACorrelatedExistsBindsItsParameter(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Sales", "Engineering" },
                CorrelatedRows(provider, "SELECT D.DNAME FROM ADO.DEPTS D WHERE EXISTS (SELECT 1 FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO)"));
        }

        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ACorrelatedScalarSubQueryYieldsAValuePerRow(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Sales|2", "Engineering|2", "Empty|0" },
                CorrelatedRows(provider, "SELECT D.DNAME, (SELECT COUNT(*) FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO) FROM ADO.DEPTS D"));
        }

        /// <summary>
        /// The case a positional marker can get wrong and a named one cannot: two parameters of different
        /// types in one statement. Swapping them compares a department against a salary, which gives a
        /// different answer rather than an error. Only Alice has a colleague in her own department earning
        /// more.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void TwoCorrelationVariablesAreBoundInOrder(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice" },
                CorrelatedRows(provider, """
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO AND E2.SALARY > E.SALARY)
                    """));
        }

        /// <summary>
        /// One variable read twice is two markers carrying one value, so the count of markers and the count
        /// of variables are not the same number.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void OneVariableReadTwiceFillsBothParameters(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob", "Carol" },
                CorrelatedRows(provider, """
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.EMPNO > E.EMPNO AND E2.EMPNO <= E.EMPNO + 2)
                    """));
        }

        /// <summary>
        /// Correlating on a <c>DECIMAL</c>, which leaves the plan as a <c>java.math.BigDecimal</c> and has to
        /// reach the parameter as something the driver will bind.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void CorrelatingOnADecimal(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob" },
                CorrelatedRows(provider, "SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.SALARY > E.SALARY)"));
        }

        /// <summary>
        /// Correlating on a character column: everyone but the last name in order has one after them.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void CorrelatingOnAString(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob", "Carol" },
                CorrelatedRows(provider, "SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.NAME > E.NAME)"));
        }

        /// <summary>
        /// A null correlation value is bound rather than skipped: Dave's salary is null, so his marker takes
        /// <see cref="DBNull"/> and the inner comparison is unknown for every row, giving zero rather than
        /// an unfilled parameter.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void ANullCorrelationValueIsBound(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|1", "Bob|1", "Carol|1", "Dave|0" },
                CorrelatedRows(provider, "SELECT E.NAME, (SELECT COUNT(*) FROM ADO.EMPS E2 WHERE E2.SALARY = E.SALARY) FROM ADO.EMPS E"));
        }

        /// <summary>
        /// The remaining conversions <c>AdoEnumerable.ToProviderValue</c> performs on the way out of the
        /// plan and into a parameter, each reached by correlating on a column of that type.
        /// </summary>
        /// <remarks>
        /// A correlation value leaves the plan as the boxed Java type Calcite holds it in — a
        /// <c>java.lang.Boolean</c>, a <c>java.lang.Double</c>, an <c>avatica ByteString</c> — and no
        /// provider knows what any of those are. The row of <c>TYPES</c> that holds values matches itself
        /// and the row of nulls matches nothing, so the answer is the same shape whichever column is used
        /// and a conversion that did not happen shows up as an exception rather than as a wrong row.
        /// </remarks>
        /// <param name="provider"></param>
        /// <param name="columnName"></param>
        [TestMethod]
        [DataRow(SqlClient, "C_BIT")]
        [DataRow(Odbc, "C_BIT")]
        [DataRow(OleDb, "C_BIT")]
        [DataRow(SqlClient, "C_FLOAT")]
        [DataRow(Odbc, "C_FLOAT")]
        [DataRow(OleDb, "C_FLOAT")]
        [DataRow(SqlClient, "C_VARBINARY")]
        [DataRow(Odbc, "C_VARBINARY")]
        [DataRow(OleDb, "C_VARBINARY")]
        [DataRow(SqlClient, "C_BIGINT")]
        [DataRow(Odbc, "C_BIGINT")]
        [DataRow(OleDb, "C_BIGINT")]
        // the one unsigned type a real backend reports: a tinyint leaves the plan as a joou UByte
        [DataRow(SqlClient, "C_TINYINT")]
        [DataRow(Odbc, "C_TINYINT")]
        [DataRow(OleDb, "C_TINYINT")]
        public void CorrelatingOnAColumnConvertsItsValueForTheProvider(string provider, string columnName)
        {
            CollectionAssert.AreEqual(
                new[] { "1" },
                CorrelatedRows(provider, $"""
                    SELECT T.ID FROM ADO.TYPES T
                    WHERE EXISTS (SELECT 1 FROM ADO.TYPES T2 WHERE T2.{columnName} = T.{columnName})
                    """));
        }

        /// <summary>
        /// A correlated sub-query inside a correlated sub-query: two contexts live at once, each closed over
        /// a different outer row, and both filling markers in the same statement.
        /// </summary>
        [TestMethod]
        [DataRow(SqlClient)]
        [DataRow(Odbc)]
        [DataRow(OleDb)]
        public void NestedCorrelation(string provider)
        {
            CollectionAssert.AreEquivalent(
                new[] { "Sales", "Engineering" },
                CorrelatedRows(provider, """
                    SELECT D.DNAME FROM ADO.DEPTS D
                    WHERE EXISTS (
                        SELECT 1 FROM ADO.EMPS E
                        WHERE E.DEPTNO = D.DEPTNO
                          AND EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO AND E2.EMPNO <> E.EMPNO))
                    """));
        }

    }

}
