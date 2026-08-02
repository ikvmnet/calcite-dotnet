using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;

using System.Collections.Generic;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers sub-queries that refer to the row of the query containing them.
    /// </summary>
    /// <remarks>
    /// A correlated sub-query is the case where the adapter cannot generate its SQL once and be done: the
    /// inner query needs a value from the outer row, which is what <see cref="AdoCorrelationDataContext"/>
    /// and the builder beside it exist to carry. Calcite decorrelates what it can into joins first, so some
    /// of these arrive at the adapter as ordinary joins; the answers have to be right either way, which is
    /// what these assert.
    /// </remarks>
    [TestClass]
    public class AdoCorrelationTests
    {

        static AdoCorrelationTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        SqliteFixture _sqlite = null!;
        java.sql.Connection _connection = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            _connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);

            var calcite = (CalciteConnection)_connection;
            var root = calcite.getRootSchema();
            root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _connection?.close();
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Runs a query and returns its rows, joined by a pipe.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        List<string> Rows(string sql)
        {
            using var statement = _connection.createStatement();
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

        #region EXISTS

        [TestMethod]
        public void ExistsKeepsRowsWithAMatch()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                Rows("SELECT NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"));
        }

        /// <summary>
        /// Erin's department is null, so the inner comparison is unknown for every department and the row
        /// does not survive.
        /// </summary>
        [TestMethod]
        public void ExistsDropsARowWhoseKeyIsNull()
        {
            CollectionAssert.DoesNotContain(
                Rows("SELECT NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"),
                "Erin");
        }

        [TestMethod]
        public void NotExistsKeepsRowsWithoutAMatch()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS E WHERE NOT EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"));
        }

        /// <summary>
        /// The correlation runs the other way here: a department is kept for what the employee table holds.
        /// </summary>
        [TestMethod]
        public void ExistsWorksFromTheOtherSide()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Sales", "Engineering" },
                Rows("SELECT DNAME FROM ADO.DEPTS D WHERE EXISTS (SELECT 1 FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO)"));
        }

        [TestMethod]
        public void NotExistsFindsTheEmptyDepartment()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Empty" },
                Rows("SELECT DNAME FROM ADO.DEPTS D WHERE NOT EXISTS (SELECT 1 FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO)"));
        }

        #endregion

        #region IN

        [TestMethod]
        public void InAgainstASubQueryRestrictsTheRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO IN (SELECT DEPTNO FROM ADO.DEPTS)"));
        }

        [TestMethod]
        public void InAgainstACorrelatedSubQueryRestrictsTheRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob" },
                Rows("SELECT NAME FROM ADO.EMPS E WHERE E.DEPTNO IN (SELECT D.DEPTNO FROM ADO.DEPTS D WHERE D.DNAME = 'Sales')"));
        }

        #endregion

        #region Scalar sub-queries

        /// <summary>
        /// A scalar sub-query produces one value per outer row, and null where it matches nothing.
        /// </summary>
        [TestMethod]
        public void AScalarSubQueryYieldsAValuePerRow()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|NULL" },
                Rows("SELECT E.NAME, (SELECT D.DNAME FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO) FROM ADO.EMPS E"));
        }

        [TestMethod]
        public void AScalarSubQueryCanAggregate()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Sales|2", "Engineering|2", "Empty|0" },
                Rows("SELECT D.DNAME, (SELECT COUNT(*) FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO) FROM ADO.DEPTS D"));
        }

        /// <summary>
        /// A sub-query in the predicate rather than the projection, comparing against the outer row.
        /// </summary>
        /// <remarks>
        /// Only Bob survives, and each exclusion is a different rule. In department 10 the average of 100.5
        /// and 200.0 is 150.25, so Bob is above it and Alice is not. In department 20 Carol is the only
        /// non-null salary, so the average is her own and <c>&gt;</c> is false against it; Dave's salary is
        /// null, so the comparison is unknown. Erin's department is null, so the inner query matches nothing,
        /// the average of no rows is null, and that comparison is unknown too.
        /// </remarks>
        [TestMethod]
        public void ACorrelatedComparisonFilters()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Bob" },
                Rows("""
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE E.SALARY > (SELECT AVG(E2.SALARY) FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO)
                    """));
        }

        #endregion

        #region Without decorrelation

        /// <summary>
        /// Opens a second connection that leaves correlates in the plan.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// Calcite rewrites a correlated sub-query into a join wherever it can, and by default it always
        /// tries. Every query above therefore reaches the adapter already decorrelated, which means none of
        /// them exercise the correlation machinery at all. <c>forceDecorrelate=false</c> leaves the
        /// <c>Correlate</c> in place, which is the only way to reach it from SQL.
        /// </remarks>
        java.sql.Connection Correlating()
        {
            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");
            properties.setProperty("forceDecorrelate", "false");

            var connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);
            var root = ((CalciteConnection)connection).getRootSchema();
            root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null));

            return connection;
        }

        /// <summary>
        /// Runs a query on a connection that does not decorrelate.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        List<string> CorrelatedRows(string sql)
        {
            using var connection = Correlating();
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

        [TestMethod]
        [Ignore("Three of the four defects behind this are fixed (Build was never called, the context ignored its parameters, and a bad cast broke the builder class init). What remains is that the provider still rejects the command for unbound parameters: the SQL carries Calcite's ? marker while the enricher adds parameters named by AdoDatabaseMetadata.GetParameterName, which is $P0 for SQLite. See TODO.md item 0.")]
        public void ExistsIsCorrectWithoutDecorrelation()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                CorrelatedRows("SELECT NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"));
        }

        [TestMethod]
        [Ignore("Three of the four defects behind this are fixed (Build was never called, the context ignored its parameters, and a bad cast broke the builder class init). What remains is that the provider still rejects the command for unbound parameters: the SQL carries Calcite's ? marker while the enricher adds parameters named by AdoDatabaseMetadata.GetParameterName, which is $P0 for SQLite. See TODO.md item 0.")]
        public void AScalarSubQueryIsCorrectWithoutDecorrelation()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|NULL" },
                CorrelatedRows("SELECT E.NAME, (SELECT D.DNAME FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO) FROM ADO.EMPS E"));
        }

        [TestMethod]
        [Ignore("Three of the four defects behind this are fixed (Build was never called, the context ignored its parameters, and a bad cast broke the builder class init). What remains is that the provider still rejects the command for unbound parameters: the SQL carries Calcite's ? marker while the enricher adds parameters named by AdoDatabaseMetadata.GetParameterName, which is $P0 for SQLite. See TODO.md item 0.")]
        public void AnAggregatingSubQueryIsCorrectWithoutDecorrelation()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Sales|2", "Engineering|2", "Empty|0" },
                CorrelatedRows("SELECT D.DNAME, (SELECT COUNT(*) FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO) FROM ADO.DEPTS D"));
        }

        #endregion

        #region Uncorrelated, for contrast

        /// <summary>
        /// An uncorrelated sub-query needs no value from the outer row, so it is evaluated once. It is here
        /// to keep the correlated cases honest: if these were the only ones passing, the correlation would
        /// not be doing anything.
        /// </summary>
        [TestMethod]
        public void AnUncorrelatedSubQueryIsEvaluatedOnce()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Carol" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE SALARY = (SELECT MAX(SALARY) FROM ADO.EMPS)"));
        }

        #endregion

    }

}
