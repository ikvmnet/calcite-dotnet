using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;

using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers a query travelling the whole way: parsed by Calcite, planned into the
    /// <see cref="AdoConvention"/>, executed against the provider, and read back.
    /// </summary>
    /// <remarks>
    /// This is the only thing that exercises the converter rules, the relational factories and the row
    /// builder together, and the only place a mistake in any of them shows up as a wrong answer rather than
    /// a plan that merely looks reasonable.
    /// </remarks>
    [TestClass]
    public class AdoQueryTests
    {

        static AdoQueryTests()
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
        /// Runs a query and returns its rows as strings, so a comparison does not depend on which numeric
        /// type a provider chose.
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

        /// <summary>
        /// Runs a query and returns the single value it produces.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        string Scalar(string sql)
        {
            var rows = Rows(sql);
            Assert.AreEqual(1, rows.Count, $"expected one row from: {sql}");
            return rows[0];
        }

        #region Scan and project

        [TestMethod]
        public void EveryRowIsReturned()
        {
            Assert.AreEqual(5, Rows("SELECT * FROM ADO.EMPS").Count);
        }

        [TestMethod]
        public void ColumnsAreProjected()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob", "Carol", "Dave", "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS"));
        }

        [TestMethod]
        public void AProjectionCanComputeAValue()
        {
            Assert.AreEqual("2", Scalar("SELECT EMPNO + 1 FROM ADO.EMPS WHERE EMPNO = 1"));
        }

        #endregion

        #region Filter

        [TestMethod]
        public void AFilterRestrictsTheRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice", "Bob" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO = 10"));
        }

        [TestMethod]
        public void AFilterCanCombineConditions()
        {
            Assert.AreEqual("Bob", Scalar("SELECT NAME FROM ADO.EMPS WHERE DEPTNO = 10 AND EMPNO > 1"));
        }

        [TestMethod]
        public void AFilterSeesNullAsUnknown()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO IS NULL"));
        }

        [TestMethod]
        public void AFilterMatchingNothingReturnsNothing()
        {
            Assert.AreEqual(0, Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO = 999").Count);
        }

        #endregion

        #region Sort

        [TestMethod]
        public void OrderByOrdersTheRows()
        {
            CollectionAssert.AreEqual(
                new[] { "Alice", "Bob", "Carol", "Dave", "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS ORDER BY NAME"));
        }

        [TestMethod]
        public void OrderByDescendingReverses()
        {
            CollectionAssert.AreEqual(
                new[] { "Erin", "Dave", "Carol", "Bob", "Alice" },
                Rows("SELECT NAME FROM ADO.EMPS ORDER BY NAME DESC"));
        }

        [TestMethod]
        public void LimitTakesThePrefix()
        {
            CollectionAssert.AreEqual(
                new[] { "Alice", "Bob" },
                Rows("SELECT NAME FROM ADO.EMPS ORDER BY NAME LIMIT 2"));
        }

        #endregion

        #region Aggregate

        [TestMethod]
        public void CountCountsTheRows()
        {
            Assert.AreEqual("5", Scalar("SELECT COUNT(*) FROM ADO.EMPS"));
        }

        [TestMethod]
        public void GroupByGroupsTheRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "10|2", "20|2", "NULL|1" },
                Rows("SELECT DEPTNO, COUNT(*) FROM ADO.EMPS GROUP BY DEPTNO"));
        }

        /// <summary>
        /// An aggregate skips nulls rather than counting them, which is the SQL rule and not the obvious
        /// one to get right by accident.
        /// </summary>
        [TestMethod]
        public void CountOfAColumnSkipsNulls()
        {
            Assert.AreEqual("4", Scalar("SELECT COUNT(SALARY) FROM ADO.EMPS"));
        }

        [TestMethod]
        public void MinAndMaxAgree()
        {
            Assert.AreEqual("1|5", Scalar("SELECT MIN(EMPNO), MAX(EMPNO) FROM ADO.EMPS"));
        }

        #endregion

        #region Join

        [TestMethod]
        public void AnInnerJoinMatchesOnTheKey()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering" },
                Rows("SELECT E.NAME, D.DNAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO"));
        }

        /// <summary>
        /// A null key joins to nothing: two unknowns are not equal in SQL, however tempting a hash lookup
        /// makes it.
        /// </summary>
        [TestMethod]
        public void ANullKeyJoinsToNothing()
        {
            var rows = Rows("SELECT E.NAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO");
            CollectionAssert.DoesNotContain(rows, "Erin");
        }

        [TestMethod]
        public void ALeftJoinKeepsUnmatchedRows()
        {
            var rows = Rows("SELECT E.NAME, D.DNAME FROM ADO.EMPS E LEFT JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO");

            Assert.AreEqual(5, rows.Count);
            CollectionAssert.Contains(rows, "Erin|NULL");
        }

        [TestMethod]
        public void ADepartmentWithNoEmployeesIsAbsentFromAnInnerJoin()
        {
            var rows = Rows("SELECT D.DNAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO");
            CollectionAssert.DoesNotContain(rows, "Empty");
        }

        #endregion

        #region Set operations

        [TestMethod]
        public void UnionAllConcatenates()
        {
            Assert.AreEqual(8, Rows("SELECT DEPTNO FROM ADO.EMPS UNION ALL SELECT DEPTNO FROM ADO.DEPTS").Count);
        }

        [TestMethod]
        public void UnionRemovesDuplicates()
        {
            CollectionAssert.AreEquivalent(
                new[] { "10", "20", "30", "NULL" },
                Rows("SELECT DEPTNO FROM ADO.EMPS UNION SELECT DEPTNO FROM ADO.DEPTS"));
        }

        [TestMethod]
        public void IntersectKeepsWhatIsInBoth()
        {
            CollectionAssert.AreEquivalent(
                new[] { "10", "20" },
                Rows("SELECT DEPTNO FROM ADO.EMPS INTERSECT SELECT DEPTNO FROM ADO.DEPTS"));
        }

        [TestMethod]
        public void ExceptRemovesWhatIsInTheSecond()
        {
            CollectionAssert.AreEquivalent(
                new[] { "30" },
                Rows("SELECT DEPTNO FROM ADO.DEPTS EXCEPT SELECT DEPTNO FROM ADO.EMPS"));
        }

        #endregion

        #region Values and literals

        [TestMethod]
        public void AQueryOverLiteralsNeedsNoTable()
        {
            Assert.AreEqual("1|two", Scalar("SELECT * FROM (VALUES (1, 'two'))"));
        }

        #endregion

        #region Dates

        /// <summary>
        /// The end of the chain the DATE representation runs through: read from the provider as a day count,
        /// carried through the plan, and decoded back to a date.
        /// </summary>
        [TestMethod]
        public void ADateComesBackAsTheDateThatWasStored()
        {
            Assert.AreEqual("2020-01-15", Scalar("SELECT HIREDATE FROM ADO.EMPS WHERE EMPNO = 1"));
        }

        [TestMethod]
        public void DatesOrderChronologically()
        {
            CollectionAssert.AreEqual(
                new[] { "2019-11-01", "2020-01-15", "2021-06-30", "2022-03-15" },
                Rows("SELECT HIREDATE FROM ADO.EMPS WHERE HIREDATE IS NOT NULL ORDER BY HIREDATE"));
        }

        #endregion

    }

}
