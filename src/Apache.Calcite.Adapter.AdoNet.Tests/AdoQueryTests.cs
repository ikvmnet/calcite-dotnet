using System;
using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using org.apache.calcite.jdbc;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests queries end to end over SQLite through Calcite's JDBC driver: parsed, planned into the
    /// <see cref="AdoConvention"/>, executed against the provider, and read back.
    /// </summary>
    public class AdoQueryTests : IDisposable
    {

        static AdoQueryTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        SqliteFixture _sqlite = null!;
        java.sql.Connection _connection = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public AdoQueryTests()
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

        /// <inheritdoc />
        public void Dispose()
        {
            _connection?.close();
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Runs a query and returns each row's values as strings joined by a pipe, so a comparison does not
        /// depend on which numeric type a provider chose.
        /// </summary>
        /// <param name="sql">The statement to run through Calcite against the SQLite database.</param>
        /// <returns>One string per row, its values joined by a pipe with <c>NULL</c> for a null.</returns>
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
        /// Runs a query that must return one row and returns that row as <see cref="Rows"/> formats it.
        /// </summary>
        /// <param name="sql">A statement expected to produce exactly one row.</param>
        /// <returns>The single row, its values joined by a pipe.</returns>
        string Scalar(string sql)
        {
            var rows = Rows(sql);
            rows.Count.Should().Be(1, $"expected one row from: {sql}");
            return rows[0];
        }

        #region Scan and project

        [Fact]
        public void EveryRowIsReturned()
        {
            Assert.Equal(5, Rows("SELECT * FROM ADO.EMPS").Count);
        }

        [Fact]
        public void ColumnsAreProjected()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol", "Dave", "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS"), strict: true);
        }

        [Fact]
        public void AProjectionCanComputeAValue()
        {
            Assert.Equal("2", Scalar("SELECT EMPNO + 1 FROM ADO.EMPS WHERE EMPNO = 1"));
        }

        #endregion

        #region Filter

        [Fact]
        public void AFilterRestrictsTheRows()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO = 10"), strict: true);
        }

        [Fact]
        public void AFilterCanCombineConditions()
        {
            Assert.Equal("Bob", Scalar("SELECT NAME FROM ADO.EMPS WHERE DEPTNO = 10 AND EMPNO > 1"));
        }

        [Fact]
        public void AFilterSeesNullAsUnknown()
        {
            Assert.Equivalent(
                new[] { "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO IS NULL"), strict: true);
        }

        [Fact]
        public void AFilterMatchingNothingReturnsNothing()
        {
            Assert.Empty(Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO = 999"));
        }

        #endregion

        #region Sort

        [Fact]
        public void OrderByOrdersTheRows()
        {
            Assert.Equal(
                new[] { "Alice", "Bob", "Carol", "Dave", "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS ORDER BY NAME"));
        }

        [Fact]
        public void OrderByDescendingReverses()
        {
            Assert.Equal(
                new[] { "Erin", "Dave", "Carol", "Bob", "Alice" },
                Rows("SELECT NAME FROM ADO.EMPS ORDER BY NAME DESC"));
        }

        [Fact]
        public void LimitTakesThePrefix()
        {
            Assert.Equal(
                new[] { "Alice", "Bob" },
                Rows("SELECT NAME FROM ADO.EMPS ORDER BY NAME LIMIT 2"));
        }

        #endregion

        #region Aggregate

        [Fact]
        public void CountCountsTheRows()
        {
            Assert.Equal("5", Scalar("SELECT COUNT(*) FROM ADO.EMPS"));
        }

        [Fact]
        public void GroupByGroupsTheRows()
        {
            Assert.Equivalent(
                new[] { "10|2", "20|2", "NULL|1" },
                Rows("SELECT DEPTNO, COUNT(*) FROM ADO.EMPS GROUP BY DEPTNO"), strict: true);
        }

        /// <summary>
        /// <c>COUNT</c> of a column skips nulls.
        /// </summary>
        [Fact]
        public void CountOfAColumnSkipsNulls()
        {
            Assert.Equal("4", Scalar("SELECT COUNT(SALARY) FROM ADO.EMPS"));
        }

        [Fact]
        public void MinAndMaxAgree()
        {
            Assert.Equal("1|5", Scalar("SELECT MIN(EMPNO), MAX(EMPNO) FROM ADO.EMPS"));
        }

        #endregion

        #region Join

        [Fact]
        public void AnInnerJoinMatchesOnTheKey()
        {
            Assert.Equivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering" },
                Rows("SELECT E.NAME, D.DNAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO"), strict: true);
        }

        /// <summary>
        /// A null key joins to nothing, since null is not equal to null in SQL.
        /// </summary>
        [Fact]
        public void ANullKeyJoinsToNothing()
        {
            var rows = Rows("SELECT E.NAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO");
            Assert.DoesNotContain("Erin", rows);
        }

        [Fact]
        public void ALeftJoinKeepsUnmatchedRows()
        {
            var rows = Rows("SELECT E.NAME, D.DNAME FROM ADO.EMPS E LEFT JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO");

            Assert.Equal(5, rows.Count);
            Assert.Contains("Erin|NULL", rows);
        }

        [Fact]
        public void ADepartmentWithNoEmployeesIsAbsentFromAnInnerJoin()
        {
            var rows = Rows("SELECT D.DNAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO");
            Assert.DoesNotContain("Empty", rows);
        }

        #endregion

        #region Set operations

        [Fact]
        public void UnionAllConcatenates()
        {
            Assert.Equal(8, Rows("SELECT DEPTNO FROM ADO.EMPS UNION ALL SELECT DEPTNO FROM ADO.DEPTS").Count);
        }

        [Fact]
        public void UnionRemovesDuplicates()
        {
            Assert.Equivalent(
                new[] { "10", "20", "30", "NULL" },
                Rows("SELECT DEPTNO FROM ADO.EMPS UNION SELECT DEPTNO FROM ADO.DEPTS"), strict: true);
        }

        [Fact]
        public void IntersectKeepsWhatIsInBoth()
        {
            Assert.Equivalent(
                new[] { "10", "20" },
                Rows("SELECT DEPTNO FROM ADO.EMPS INTERSECT SELECT DEPTNO FROM ADO.DEPTS"), strict: true);
        }

        [Fact]
        public void ExceptRemovesWhatIsInTheSecond()
        {
            Assert.Equivalent(
                new[] { "30" },
                Rows("SELECT DEPTNO FROM ADO.DEPTS EXCEPT SELECT DEPTNO FROM ADO.EMPS"), strict: true);
        }

        #endregion

        #region Values and literals

        [Fact]
        public void AQueryOverLiteralsNeedsNoTable()
        {
            Assert.Equal("1|two", Scalar("SELECT * FROM (VALUES (1, 'two'))"));
        }

        #endregion

        #region Dates

        /// <summary>
        /// A <c>DATE</c> is read from the provider as a day count, carried through the plan, and returned as the
        /// date that was stored.
        /// </summary>
        [Fact]
        public void ADateComesBackAsTheDateThatWasStored()
        {
            Assert.Equal("2020-01-15", Scalar("SELECT HIREDATE FROM ADO.EMPS WHERE EMPNO = 1"));
        }

        [Fact]
        public void DatesOrderChronologically()
        {
            Assert.Equal(
                new[] { "2019-11-01", "2020-01-15", "2021-06-30", "2022-03-15" },
                Rows("SELECT HIREDATE FROM ADO.EMPS WHERE HIREDATE IS NOT NULL ORDER BY HIREDATE"));
        }

        #endregion

    }

}
