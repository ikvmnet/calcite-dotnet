using System;
using System.Collections.Generic;

using org.apache.calcite.jdbc;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers sub-queries that refer to the row of the query containing them.
    /// </summary>
    /// <remarks>
    /// The inner query of a correlated sub-query needs a value from the outer row, which
    /// <see cref="AdoCorrelationDataContext"/> carries into the pushed-down statement's parameters. By default
    /// Calcite decorrelates these into joins; the tests under <c>Without decorrelation</c> keep the correlate.
    /// </remarks>
    public class AdoCorrelationTests : IDisposable
    {

        static AdoCorrelationTests()
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
        public AdoCorrelationTests()
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
        /// Runs a query and returns each row's values joined by a pipe, with <c>NULL</c> for a null.
        /// </summary>
        /// <param name="sql">The statement to run on the default connection, which decorrelates.</param>
        /// <returns>One string per row, in the order the result set delivered them.</returns>
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

        [Fact]
        public void ExistsKeepsRowsWithAMatch()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                Rows("SELECT NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"), strict: true);
        }

        /// <summary>
        /// Erin's department is null, so the inner comparison is unknown for every department and the row
        /// does not survive.
        /// </summary>
        [Fact]
        public void ExistsDropsARowWhoseKeyIsNull()
        {
            Assert.DoesNotContain(
                "Erin",
                Rows("SELECT NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"));
        }

        [Fact]
        public void NotExistsKeepsRowsWithoutAMatch()
        {
            Assert.Equivalent(
                new[] { "Erin" },
                Rows("SELECT NAME FROM ADO.EMPS E WHERE NOT EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"), strict: true);
        }

        /// <summary>
        /// The correlation runs the other way here: a department is kept for what the employee table holds.
        /// </summary>
        [Fact]
        public void ExistsWorksFromTheOtherSide()
        {
            Assert.Equivalent(
                new[] { "Sales", "Engineering" },
                Rows("SELECT DNAME FROM ADO.DEPTS D WHERE EXISTS (SELECT 1 FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO)"), strict: true);
        }

        [Fact]
        public void NotExistsFindsTheEmptyDepartment()
        {
            Assert.Equivalent(
                new[] { "Empty" },
                Rows("SELECT DNAME FROM ADO.DEPTS D WHERE NOT EXISTS (SELECT 1 FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO)"), strict: true);
        }

        #endregion

        #region IN

        [Fact]
        public void InAgainstASubQueryRestrictsTheRows()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE DEPTNO IN (SELECT DEPTNO FROM ADO.DEPTS)"), strict: true);
        }

        [Fact]
        public void InAgainstACorrelatedSubQueryRestrictsTheRows()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob" },
                Rows("SELECT NAME FROM ADO.EMPS E WHERE E.DEPTNO IN (SELECT D.DEPTNO FROM ADO.DEPTS D WHERE D.DNAME = 'Sales')"), strict: true);
        }

        #endregion

        #region Scalar sub-queries

        /// <summary>
        /// A scalar sub-query produces one value per outer row, and null where it matches nothing.
        /// </summary>
        [Fact]
        public void AScalarSubQueryYieldsAValuePerRow()
        {
            Assert.Equivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|NULL" },
                Rows("SELECT E.NAME, (SELECT D.DNAME FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO) FROM ADO.EMPS E"), strict: true);
        }

        [Fact]
        public void AScalarSubQueryCanAggregate()
        {
            Assert.Equivalent(
                new[] { "Sales|2", "Engineering|2", "Empty|0" },
                Rows("SELECT D.DNAME, (SELECT COUNT(*) FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO) FROM ADO.DEPTS D"), strict: true);
        }

        /// <summary>
        /// A sub-query in the predicate rather than the projection, comparing against the outer row.
        /// </summary>
        /// <remarks>
        /// Only Bob survives. In department 10 the average is 150.25, so Bob is above it and Alice is not. In
        /// department 20 Carol is the only non-null salary, so the average is her own; Dave's salary is null,
        /// so his comparison is unknown. Erin's department is null, so the inner query matches nothing and
        /// the average is null.
        /// </remarks>
        [Fact]
        public void ACorrelatedComparisonFilters()
        {
            Assert.Equivalent(
                new[] { "Bob" },
                Rows("""
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE E.SALARY > (SELECT AVG(E2.SALARY) FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO)
                    """), strict: true);
        }

        #endregion

        #region Without decorrelation

        /// <summary>
        /// Opens a second connection that leaves correlates in the plan.
        /// </summary>
        /// <remarks>
        /// By default Calcite rewrites a correlated sub-query into a join, so the queries above reach the
        /// adapter decorrelated and never bind a correlation variable. <c>forceDecorrelate=false</c> keeps the
        /// <c>Correlate</c>.
        /// </remarks>
        /// <returns>
        /// An open Calcite connection with the SQLite database mounted as <c>ADO</c>; the caller closes it.
        /// </returns>
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
        /// Runs a query on a connection that does not decorrelate, returning rows as <see cref="Rows"/> does.
        /// </summary>
        /// <param name="sql">The statement to run, whose correlated sub-queries stay correlates in the
        /// plan.</param>
        /// <returns>One pipe-joined string per row, with <c>NULL</c> for a null.</returns>
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

        [Fact]
        public void ExistsIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                CorrelatedRows("SELECT NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)"), strict: true);
        }

        [Fact]
        public void AScalarSubQueryIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering", "Erin|NULL" },
                CorrelatedRows("SELECT E.NAME, (SELECT D.DNAME FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO) FROM ADO.EMPS E"), strict: true);
        }

        [Fact]
        public void AnAggregatingSubQueryIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Sales|2", "Engineering|2", "Empty|0" },
                CorrelatedRows("SELECT D.DNAME, (SELECT COUNT(*) FROM ADO.EMPS E WHERE E.DEPTNO = D.DEPTNO) FROM ADO.DEPTS D"), strict: true);
        }

        /// <summary>
        /// Correlating on an approximate column rather than an integer.
        /// </summary>
        /// <remarks>
        /// The bound value leaves the plan as a boxed Java type and has to be converted for a
        /// <see cref="System.Data.Common.DbParameter"/>. Everyone but the highest paid has someone above them;
        /// Dave's salary is null, so his comparison is unknown.
        /// </remarks>
        [Fact]
        public void CorrelatingOnARealIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Erin" },
                CorrelatedRows("SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.SALARY > E.SALARY)"), strict: true);
        }

        /// <summary>
        /// Correlating on a character column: everyone but the last name in order has one after them.
        /// </summary>
        [Fact]
        public void CorrelatingOnAStringIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                CorrelatedRows("SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.NAME > E.NAME)"), strict: true);
        }

        /// <summary>
        /// Correlating on a date, which leaves the plan as a day count and is converted back to a date for the
        /// parameter.
        /// </summary>
        [Fact]
        public void CorrelatingOnADateIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Carol", "Alice", "Bob" },
                CorrelatedRows("SELECT E.NAME FROM ADO.EMPS E WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.HIREDATE > E.HIREDATE)"), strict: true);
        }

        /// <summary>
        /// Two correlation variables in one statement, of different types.
        /// </summary>
        /// <remarks>
        /// Each marker is named for its position among the statement's parameters, while its value is read by
        /// correlation variable index, so the writer and the enricher must agree on the mapping. Swapping the
        /// two would compare a department against a salary and return a wrong answer rather than an error.
        /// Only Alice has someone in her own department earning more.
        /// </remarks>
        [Fact]
        public void TwoCorrelationVariablesAreBoundToTheRightParameters()
        {
            Assert.Equivalent(
                new[] { "Alice" },
                CorrelatedRows("""
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO AND E2.SALARY > E.SALARY)
                    """), strict: true);
        }

        /// <summary>
        /// One correlation variable read twice, which is two parameters carrying one value.
        /// </summary>
        /// <remarks>
        /// Everyone with a later employee within two of them, which is everyone but the last.
        /// </remarks>
        [Fact]
        public void OneVariableReadTwiceFillsBothParameters()
        {
            Assert.Equivalent(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                CorrelatedRows("""
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.EMPNO > E.EMPNO AND E2.EMPNO <= E.EMPNO + 2)
                    """), strict: true);
        }

        /// <summary>
        /// A correlated sub-query inside a correlated sub-query.
        /// </summary>
        /// <remarks>
        /// Two correlation contexts are live at once, each over a different outer row. A department survives
        /// when it has an employee with a colleague, so the empty one does not.
        /// </remarks>
        [Fact]
        public void NestedCorrelationIsCorrectWithoutDecorrelation()
        {
            Assert.Equivalent(
                new[] { "Sales", "Engineering" },
                CorrelatedRows("""
                    SELECT D.DNAME FROM ADO.DEPTS D
                    WHERE EXISTS (
                        SELECT 1 FROM ADO.EMPS E
                        WHERE E.DEPTNO = D.DEPTNO
                          AND EXISTS (SELECT 1 FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO AND E2.EMPNO <> E.EMPNO))
                    """), strict: true);
        }

        /// <summary>
        /// A null correlation value, which is bound rather than skipped.
        /// </summary>
        /// <remarks>
        /// Erin's department is null. The parameter is bound to <see cref="System.DBNull"/>, the inner
        /// comparison is unknown for every row, and her count is zero.
        /// </remarks>
        [Fact]
        public void ANullCorrelationValueIsBound()
        {
            Assert.Equivalent(
                new[] { "Alice|2", "Bob|2", "Carol|2", "Dave|2", "Erin|0" },
                CorrelatedRows("SELECT E.NAME, (SELECT COUNT(*) FROM ADO.EMPS E2 WHERE E2.DEPTNO = E.DEPTNO) FROM ADO.EMPS E"), strict: true);
        }

        /// <summary>
        /// A correlate under a sort: the inner query runs per row, and the ordering is applied to what
        /// survives.
        /// </summary>
        [Fact]
        public void ACorrelateSurvivesSorting()
        {
            Assert.Equal(
                new[] { "Alice", "Bob", "Carol", "Dave" },
                CorrelatedRows("""
                    SELECT E.NAME FROM ADO.EMPS E
                    WHERE EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)
                    ORDER BY E.NAME
                    """));
        }

        /// <summary>
        /// An outer query matching no rows yields no rows.
        /// </summary>
        [Fact]
        public void AnEmptyOuterYieldsNothing()
        {
            Assert.Empty(CorrelatedRows("""
                SELECT E.NAME FROM ADO.EMPS E
                WHERE E.EMPNO = 999 AND EXISTS (SELECT 1 FROM ADO.DEPTS D WHERE D.DEPTNO = E.DEPTNO)
                """));
        }

        #endregion

        #region Uncorrelated, for contrast

        /// <summary>
        /// An uncorrelated sub-query, which needs no value from the outer row, as a baseline for the
        /// correlated cases.
        /// </summary>
        [Fact]
        public void AnUncorrelatedSubQueryIsEvaluatedOnce()
        {
            Assert.Equivalent(
                new[] { "Carol" },
                Rows("SELECT NAME FROM ADO.EMPS WHERE SALARY = (SELECT MAX(SALARY) FROM ADO.EMPS)"), strict: true);
        }

        #endregion

    }

}
