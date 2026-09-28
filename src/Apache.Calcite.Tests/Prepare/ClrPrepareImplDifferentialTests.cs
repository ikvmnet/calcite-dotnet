using System;
using System.Collections.Generic;
using System.Text;

using Apache.Calcite.Extensions.Adapter.Cursor.Tests;
using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Extensions.Prepare.Tests
{

    /// <summary>
    /// Runs the same SQL through <see cref="ClrPrepareImpl"/> and through Calcite's <c>CalcitePrepareImpl</c>,
    /// and requires the same rows.
    /// </summary>
    /// <remarks>
    /// <see cref="Extensions.Adapter.Cursor.Tests.ClrCursorConventionDifferentialTests"/> drives a
    /// <c>Frameworks</c> planner with a program of its own; this goes through the prepare pipeline, so each
    /// plan is whatever mixture of this convention and <c>EnumerableConvention</c> that pipeline's planner
    /// chooses, with converters where they meet.
    ///
    /// <para>Both sides are read at the pipeline rather than through a connection, so both yield the row
    /// objects their plan produced and one renderer serves both.</para>
    /// </remarks>
    public class ClrPrepareImplDifferentialTests
    {

        /// <summary>
        /// Prepares and runs a statement through <see cref="ClrPrepareImpl"/> and renders its rows.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="maxRowCount">The row limit, or a negative value for none.</param>
        /// <returns>Each row rendered as text.</returns>
        internal static List<string> RunClr(string sql, long maxRowCount = -1)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), maxRowCount);

                var rows = new List<string>();
                foreach (var row in signature.Bind(context.getDataContext()))
                    rows.Add(Render(row));

                return rows;
            });
        }

        /// <summary>
        /// Prepares and runs a statement through Calcite's own pipeline and renders its rows.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> RunCalcite(string sql)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                var signature = prepare.prepareSql(context, CalcitePrepare.Query.of(sql), (java.lang.Class)typeof(object[]), -1);

                var rows = new List<string>();
                var enumerator = signature.enumerable(context.getDataContext()).enumerator();

                try
                {
                    while (enumerator.moveNext())
                        rows.Add(Render(enumerator.current()));
                }
                finally
                {
                    enumerator.close();
                }

                return rows;
            });
        }

        /// <summary>
        /// Renders a row as text, so rows from the two pipelines compare by value whatever object holds each
        /// value.
        /// </summary>
        /// <param name="row">A row, an <c>object[]</c> for a multi-column result or the value itself for one column.</param>
        /// <returns>Each value rendered by <see cref="Cell"/>, joined with <c>|</c>.</returns>
        static string Render(object? row)
        {
            if (row is object[] array)
            {
                var sb = new StringBuilder();
                for (int i = 0; i < array.Length; i++)
                {
                    if (i > 0)
                        sb.Append('|');

                    sb.Append(Cell(array[i]));
                }

                return sb.ToString();
            }

            return Cell(row);
        }

        /// <summary>
        /// Renders one value: a Java object by its <c>toString()</c>, anything else in the invariant culture.
        /// </summary>
        /// <param name="value">The value, which may be null.</param>
        /// <returns>The value's text, or <c>null</c> for a null.</returns>
        static string Cell(object? value) => value switch
        {
            null => "null",
            java.lang.Object o => o.toString(),
            _ => System.Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "null",
        };

        /// <summary>
        /// The first line of the message of an exception and of each inner exception.
        /// </summary>
        /// <param name="e">The outermost exception.</param>
        /// <returns>One line per exception, outermost first.</returns>
        static IEnumerable<string> Chain(Exception e)
        {
            for (Exception? x = e; x != null; x = x.InnerException)
                yield return x.Message.Split('\n')[0];
        }

        [Theory]
        [InlineData("SELECT * FROM SALES ORDER BY ID")]
        [InlineData("SELECT ID, REGION FROM SALES WHERE AMOUNT > 10 ORDER BY ID")]
        [InlineData("SELECT ID FROM SALES WHERE AMOUNT IS NULL")]
        [InlineData("SELECT ID + 1, UPPER(REGION) FROM SALES ORDER BY ID")]
        [InlineData("SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION ORDER BY REGION")]
        [InlineData("SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION ORDER BY REGION")]
        [InlineData("SELECT REGION, AVG(AMOUNT), MIN(AMOUNT), MAX(AMOUNT) FROM SALES GROUP BY REGION ORDER BY REGION")]
        [InlineData("SELECT COUNT(*) FROM SALES")]
        [InlineData("SELECT COUNT(DISTINCT REGION) FROM SALES")]
        [InlineData("SELECT DISTINCT REGION FROM SALES ORDER BY REGION")]
        [InlineData("SELECT REGION, GRADE, COUNT(*) FROM SALES GROUP BY GROUPING SETS ((REGION), (GRADE), ()) ORDER BY REGION, GRADE")]
        [InlineData("SELECT ID FROM SALES ORDER BY ID DESC LIMIT 3")]
        [InlineData("SELECT ID FROM SALES ORDER BY ID OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY")]
        [InlineData("SELECT ID FROM SALES UNION SELECT ID FROM SALES ORDER BY ID")]
        [InlineData("SELECT ID FROM SALES UNION ALL SELECT ID FROM SALES ORDER BY ID")]
        [InlineData("SELECT ID FROM SALES INTERSECT SELECT ID FROM SALES WHERE ID > 3 ORDER BY ID")]
        [InlineData("SELECT ID FROM SALES EXCEPT SELECT ID FROM SALES WHERE ID > 3 ORDER BY ID")]
        [InlineData("SELECT a.ID, b.ID FROM SALES a JOIN SALES b ON a.REGION = b.REGION AND a.ID < b.ID ORDER BY a.ID, b.ID")]
        [InlineData("SELECT a.ID FROM SALES a LEFT JOIN SALES b ON a.AMOUNT = b.AMOUNT AND b.ID > 4 ORDER BY a.ID")]
        [InlineData("SELECT ID FROM SALES WHERE REGION IN (SELECT REGION FROM SALES WHERE AMOUNT = 30) ORDER BY ID")]
        [InlineData("SELECT ID FROM SALES a WHERE EXISTS (SELECT 1 FROM SALES b WHERE b.REGION = a.REGION AND b.ID > a.ID) ORDER BY ID")]
        [InlineData("SELECT ID, (SELECT COUNT(*) FROM SALES b WHERE b.REGION = a.REGION) FROM SALES a ORDER BY ID")]
        [InlineData("SELECT ID, ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY ID) FROM SALES ORDER BY ID")]
        [InlineData("SELECT ID, SUM(AMOUNT) OVER (ORDER BY ID) FROM SALES ORDER BY ID")]
        [InlineData("SELECT CASE WHEN AMOUNT IS NULL THEN 'none' WHEN AMOUNT > 15 THEN 'big' ELSE 'small' END FROM SALES ORDER BY ID")]
        [InlineData("SELECT COALESCE(AMOUNT, -1) FROM SALES ORDER BY ID")]
        [InlineData("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) ORDER BY x")]
        [InlineData("SELECT NULL FROM SALES WHERE ID = 1")]
        [InlineData("SELECT SUM(AMOUNT) FROM SALES WHERE ID > 100")]
        [InlineData("SELECT REGION FROM SALES WHERE GRADE LIKE 'A%' ORDER BY REGION")]
        [InlineData("SELECT CAST(AMOUNT AS BIGINT), CAST(ID AS VARCHAR(4)) FROM SALES ORDER BY ID")]
        [InlineData("SELECT N FROM NUMS ORDER BY N")]
        [InlineData("SELECT COUNT(*) FROM NUMS")]
        [InlineData("SELECT SUM(N) FROM NUMS")]
        [InlineData("SELECT N FROM NUMS WHERE N > 1 ORDER BY N")]
        [InlineData("SELECT DISTINCT N FROM NUMS ORDER BY N")]
        [InlineData("SELECT a.N, b.N FROM NUMS a JOIN NUMS b ON a.N = b.N ORDER BY a.N")]
        [InlineData("SELECT `name` FROM HR.`emps` ORDER BY `empid`")]
        [InlineData("SELECT `deptno`, COUNT(*) FROM HR.`emps` GROUP BY `deptno` ORDER BY `deptno`")]
        [InlineData("SELECT e.`name`, d.`name` FROM HR.`emps` e JOIN HR.`depts` d ON e.`deptno` = d.`deptno` ORDER BY e.`name`")]
        [InlineData("SELECT `empid`, SUM(`salary`) OVER (PARTITION BY `deptno`) FROM HR.`emps` ORDER BY `empid`")]
        [InlineData("SELECT SUM(N) FROM NUMS GROUP BY N ORDER BY 1")]
        [InlineData("SELECT N FROM NUMS ORDER BY N DESC LIMIT 2")]
        [InlineData("SELECT N FROM NUMS ORDER BY N OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY")]
        [InlineData("SELECT N FROM NUMS UNION SELECT N FROM NUMS ORDER BY N")]
        [InlineData("SELECT N FROM NUMS UNION ALL SELECT N FROM NUMS ORDER BY N")]
        [InlineData("SELECT N FROM NUMS INTERSECT SELECT N FROM NUMS WHERE N > 1 ORDER BY N")]
        [InlineData("SELECT N FROM NUMS EXCEPT SELECT N FROM NUMS WHERE N > 1 ORDER BY N")]
        [InlineData("SELECT N, COUNT(*) FROM NUMS GROUP BY N ORDER BY N")]
        [InlineData("SELECT N FROM NUMS GROUP BY N HAVING COUNT(*) > 0 ORDER BY N")]
        [InlineData("SELECT MIN(N), MAX(N), COUNT(DISTINCT N) FROM NUMS")]
        [InlineData("SELECT N, ROW_NUMBER() OVER (ORDER BY N) FROM NUMS ORDER BY N")]
        [InlineData("SELECT N, SUM(N) OVER (ORDER BY N) FROM NUMS ORDER BY N")]
        [InlineData("SELECT a.N, b.N FROM NUMS a LEFT JOIN NUMS b ON a.N = b.N AND b.N > 1 ORDER BY a.N")]
        [InlineData("SELECT a.N, b.N FROM NUMS a JOIN NUMS b ON a.N < b.N ORDER BY a.N, b.N")]
        [InlineData("SELECT N FROM NUMS WHERE N IN (SELECT N FROM NUMS WHERE N > 1) ORDER BY N")]
        [InlineData("SELECT N FROM NUMS a WHERE EXISTS (SELECT 1 FROM NUMS b WHERE b.N > a.N) ORDER BY N")]
        [InlineData("SELECT N, (SELECT COUNT(*) FROM NUMS b WHERE b.N > a.N) FROM NUMS a ORDER BY N")]
        [InlineData("SELECT N FROM NUMS WHERE N NOT IN (SELECT N FROM NUMS WHERE N > 2) ORDER BY N")]
        [InlineData("SELECT CASE WHEN N > 1 THEN N ELSE 0 END FROM NUMS ORDER BY 1")]
        [InlineData("SELECT CAST(N AS BIGINT) FROM NUMS ORDER BY 1")]
        [InlineData("SELECT N * 2 FROM NUMS ORDER BY 1")]
        [InlineData("SELECT * FROM (VALUES (1), (2)) AS t(x) ORDER BY x")]
        [InlineData("SELECT SUM(N) FROM NUMS HAVING SUM(N) > 0")]
        [InlineData("SELECT a.N FROM NUMS a JOIN SALES s ON a.N = s.ID ORDER BY a.N")]
        [InlineData("SELECT s.ID FROM SALES s JOIN NUMS a ON s.ID = a.N ORDER BY s.ID")]
        [InlineData("SELECT DISTINCT r FROM (SELECT ROW(x, y) AS r FROM (VALUES (1, 'a'), (1, 'a'), (2, NULL), (2, NULL)) AS v(x, y)) AS t ORDER BY r")]
        [InlineData("SELECT r, COUNT(*) AS c FROM (SELECT ROW(x, y) AS r FROM (VALUES (1, 'a'), (2, NULL), (2, NULL)) AS v(x, y)) AS t GROUP BY r ORDER BY c")]
        [InlineData("SELECT r, COUNT(*) AS c FROM (SELECT ROW(ROW(x, y), z) AS r FROM (VALUES (1, 'a', 10), (1, 'a', 10), (2, 'b', 20)) AS v(x, y, z)) AS t GROUP BY r ORDER BY c")]
        [InlineData("SELECT ROW(x, y) AS r FROM (VALUES (1, 'a'), (2, NULL)) AS v(x, y) UNION SELECT ROW(x, y) AS r FROM (VALUES (2, NULL)) AS w(x, y) ORDER BY r")]
        [InlineData("SELECT ROW(x, y) AS r FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y) INTERSECT SELECT ROW(x, y) AS r FROM (VALUES (2, 'b'), (3, 'c')) AS w(x, y)")]
        [InlineData("SELECT COUNT(*) AS c FROM (SELECT DISTINCT m FROM (VALUES MAP[1, 2, 3, 4], MAP[3, 4, 1, 2]) AS t(m))")]
        [InlineData("SELECT COUNT(*) AS c FROM (VALUES MAP[1, 2, 3, 4], MAP[3, 4, 1, 2]) AS t(m) GROUP BY m")]
        public void Should_agree_with_calcite(string sql)
        {
            List<string> calcite;

            try
            {
                calcite = RunCalcite(sql);
            }
            catch (Exception e)
            {
                Assert.Skip($"Calcite cannot run this, so it is no oracle for it: {string.Join(" <- ", Chain(e))}");
                return;
            }

            var clr = RunClr(sql);

            clr.Should().Equal(calcite,
                $"{sql}{Environment.NewLine}calcite: [{string.Join(", ", calcite)}]{Environment.NewLine}clr:     [{string.Join(", ", clr)}]");
        }

        /// <summary>
        /// Two rows with an array column, written inline so the statement needs no schema.
        /// </summary>
        const string Docs = "(VALUES (1, ARRAY['red','green']), (2, ARRAY['blue'])) AS d(ID, TAGS)";

        /// <summary>
        /// The same rows under a second alias, for the inner relation.
        /// </summary>
        const string Docs2 = "(VALUES (1, ARRAY['red','green']), (2, ARRAY['blue'])) AS d2(ID, TAGS)";

        /// <summary>
        /// A correlated <c>EXISTS</c> whose inner relation contains an <c>UNNEST</c>, correlated on a
        /// column that is not the unnested one.
        /// </summary>
        const string CorrelatedExistsOverAnUncollect =
            "SELECT d.ID FROM " + Docs + " WHERE EXISTS (" +
            "SELECT 1 FROM (SELECT d2.ID AS PID, t.X AS CITY FROM " + Docs2 + ", UNNEST(d2.TAGS) AS t(X)) c " +
            "WHERE c.PID = d.ID AND c.CITY = 'red')";

        /// <summary>
        /// Prepares and runs a statement with connection properties set by <paramref name="connectionProperties"/>.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <param name="connectionProperties">Sets properties on the connection the statement is prepared on.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> RunClrWith(string sql, Action<java.util.Properties> connectionProperties)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);

                var rows = new List<string>();
                foreach (var row in signature.Bind(context.getDataContext()))
                    rows.Add(Render(row));

                return rows;
            },
            connectionProperties);
        }

        /// <summary>
        /// Prepares and runs a statement with decorrelation turned off.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> RunClrWithoutDecorrelation(string sql)
        {
            return RunClrWith(sql, p => p.setProperty(CalciteConnectionProperty.FORCE_DECORRELATE.camelName(), "false"));
        }

        /// <summary>
        /// Prepares and runs a statement with the top-down general decorrelator.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <returns>The rows, each rendered by <see cref="Render"/>.</returns>
        static List<string> RunClrWithTopDownDecorrelation(string sql)
        {
            return RunClrWith(sql, p => p.setProperty(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED.camelName(), "true"));
        }

        /// <summary>
        /// Both pipelines fail a correlated <c>EXISTS</c> whose inner relation contains an <c>UNNEST</c>,
        /// because <c>RelDecorrelator</c> produces a malformed plan.
        /// </summary>
        /// <remarks>
        /// <c>RelDecorrelator</c> rewrites the correlate into a join and leaves the correlation variable in
        /// the right input, where nothing binds it:
        /// <c>Calc($cor1.ID) / NestedLoopJoin(condition=true) / [scan, Aggregate/Calc($cor1)]</c>. Only a
        /// <c>Correlate</c> binds a correlation variable.
        ///
        /// <para>Calcite fails on the same plan, and this reproduces that; the test asserts both sides so that
        /// an upstream fix shows up here. Only the report differs.
        /// <c>EnumerableRelImplementor.getCorrelVariableGetter</c> guards with an <c>assert</c>, which is off at
        /// run time, so Calcite throws a <c>NullPointerException</c> that <c>implementRoot</c> attaches with
        /// <c>addSuppressed</c> rather than as a cause. This convention raises the message the assertion
        /// carries.</para>
        ///
        /// <para>The statement runs without decorrelation
        /// (<see cref="ShouldRunACorrelatedExistsOverAnUncollectWithoutDecorrelation"/>) and with the top-down
        /// decorrelator (<see cref="ShouldRunACorrelatedExistsOverAnUncollectWithTopDownDecorrelation"/>).</para>
        /// </remarks>
        [Fact]
        public void ShouldAgreeOnFailingACorrelatedExistsOverAnUncollect()
        {
            var theirs = () => RunCalcite(CorrelatedExistsOverAnUncollect);
            theirs.Should().Throw<java.lang.IllegalStateException>(
                "Calcite implements this plan; if it has been fixed upstream, follow it");

            var mine = Assert.ThrowsAny<java.lang.IllegalStateException>(
                () => RunClr(CorrelatedExistsOverAnUncollect));

            string.Join(" <- ", Chain(mine)).Should().Contain("Correlation variable",
                "the reason is the unbound variable, and unlike Calcite's we say so");
        }

        /// <summary>
        /// The statement of <see cref="ShouldAgreeOnFailingACorrelatedExistsOverAnUncollect"/> runs correctly
        /// with decorrelation turned off.
        /// </summary>
        /// <remarks>
        /// The correlate is kept and <c>ClrCursorCorrelate</c> binds the variable. A caller gets this by
        /// setting <c>CalciteConnectionStringBuilder.ForceDecorrelate</c> to false.
        /// </remarks>
        [Fact]
        public void ShouldRunACorrelatedExistsOverAnUncollectWithoutDecorrelation()
        {
            RunClrWithoutDecorrelation(CorrelatedExistsOverAnUncollect).Should().Equal(["1"]);
        }

        /// <summary>
        /// The same statement runs correctly with the top-down general decorrelator.
        /// </summary>
        /// <remarks>
        /// <c>Programs.DecorrelateProgram</c> chooses between <c>RelDecorrelator</c> and
        /// <c>TopDownGeneralDecorrelator</c> on <c>topDownGeneralDecorrelationEnabled</c>, and the top-down one
        /// produces a plan that binds every variable it references, so the statement is still decorrelated. It
        /// is off by default, as it is in Calcite.
        /// </remarks>
        [Fact]
        public void ShouldRunACorrelatedExistsOverAnUncollectWithTopDownDecorrelation()
        {
            RunClrWithTopDownDecorrelation(CorrelatedExistsOverAnUncollect).Should().Equal(["1"]);
        }

    }

}
