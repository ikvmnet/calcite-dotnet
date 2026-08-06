using System;
using System.Collections.Generic;
using System.Text;

using Apache.Calcite.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// The same SQL through both prepares on a real connection, required to give the same rows.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableDifferentialTests"/> plans with <see cref="ClrEnumerablePrograms.Rules"/>,
    /// which is <c>Programs.ofRules</c> and clears the planner, so every plan it compares is built wholly in
    /// this convention. That is what proves a node is this convention's own rather than Calcite's, and it is
    /// not what a connection does.
    ///
    /// <para><see cref="ClrEnumerablePrepare"/> leaves Calcite's rules on the planner, so a real plan is
    /// whichever mixture of the two conventions the planner costed cheapest, with converters where they
    /// meet. Nothing measured that mixture against Calcite until this: the differential suite cannot see it,
    /// and the tests that do run through a connection assert rows written by hand.</para>
    /// </remarks>
    [TestClass]
    public class ClrEnumerablePrepareDifferentialTests
    {

        /// <summary>
        /// Four columns, one nullable, so a null orders and groups.
        /// </summary>
        sealed class SalesTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "A"],
                [java.lang.Integer.valueOf(2), "EAST", java.lang.Integer.valueOf(20), "B"],
                [java.lang.Integer.valueOf(3), "EAST", java.lang.Integer.valueOf(20), "C"],
                [java.lang.Integer.valueOf(4), "WEST", java.lang.Integer.valueOf(30), "D"],
                [java.lang.Integer.valueOf(5), "WEST", null, "E"],
                [java.lang.Integer.valueOf(6), "NORTH", java.lang.Integer.valueOf(10), "A"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("REGION", typeFactory.createSqlType(SqlTypeName.VARCHAR, 10))
                    .add("AMOUNT", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("GRADE", typeFactory.createSqlType(SqlTypeName.VARCHAR, 4))
                    .build();
            }

            /// <inheritdoc />
            public Enumerable scan(DataContext root) => Linq4j.asEnumerable(Rows);

        }

        /// <summary>
        /// One column, which gives a scan a SCALAR physical type while the table still yields Object[].
        /// That shape has broken twice.
        /// </summary>
        sealed class OneColumnTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(3)],
                [java.lang.Integer.valueOf(1)],
                [java.lang.Integer.valueOf(2)],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("N", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public Enumerable scan(DataContext root) => Linq4j.asEnumerable(Rows);

        }

        /// <summary>
        /// Opens a connection whose plans are compiled by the named engine.
        /// </summary>
        /// <param name="clr"></param>
        /// <returns></returns>
        static CalciteConnection Open(bool clr)
        {
            var c = new CalciteConnection(new CalciteConnectionStringBuilder { Lex = "JAVA", CaseSensitive = false }.ToString());

            if (clr == false)
                c.PrepareFactory = () => (org.apache.calcite.jdbc.CalcitePrepare)org.apache.calcite.jdbc.CalcitePrepare.DEFAULT_FACTORY.apply();

            c.Open();
            c.RootSchema.add("SALES", new SalesTable());
            c.RootSchema.add("NUMS", new OneColumnTable());

            // rows are instances of a Java class, so a scan yields a synthetic record rather than an
            // Object[] and PhysType takes its CUSTOM branch throughout
            c.RootSchema.add("HR", new org.apache.calcite.adapter.java.ReflectiveSchema(new org.apache.calcite.test.schemata.hr.HrSchema()));
            return c;
        }

        /// <summary>
        /// Runs a query and returns its rows as strings, so a comparison does not depend on which numeric
        /// type either engine chose.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="clr"></param>
        /// <returns></returns>
        static List<string> Run(string sql, bool clr)
        {
            using var c = Open(clr);
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;

            var rows = new List<string>();
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var sb = new StringBuilder();
                for (int i = 0; i < r.FieldCount; i++)
                {
                    if (i > 0)
                        sb.Append('|');

                    sb.Append(r.IsDBNull(i) ? "null" : r.GetValue(i).ToString());
                }

                rows.Add(sb.ToString());
            }

            return rows;
        }

        /// <summary>
        /// Every message in an exception and its causes.
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        static IEnumerable<string> Chain(Exception e)
        {
            for (Exception? x = e; x != null; x = x.InnerException)
                yield return x.Message.Split('\n')[0];
        }

        [DataTestMethod]
        [DataRow("SELECT * FROM SALES ORDER BY ID")]
        [DataRow("SELECT ID, REGION FROM SALES WHERE AMOUNT > 10 ORDER BY ID")]
        [DataRow("SELECT ID FROM SALES WHERE AMOUNT IS NULL")]
        [DataRow("SELECT ID + 1, UPPER(REGION) FROM SALES ORDER BY ID")]
        [DataRow("SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION ORDER BY REGION")]
        [DataRow("SELECT REGION, SUM(AMOUNT) FROM SALES GROUP BY REGION ORDER BY REGION")]
        [DataRow("SELECT REGION, AVG(AMOUNT), MIN(AMOUNT), MAX(AMOUNT) FROM SALES GROUP BY REGION ORDER BY REGION")]
        [DataRow("SELECT COUNT(*) FROM SALES")]
        [DataRow("SELECT COUNT(DISTINCT REGION) FROM SALES")]
        [DataRow("SELECT DISTINCT REGION FROM SALES ORDER BY REGION")]
        [DataRow("SELECT REGION, GRADE, COUNT(*) FROM SALES GROUP BY GROUPING SETS ((REGION), (GRADE), ()) ORDER BY REGION, GRADE")]
        [DataRow("SELECT ID FROM SALES ORDER BY ID DESC LIMIT 3")]
        [DataRow("SELECT ID FROM SALES ORDER BY ID OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY")]
        [DataRow("SELECT ID FROM SALES UNION SELECT ID FROM SALES ORDER BY ID")]
        [DataRow("SELECT ID FROM SALES UNION ALL SELECT ID FROM SALES ORDER BY ID")]
        [DataRow("SELECT ID FROM SALES INTERSECT SELECT ID FROM SALES WHERE ID > 3 ORDER BY ID")]
        [DataRow("SELECT ID FROM SALES EXCEPT SELECT ID FROM SALES WHERE ID > 3 ORDER BY ID")]
        [DataRow("SELECT a.ID, b.ID FROM SALES a JOIN SALES b ON a.REGION = b.REGION AND a.ID < b.ID ORDER BY a.ID, b.ID")]
        [DataRow("SELECT a.ID FROM SALES a LEFT JOIN SALES b ON a.AMOUNT = b.AMOUNT AND b.ID > 4 ORDER BY a.ID")]
        [DataRow("SELECT ID FROM SALES WHERE REGION IN (SELECT REGION FROM SALES WHERE AMOUNT = 30) ORDER BY ID")]
        [DataRow("SELECT ID FROM SALES a WHERE EXISTS (SELECT 1 FROM SALES b WHERE b.REGION = a.REGION AND b.ID > a.ID) ORDER BY ID")]
        [DataRow("SELECT ID, (SELECT COUNT(*) FROM SALES b WHERE b.REGION = a.REGION) FROM SALES a ORDER BY ID")]
        [DataRow("SELECT ID, ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY ID) FROM SALES ORDER BY ID")]
        [DataRow("SELECT ID, SUM(AMOUNT) OVER (ORDER BY ID) FROM SALES ORDER BY ID")]
        [DataRow("SELECT CASE WHEN AMOUNT IS NULL THEN 'none' WHEN AMOUNT > 15 THEN 'big' ELSE 'small' END FROM SALES ORDER BY ID")]
        [DataRow("SELECT COALESCE(AMOUNT, -1) FROM SALES ORDER BY ID")]
        [DataRow("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(x, y) ORDER BY x")]
        [DataRow("SELECT NULL FROM SALES WHERE ID = 1")]
        [DataRow("SELECT SUM(AMOUNT) FROM SALES WHERE ID > 100")]
        [DataRow("SELECT REGION FROM SALES WHERE GRADE LIKE 'A%' ORDER BY REGION")]
        [DataRow("SELECT CAST(AMOUNT AS BIGINT), CAST(ID AS VARCHAR(4)) FROM SALES ORDER BY ID")]
        [DataRow("SELECT N FROM NUMS ORDER BY N")]
        [DataRow("SELECT COUNT(*) FROM NUMS")]
        [DataRow("SELECT SUM(N) FROM NUMS")]
        [DataRow("SELECT N FROM NUMS WHERE N > 1 ORDER BY N")]
        [DataRow("SELECT DISTINCT N FROM NUMS ORDER BY N")]
        [DataRow("SELECT a.N, b.N FROM NUMS a JOIN NUMS b ON a.N = b.N ORDER BY a.N")]
        [DataRow("SELECT `name` FROM HR.`emps` ORDER BY `empid`")]
        [DataRow("SELECT `deptno`, COUNT(*) FROM HR.`emps` GROUP BY `deptno` ORDER BY `deptno`")]
        [DataRow("SELECT e.`name`, d.`name` FROM HR.`emps` e JOIN HR.`depts` d ON e.`deptno` = d.`deptno` ORDER BY e.`name`")]
        [DataRow("SELECT `empid`, SUM(`salary`) OVER (PARTITION BY `deptno`) FROM HR.`emps` ORDER BY `empid`")]
        [DataRow("SELECT SUM(N) FROM NUMS GROUP BY N ORDER BY 1")]
        [DataRow("SELECT N FROM NUMS ORDER BY N DESC LIMIT 2")]
        [DataRow("SELECT N FROM NUMS ORDER BY N OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY")]
        [DataRow("SELECT N FROM NUMS UNION SELECT N FROM NUMS ORDER BY N")]
        [DataRow("SELECT N FROM NUMS UNION ALL SELECT N FROM NUMS ORDER BY N")]
        [DataRow("SELECT N FROM NUMS INTERSECT SELECT N FROM NUMS WHERE N > 1 ORDER BY N")]
        [DataRow("SELECT N FROM NUMS EXCEPT SELECT N FROM NUMS WHERE N > 1 ORDER BY N")]
        [DataRow("SELECT N, COUNT(*) FROM NUMS GROUP BY N ORDER BY N")]
        [DataRow("SELECT N FROM NUMS GROUP BY N HAVING COUNT(*) > 0 ORDER BY N")]
        [DataRow("SELECT MIN(N), MAX(N), COUNT(DISTINCT N) FROM NUMS")]
        [DataRow("SELECT N, ROW_NUMBER() OVER (ORDER BY N) FROM NUMS ORDER BY N")]
        [DataRow("SELECT N, SUM(N) OVER (ORDER BY N) FROM NUMS ORDER BY N")]
        [DataRow("SELECT a.N, b.N FROM NUMS a LEFT JOIN NUMS b ON a.N = b.N AND b.N > 1 ORDER BY a.N")]
        [DataRow("SELECT a.N, b.N FROM NUMS a JOIN NUMS b ON a.N < b.N ORDER BY a.N, b.N")]
        [DataRow("SELECT N FROM NUMS WHERE N IN (SELECT N FROM NUMS WHERE N > 1) ORDER BY N")]
        [DataRow("SELECT N FROM NUMS a WHERE EXISTS (SELECT 1 FROM NUMS b WHERE b.N > a.N) ORDER BY N")]
        [DataRow("SELECT N, (SELECT COUNT(*) FROM NUMS b WHERE b.N > a.N) FROM NUMS a ORDER BY N")]
        [DataRow("SELECT N FROM NUMS WHERE N NOT IN (SELECT N FROM NUMS WHERE N > 2) ORDER BY N")]
        [DataRow("SELECT CASE WHEN N > 1 THEN N ELSE 0 END FROM NUMS ORDER BY 1")]
        [DataRow("SELECT CAST(N AS BIGINT) FROM NUMS ORDER BY 1")]
        [DataRow("SELECT N * 2 FROM NUMS ORDER BY 1")]
        [DataRow("SELECT * FROM (VALUES (1), (2)) AS t(x) ORDER BY x")]
        [DataRow("SELECT SUM(N) FROM NUMS HAVING SUM(N) > 0")]
        [DataRow("SELECT a.N FROM NUMS a JOIN SALES s ON a.N = s.ID ORDER BY a.N")]
        [DataRow("SELECT s.ID FROM SALES s JOIN NUMS a ON s.ID = a.N ORDER BY s.ID")]
        public void Should_agree_with_calcite(string sql)
        {
            List<string> calcite;

            try
            {
                calcite = Run(sql, clr: false);
            }
            catch (Exception e)
            {
                Assert.Inconclusive($"Calcite cannot run this, so it is no oracle for it: {string.Join(" <- ", Chain(e))}");
                return;
            }

            var clr = Run(sql, clr: true);

            CollectionAssert.AreEqual(calcite, clr,
                $"{sql}{Environment.NewLine}calcite: [{string.Join(", ", calcite)}]{Environment.NewLine}clr:     [{string.Join(", ", clr)}]");
        }

    }

}
