using System;
using System.Collections.Generic;
using System.Text;

using Apache.Calcite.Extensions.Prepare;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// What a prepared statement says about its result, against what Calcite says.
    /// </summary>
    /// <remarks>
    /// <c>getColumnMetaDataList</c>, <c>metaData</c>, <c>avaticaType</c> and the five helpers beneath them
    /// are private statics of <c>CalcitePrepareImpl</c>, so they are ported rather than reused, and
    /// everything the ADO.NET surface reports about a column comes out of them — the CLR type, the provider
    /// type name, nullability, precision, scale. A defect there is invisible in the rows.
    ///
    /// <para>Calcite's own pipeline is the oracle, reached directly rather than through a connection: both
    /// produce a list of <c>ColumnMetaData</c>, so the comparison is field for field.</para>
    /// </remarks>
    [TestClass]
    public class ClrPrepareMetadataTests
    {

        /// <summary>
        /// Renders one column's metadata so two lists can be compared field for field.
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        static string Render(ColumnMetaData c)
        {
            var sb = new StringBuilder();
            sb.Append(c.ordinal).Append(' ');
            sb.Append(c.columnName).Append(' ');
            sb.Append(c.label).Append(' ');
            sb.Append("null=").Append(c.nullable).Append(' ');
            sb.Append("signed=").Append(c.signed).Append(' ');
            sb.Append("display=").Append(c.displaySize).Append(' ');
            sb.Append("prec=").Append(c.precision).Append(' ');
            sb.Append("scale=").Append(c.scale).Append(' ');
            sb.Append("catalog=").Append(c.catalogName ?? "-").Append(' ');
            sb.Append("schema=").Append(c.schemaName ?? "-").Append(' ');
            sb.Append("table=").Append(c.tableName ?? "-").Append(' ');
            sb.Append("class=").Append(c.columnClassName).Append(' ');
            sb.Append("type=").Append(Render(c.type));

            return sb.ToString();
        }

        /// <summary>
        /// Renders a column's type, descending into a component or a struct as the port does.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        static string Render(ColumnMetaData.AvaticaType t)
        {
            var sb = new StringBuilder();
            sb.Append(t.id).Append('/').Append(t.name).Append('/').Append(t.rep.name());

            if (t is ColumnMetaData.ArrayType a && a.getComponent() is { } component)
                sb.Append("[[").Append(Render(component)).Append("]]");

            if (t is ColumnMetaData.StructType s)
                for (var i = s.columns.iterator(); i.hasNext();)
                    sb.Append("{{").Append(Render((ColumnMetaData)i.next())).Append("}}");

            return sb.ToString();
        }

        /// <summary>
        /// The columns this project's pipeline reports.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static List<string> Clr(string sql)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepare().Prepare(context, sql, (java.lang.Class)typeof(object[]), -1);

                var columns = new List<string>();
                for (int i = 0; i < signature.Columns.size(); i++)
                    columns.Add(Render((ColumnMetaData)signature.Columns.get(i)));

                return columns;
            });
        }

        /// <summary>
        /// The columns Calcite's own pipeline reports.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        static List<string> Calcite(string sql)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                var signature = prepare.prepareSql(context, CalcitePrepare.Query.of(sql), (java.lang.Class)typeof(object[]), -1);

                var columns = new List<string>();
                for (int i = 0; i < signature.columns.size(); i++)
                    columns.Add(Render((ColumnMetaData)signature.columns.get(i)));

                return columns;
            });
        }

        [TestMethod]
        [DataRow("SELECT * FROM SALES")]
        [DataRow("SELECT ID, REGION FROM SALES")]
        [DataRow("SELECT AMOUNT FROM SALES")]
        [DataRow("SELECT COUNT(*) FROM SALES")]
        [DataRow("SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION")]
        [DataRow("SELECT SUM(AMOUNT), AVG(AMOUNT) FROM SALES")]
        [DataRow("SELECT ID + 1 FROM SALES")]
        [DataRow("SELECT CAST(AMOUNT AS BIGINT), CAST(ID AS VARCHAR(4)) FROM SALES")]
        [DataRow("SELECT CAST(ID AS DECIMAL(9, 2)) FROM SALES")]
        [DataRow("SELECT NULL FROM SALES")]
        [DataRow("SELECT N FROM NUMS")]
        [DataRow("SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM SALES")]
        [DataRow("SELECT * FROM (VALUES (1, 'a')) AS t(x, y)")]
        [DataRow("SELECT `name`, `salary` FROM HR.`emps`")]
        [DataRow("SELECT CURRENT_TIMESTAMP FROM SALES")]
        [DataRow("SELECT CAST(NULL AS BOOLEAN) FROM SALES")]
        public void Columns_should_match_calcite(string sql)
        {
            List<string> calcite;

            try
            {
                calcite = Calcite(sql);
            }
            catch (Exception e)
            {
                Assert.Inconclusive($"Calcite cannot prepare this, so it is no oracle for it: {e.Message.Split('\n')[0]}");
                return;
            }

            var clr = Clr(sql);

            CollectionAssert.AreEqual(calcite, clr,
                $"{sql}{Environment.NewLine}calcite: {string.Join(Environment.NewLine + "         ", calcite)}{Environment.NewLine}clr:     {string.Join(Environment.NewLine + "         ", clr)}");
        }

        /// <summary>
        /// The cursor factory decides how a row is read back, and is deduced from the columns and the
        /// element type. It is worth asserting separately, because a row of one column is the value rather
        /// than a one-element array and that is the shape that has broken before.
        /// </summary>
        /// <param name="sql"></param>
        [TestMethod]
        [DataRow("SELECT * FROM SALES")]
        [DataRow("SELECT ID FROM SALES")]
        [DataRow("SELECT COUNT(*) FROM SALES")]
        [DataRow("SELECT N FROM NUMS")]
        [DataRow("SELECT `name` FROM HR.`emps`")]
        public void Cursor_factory_should_match_calcite(string sql)
        {
            var clr = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepare().Prepare(context, sql, (java.lang.Class)typeof(object[]), -1).CursorFactory.style.name());

            var calcite = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                return prepare.prepareSql(context, CalcitePrepare.Query.of(sql), (java.lang.Class)typeof(object[]), -1).cursorFactory.style.name();
            });

            Assert.AreEqual(calcite, clr, sql);
        }

        /// <summary>
        /// <c>maxRowCount</c> lives inside <c>CalciteSignature.enumerable</c> in Calcite and inside
        /// <see cref="ClrSignature.Bind"/> here. Nothing else exercises it, because every caller in this
        /// project passes -1.
        /// </summary>
        [TestMethod]
        [DataRow(-1L, 6)]
        [DataRow(0L, 0)]
        [DataRow(1L, 1)]
        [DataRow(4L, 4)]
        [DataRow(100L, 6)]
        public void Max_row_count_should_limit_the_result(long maxRowCount, int expected)
        {
            var rows = ClrPrepareDifferentialTests.RunClr("SELECT ID FROM SALES ORDER BY ID", maxRowCount);

            Assert.AreEqual(expected, rows.Count, $"maxRowCount={maxRowCount}");
        }

    }

}
