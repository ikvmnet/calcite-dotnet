using System;
using System.Collections.Generic;
using System.Text;

using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using org.apache.calcite.avatica;
using org.apache.calcite.jdbc;

using Xunit;

namespace Apache.Calcite.Extensions.Prepare.Tests
{

    /// <summary>
    /// Compares the column metadata a prepared statement reports with what Calcite's own pipeline reports.
    /// </summary>
    /// <remarks>
    /// <c>getColumnMetaDataList</c>, <c>metaData</c>, <c>avaticaType</c> and their helpers are private to
    /// <c>CalcitePrepareImpl</c> and are ported. What the ADO.NET surface reports about a column (CLR type,
    /// provider type name, nullability, precision, scale) comes from them, and a defect there does not show
    /// in the rows. Both pipelines produce <c>ColumnMetaData</c>, so the comparison is field for field.
    /// </remarks>
    public class ClrPrepareImplMetadataTests
    {

        /// <summary>
        /// Renders one column's metadata so two lists can be compared field for field.
        /// </summary>
        /// <param name="c">The column.</param>
        /// <returns>The column's ordinal, name, label, nullability, signedness, sizes, catalog, schema, table, class name and
        /// type, on one line.</returns>
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
        /// Renders a column's type, including an array's component type and a struct's columns.
        /// </summary>
        /// <param name="t">The type.</param>
        /// <returns>The type's id, name and representation, followed by its component or columns where it has them.</returns>
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
        /// The columns <see cref="ClrPrepareImpl"/> reports for <paramref name="sql"/>.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <returns>Each column's metadata, rendered by <c>Render(ColumnMetaData)</c>.</returns>
        static List<string> Clr(string sql)
        {
            return ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var signature = new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);

                var columns = new List<string>();
                for (int i = 0; i < signature.Columns.size(); i++)
                    columns.Add(Render((ColumnMetaData)signature.Columns.get(i)));

                return columns;
            });
        }

        /// <summary>
        /// The columns Calcite's own pipeline reports for <paramref name="sql"/>.
        /// </summary>
        /// <param name="sql">The statement.</param>
        /// <returns>Each column's metadata, rendered by <c>Render(ColumnMetaData)</c>.</returns>
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

        [Theory]
        [InlineData("SELECT * FROM SALES")]
        [InlineData("SELECT ID, REGION FROM SALES")]
        [InlineData("SELECT AMOUNT FROM SALES")]
        [InlineData("SELECT COUNT(*) FROM SALES")]
        [InlineData("SELECT REGION, COUNT(*) FROM SALES GROUP BY REGION")]
        [InlineData("SELECT SUM(AMOUNT), AVG(AMOUNT) FROM SALES")]
        [InlineData("SELECT ID + 1 FROM SALES")]
        [InlineData("SELECT CAST(AMOUNT AS BIGINT), CAST(ID AS VARCHAR(4)) FROM SALES")]
        [InlineData("SELECT CAST(ID AS DECIMAL(9, 2)) FROM SALES")]
        [InlineData("SELECT NULL FROM SALES")]
        [InlineData("SELECT N FROM NUMS")]
        [InlineData("SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) FROM SALES")]
        [InlineData("SELECT * FROM (VALUES (1, 'a')) AS t(x, y)")]
        [InlineData("SELECT `name`, `salary` FROM HR.`emps`")]
        [InlineData("SELECT CURRENT_TIMESTAMP FROM SALES")]
        [InlineData("SELECT CAST(NULL AS BOOLEAN) FROM SALES")]
        public void Columns_should_match_calcite(string sql)
        {
            List<string> calcite;

            try
            {
                calcite = Calcite(sql);
            }
            catch (Exception e)
            {
                Assert.Skip($"Calcite cannot prepare this, so it is no oracle for it: {e.Message.Split('\n')[0]}");
                return;
            }

            var clr = Clr(sql);

            clr.Should().Equal(calcite,
                $"{sql}{Environment.NewLine}calcite: {string.Join(Environment.NewLine + "         ", calcite)}{Environment.NewLine}clr:     {string.Join(Environment.NewLine + "         ", clr)}");
        }

        /// <summary>
        /// The cursor factory, deduced from the columns and the element type, decides how a row is read back;
        /// a one-column row is the value itself rather than a one-element array.
        /// </summary>
        /// <param name="sql">The statement, of one column or several.</param>
        [Theory]
        [InlineData("SELECT * FROM SALES")]
        [InlineData("SELECT ID FROM SALES")]
        [InlineData("SELECT COUNT(*) FROM SALES")]
        [InlineData("SELECT N FROM NUMS")]
        [InlineData("SELECT `name` FROM HR.`emps`")]
        public void Cursor_factory_should_match_calcite(string sql)
        {
            var clr = ClrPrepareFixture.WithContext(sql, (context, _) =>
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1).CursorFactory.style.name());

            var calcite = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                var prepare = (CalcitePrepare)CalcitePrepare.DEFAULT_FACTORY.apply();
                return prepare.prepareSql(context, CalcitePrepare.Query.of(sql), (java.lang.Class)typeof(object[]), -1).cursorFactory.style.name();
            });

            clr.Should().Be(calcite, sql);
        }

        /// <summary>
        /// <c>maxRowCount</c> limits the rows <see cref="IClrPrepare.Signature.Bind"/> returns, as it does in
        /// Calcite's <c>CalciteSignature.enumerable</c>; a negative value means no limit.
        /// </summary>
        /// <param name="maxRowCount">The limit passed to <c>PrepareSql</c>.</param>
        /// <param name="expected">The number of rows the six-row table should then return.</param>
        [Theory]
        [InlineData(-1L, 6)]
        [InlineData(0L, 0)]
        [InlineData(1L, 1)]
        [InlineData(4L, 4)]
        [InlineData(100L, 6)]
        public void Max_row_count_should_limit_the_result(long maxRowCount, int expected)
        {
            var rows = ClrPrepareImplDifferentialTests.RunClr("SELECT ID FROM SALES ORDER BY ID", maxRowCount);

            rows.Count.Should().Be(expected, $"maxRowCount={maxRowCount}");
        }

    }

}
