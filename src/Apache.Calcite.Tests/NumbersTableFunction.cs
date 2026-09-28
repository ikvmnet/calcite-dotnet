using Apache.Calcite.Extensions;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A user-defined table function returning the integers from one to a count, for tests of a
    /// <c>TableFunctionScan</c> whose call yields the rows.
    /// </summary>
    /// <remarks>
    /// The method is named <c>eval</c> because <c>TableFunctionImpl.create</c> looks it up by that name through
    /// reflection.
    /// </remarks>
    public class NumbersTableFunction
    {

        /// <summary>
        /// Returns a one-column table of the integers from one to <paramref name="count"/>.
        /// </summary>
        /// <param name="count">The last number in the table.</param>
        /// <returns>A table with one <c>INTEGER</c> column, <c>N</c>.</returns>
        public static ScannableTable eval(int count)
        {
            return new NumbersTable(count);
        }

        /// <summary>
        /// The table one call returns.
        /// </summary>
        /// <param name="count">The last number in the table.</param>
        sealed class NumbersTable(int count) : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("N", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                for (int i = 1; i <= count; i++)
                    list.add(new object[] { java.lang.Integer.valueOf(i) });

                return Linq4j.asEnumerable(list);
            }

        }

    }

}
