using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// A user-defined table function, which is the path of <c>TableFunctionScan</c> where the call itself
    /// yields the sequence.
    /// </summary>
    /// <remarks>
    /// Top level, and with the method named as Calcite looks for it, because
    /// <c>TableFunctionImpl.create</c> finds it by name through reflection.
    /// </remarks>
    public class NumbersTableFunction
    {

        /// <summary>
        /// Returns a table of the numbers from one to <paramref name="count"/>.
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static ScannableTable eval(int count)
        {
            return new NumbersTable(count);
        }

        /// <summary>
        /// The table one call yields.
        /// </summary>
        /// <param name="count"></param>
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
            public Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                for (int i = 1; i <= count; i++)
                    list.add(new object[] { java.lang.Integer.valueOf(i) });

                return Linq4j.asEnumerable(list);
            }

        }

    }

}
