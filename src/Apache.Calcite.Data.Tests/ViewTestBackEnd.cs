using System;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// A schema whose one table is a <see cref="ScannableTable"/> written here, standing in for a second
    /// back end in the view tests.
    /// </summary>
    /// <remarks>
    /// A <c>Schema</c> of <c>Table</c>s is the whole of what an adapter is to Calcite, so a view that
    /// spans this and a table the server DDL made spans two back ends in the only sense the planner has:
    /// two implementations of the table SPI, reached through one plan. Using a real provider — the ADO.NET
    /// adapter over SQLite, say — would exercise more of that adapter and nothing more of the view, and
    /// would put a database in a test suite that has none.
    ///
    /// <para>Rows hold Java-boxed values, because a table is a boundary and a boundary converts.</para>
    /// </remarks>
    sealed class ViewTestBackEnd : AbstractSchema
    {

        readonly java.util.Map _tables = new java.util.HashMap();

        /// <summary>
        /// Initializes a new instance holding a single REGIONS table.
        /// </summary>
        public ViewTestBackEnd()
        {
            _tables.put("REGIONS", new RowsTable(
                [
                    [java.lang.Integer.valueOf(10), "north"],
                    [java.lang.Integer.valueOf(20), "south"],
                ],
                f => f.builder()
                    .add("RID", f.createSqlType(SqlTypeName.INTEGER))
                    .add("RNAME", f.createSqlType(SqlTypeName.VARCHAR))
                    .build()));
        }

        /// <inheritdoc />
        protected override java.util.Map getTableMap() => _tables;

        /// <summary>
        /// A table over a fixed set of rows.
        /// </summary>
        sealed class RowsTable(object[][] rows, Func<RelDataTypeFactory, RelDataType> rowType) : AbstractTable, ScannableTable
        {

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory) => rowType(typeFactory);

            /// <inheritdoc />
            public Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                foreach (var row in rows)
                    list.add(row);

                return Linq4j.asEnumerable(list);
            }

        }

    }

}
