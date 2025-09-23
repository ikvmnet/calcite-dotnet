using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Queryable implementation across an entire <see cref="AdoTable"/>.
    /// </summary>
    public class AdoTableQueryable : AbstractTableQueryable
    {

        readonly AdoTable _adoTable;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="adoTable"></param>
        /// <param name="queryProvider"></param>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        public AdoTableQueryable(AdoTable adoTable, QueryProvider queryProvider, SchemaPlus schema, string tableName) :
            base(queryProvider, schema, adoTable, tableName)
        {
            _adoTable = adoTable;
        }

        /// <inheritdoc />
        public override Enumerator enumerator()
        {
            return AdoEnumerable.CreateReader(
                    _adoTable.Schema.DataSource, 
                    _adoTable.GenerateSqlString().getSql(), 
                    AdoUtils.RowBuilderFactory2(_adoTable.GetFieldTypes(((CalciteConnection)queryProvider).getTypeFactory())))
                .enumerator();
        }

    }

}
