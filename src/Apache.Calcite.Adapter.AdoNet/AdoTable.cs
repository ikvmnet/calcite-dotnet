using org.apache.calcite.adapter.java;
using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.AdoNet
{

    public class AdoTable : AbstractQueryableTable, TranslatableTable, ScannableTable, ModifiableTable
    {

        readonly AdoSchema _schema;

    }

}
