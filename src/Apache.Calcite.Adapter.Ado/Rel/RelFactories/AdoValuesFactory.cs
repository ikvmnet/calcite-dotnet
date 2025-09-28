using java.lang;
using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.Ado.Rel.RelFactories
{

    class AdoValuesFactory : ValuesFactory
    {

        public RelNode createValues(RelOptCluster cluster, RelDataType rowType, List tuples)
        {
            throw new UnsupportedOperationException();
        }

    }

}
