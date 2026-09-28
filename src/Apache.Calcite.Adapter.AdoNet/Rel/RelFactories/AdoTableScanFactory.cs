using java.lang;

using org.apache.calcite.plan;
using org.apache.calcite.rel;

using static org.apache.calcite.plan.RelOptTable;
using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="TableScanFactory"/> that throws. An <see cref="AdoTableScan"/> is created by
    /// <see cref="AdoTable.toRel"/>.
    /// </summary>
    public class AdoTableScanFactory : TableScanFactory
    {

        /// <inheritdoc />
        public RelNode createScan(ToRelContext toRelContext, RelOptTable table)
        {
            throw new UnsupportedOperationException();
        }

    }

}
