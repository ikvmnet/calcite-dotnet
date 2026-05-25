using java.lang;

using org.apache.calcite.plan;
using org.apache.calcite.rel;

using static org.apache.calcite.plan.RelOptTable;
using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="TableScanFactory"/> implementation for the <see cref="AdoConvention"/>.
    /// Table scans are created directly by the schema layer; this factory always throws.
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
