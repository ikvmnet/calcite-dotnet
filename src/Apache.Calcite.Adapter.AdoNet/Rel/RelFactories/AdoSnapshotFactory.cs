using java.lang;

using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    public class AdoSnapshotFactory : SnapshotFactory
    {

        public RelNode createSnapshot(RelNode input, RexNode period)
        {
            throw new UnsupportedOperationException();
        }

    }

}
