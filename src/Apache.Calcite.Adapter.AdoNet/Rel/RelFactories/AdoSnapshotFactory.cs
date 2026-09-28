using java.lang;

using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="SnapshotFactory"/> that throws: the adapter does not push down a temporal snapshot.
    /// </summary>
    public class AdoSnapshotFactory : SnapshotFactory
    {

        /// <inheritdoc />
        public RelNode createSnapshot(RelNode input, RexNode period)
        {
            throw new UnsupportedOperationException();
        }

    }

}
