using java.lang;

using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="SnapshotFactory"/> implementation for the <see cref="AdoConvention"/>.
    /// Temporal snapshot operators are not supported by the ADO adapter; this factory always throws.
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
