using com.google.common.collect;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.Ado.Rel
{

    /// <summary>
    /// Values operator implemented in ADO convention.
    /// </summary>
    public class AdoValues : Values, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="rowType"></param>
        /// <param name="tuples"></param>
        /// <param name="traitSet"></param>
        public AdoValues(RelOptCluster cluster, RelDataType rowType, ImmutableList tuples, RelTraitSet traitSet) :
            base(cluster, rowType, tuples, traitSet)
        {

        }

        /// <inheritdoc />
        public override Values copy(RelTraitSet traitSet, List inputs)
        {
            return new AdoValues(getCluster(), getRowType(), tuples, traitSet);
        }

    }

}
