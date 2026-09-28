using com.google.common.collect;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A literal <c>VALUES</c> relation written into the pushed-down statement. Mirrors
    /// <c>JdbcRules.JdbcValues</c>.
    /// </summary>
    public class AdoValues : Values, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="rowType">The row type.</param>
        /// <param name="tuples">The rows, each a list of literals.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
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
