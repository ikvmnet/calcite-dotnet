using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// An <c>INTERSECT</c> pushed down to the source. Mirrors <c>JdbcRules.JdbcIntersect</c>.
    /// </summary>
    public class AdoIntersect : Intersect, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="inputs">The inputs.</param>
        /// <param name="all">Whether duplicates are kept (<c>INTERSECT ALL</c>).</param>
        public AdoIntersect(RelOptCluster cluster, RelTraitSet traitSet, List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, List inputs, bool all)
        {
            return new AdoIntersect(getCluster(), traitSet, inputs, all);
        }

    }

}
