using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// An <c>EXCEPT</c> pushed down to the source. Mirrors <c>JdbcRules.JdbcMinus</c>.
    /// </summary>
    public class AdoMinus : Minus, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="inputs">The inputs.</param>
        /// <param name="all">Whether duplicates are kept (<c>EXCEPT ALL</c>).</param>
        public AdoMinus(RelOptCluster cluster, RelTraitSet traitSet, List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />

        public override SetOp copy(RelTraitSet traitSet, List inputs, bool all)
        {
            return new AdoMinus(getCluster(), traitSet, inputs, all);
        }

    }

}
