using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A filter pushed down to the source as <c>WHERE</c> or <c>HAVING</c>. Mirrors <c>JdbcRules.JdbcFilter</c>.
    /// </summary>
    public class AdoFilter : Filter, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="condition">The condition.</param>
        public AdoFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) :
            base(cluster, traitSet, input, condition)
        {

        }

        /// <inheritdoc />
        public override Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition)
        {
            return new AdoFilter(getCluster(), traitSet, input, condition);
        }

    }

}
