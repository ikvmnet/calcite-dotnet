using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.Ado.Rel
{

    /// <summary>
    /// Implementation of <see cref="Filter"/> in <see cref="AdoConvention"/> calling convention.
    /// </summary>
    class AdoFilter : Filter, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="condition"></param>
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
