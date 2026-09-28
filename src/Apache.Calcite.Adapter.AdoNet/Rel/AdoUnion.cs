using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A <c>UNION</c> pushed down to the source. Mirrors <c>JdbcRules.JdbcUnion</c>.
    /// </summary>
    public class AdoUnion : Union, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="inputs">The inputs.</param>
        /// <param name="all">Whether duplicates are kept (<c>UNION ALL</c>).</param>
        public AdoUnion(RelOptCluster cluster, RelTraitSet traitSet, List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, List inputs, bool all)
        {
            return new AdoUnion(getCluster(), traitSet, inputs, all);
        }

        /// <summary>
        /// Returns the base cost multiplied by <see cref="AdoConvention.CostMultiplier"/>.
        /// </summary>
        /// <param name="planner">The planner.</param>
        /// <param name="mq">The metadata query.</param>
        /// <returns>The cost, or <see langword="null"/>.</returns>
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return base.computeSelfCost(planner, mq)?.multiplyBy(AdoConvention.CostMultiplier);
        }

    }

}
