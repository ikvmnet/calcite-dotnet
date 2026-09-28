using com.google.common.collect;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A projection pushed down to the source as a select list. Mirrors <c>JdbcRules.JdbcProject</c>.
    /// </summary>
    public class AdoProject : Project, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="projects">The projected expressions.</param>
        /// <param name="rowType">The output row type.</param>
        public AdoProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List projects, RelDataType rowType) :
            base(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of())
        {

        }

        /// <inheritdoc />
        public override Project copy(RelTraitSet traitSet, RelNode input, List projects, RelDataType rowType)
        {
            return new AdoProject(getCluster(), traitSet, input, projects, rowType);
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
