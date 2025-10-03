using com.google.common.collect;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.Ado.Rel
{

    /// <summary>
    /// Implementation of <see cref="Project"/> in <see cref="AdoConvention"/> calling convention.
    /// </summary>
    public class AdoProject : Project, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="projects"></param>
        /// <param name="rowType"></param>
        public AdoProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List projects, RelDataType rowType) :
            base(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of())
        {

        }

        /// <inheritdoc />
        public override Project copy(RelTraitSet traitSet, RelNode input, List projects, RelDataType rowType)
        {
            return new AdoProject(getCluster(), traitSet, input, projects, rowType);
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return base.computeSelfCost(planner, mq)?.multiplyBy(AdoConvention.CostMultiplier);
        }

    }

}
