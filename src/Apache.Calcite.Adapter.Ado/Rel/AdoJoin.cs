using System;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.Ado.Rel
{

    /// <summary>
    /// Join operator implemented in ADO convention.
    /// </summary>
    public class AdoJoin : Join, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="hints"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        public AdoJoin(RelOptCluster cluster, RelTraitSet traitSet, List hints, RelNode left, RelNode right, RexNode condition, Set variablesSet, JoinRelType joinType) :
            base(cluster, traitSet, hints, left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new AdoJoin(getCluster(), traitSet, hints, left, right, condition, variablesSet, joinType);
        }

        /// <inheritdoc />
        public override RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return planner.getCostFactory().makeCost(mq.getRowCount(this).doubleValue(), 0, 0);
        }

        /// <inheritdoc />
        public override double estimateRowCount(RelMetadataQuery mq)
        {
            var lRowCount = mq.getRowCount(left)!;
            var rRowCount = mq.getRowCount(right)!;
            return Math.Max(lRowCount.doubleValue(), rRowCount.doubleValue());
        }

    }

}
