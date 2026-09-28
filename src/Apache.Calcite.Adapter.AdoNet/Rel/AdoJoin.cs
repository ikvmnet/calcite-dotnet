using System;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A join of two inputs from the same source, pushed down as one statement. Mirrors
    /// <c>JdbcRules.JdbcJoin</c>, including its cost.
    /// </summary>
    public class AdoJoin : Join, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="hints">The join hints.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables set by this join.</param>
        /// <param name="joinType">The join type.</param>
        public AdoJoin(RelOptCluster cluster, RelTraitSet traitSet, List hints, RelNode left, RelNode right, RexNode condition, Set variablesSet, JoinRelType joinType) :
            base(cluster, traitSet, hints, left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new AdoJoin(getCluster(), traitSet, hints, left, right, condition, variablesSet, joinType);
        }

        /// <summary>
        /// Returns a cost of the join's row count, with no CPU or I/O.
        /// </summary>
        /// <param name="planner">The planner.</param>
        /// <param name="mq">The metadata query.</param>
        /// <returns>The cost.</returns>
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return planner.getCostFactory().makeCost(mq.getRowCount(this).doubleValue(), 0, 0);
        }

        /// <summary>
        /// Returns the larger of the two inputs' row counts.
        /// </summary>
        /// <param name="mq">The metadata query.</param>
        /// <returns>The estimate.</returns>
        public override double estimateRowCount(RelMetadataQuery mq)
        {
            var lRowCount = mq.getRowCount(left);
            var rRowCount = mq.getRowCount(right);
            return Math.Max(lRowCount.doubleValue(), rRowCount.doubleValue());
        }

    }

}
