using System.Linq.Expressions;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrEnumerableConvention"/> calling convention,
    /// by comparing every pair of rows.
    /// </summary>
    /// <remarks>
    /// What a join with no equality to build a lookup on becomes, and what
    /// <see cref="ClrEnumerableHashJoin"/> leaves for it.
    /// </remarks>
    public class ClrEnumerableNestedLoopJoin : Join, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableNestedLoopJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrEnumerableNestedLoopJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableNestedLoopJoin(mq, left, right, joinType)));

            return new ClrEnumerableNestedLoopJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        public ClrEnumerableNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrEnumerableNestedLoopJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The collation passes to the left input and to no other: the left is the outer loop, so only it can
        /// preserve an ordering. Pushing a sort to the right does not help a right outer join either, because
        /// the unmatched right rows are produced together at the end.
        /// </remarks>
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrEnumerableTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet());
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            if (joinType.name() == nameof(JoinRelType.FULL) || joinType.name() == nameof(JoinRelType.RIGHT))
                return DeriveMode.PROHIBITED;

            return DeriveMode.LEFT_FIRST;
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            var rowCount = mq.getRowCount(this).doubleValue();

            // a join can be flipped, and for many algorithms both versions are viable and cost the same. To
            // keep the answer stable from one version of the planner to the next, one of them is made
            // slightly more expensive.
            switch (joinType.name())
            {
                case nameof(JoinRelType.SEMI):
                case nameof(JoinRelType.ANTI):
                    // SEMI and ANTI cannot be flipped
                    break;
                case nameof(JoinRelType.RIGHT):
                    rowCount = RelMdUtil.addEpsilon(rowCount);
                    break;
                default:
                    if (RelNodes.COMPARATOR.compare(getLeft(), getRight()) > 0)
                        rowCount = RelMdUtil.addEpsilon(rowCount);
                    break;
            }

            var rightRowCount = mq.getRowCount(getRight()).doubleValue();
            var leftRowCount = mq.getRowCount(getLeft()).doubleValue();
            if (double.IsInfinity(leftRowCount))
                rowCount = leftRowCount;
            if (double.IsInfinity(rightRowCount))
                rowCount = rightRowCount;

            // give it some penalty
            return planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(10);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (joinType.name() == nameof(JoinRelType.LEFT_MARK))
                return ImplementNLMarkJoin(implementor, pref);

            return ImplementNLJoin(implementor, pref);
        }

        /// <summary>
        /// Implements a mark join, which returns every left row with a marker saying whether the right side
        /// had a match.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>implementNLMarkJoin</c>. The predicate is the whole condition rather than
        /// its non-equi part, and it is the three-valued one: a mark join's marker is null where a comparison
        /// was unknown, which is what makes <c>IN</c> over a nullable column answer UNKNOWN.
        /// </remarks>
        ClrEnumerableResult ImplementNLMarkJoin(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition(), true);
            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.LeftMarkNestedLoopJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    predicate,
                    selector));
        }

        /// <summary>
        /// Implements the join by comparing every pair.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        ClrEnumerableResult ImplementNLJoin(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition());
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.NestedLoopJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    selector,
                    predicate,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType))));
        }

    }

}
