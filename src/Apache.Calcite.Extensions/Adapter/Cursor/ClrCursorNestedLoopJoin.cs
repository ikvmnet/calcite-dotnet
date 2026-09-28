using System.Linq.Expressions;


using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrCursorConvention"/> calling convention that
    /// tests the condition against every pair of rows.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableNestedLoopJoin</c>, and implements joins that have no equi-join keys or that
    /// <see cref="ClrCursorHashJoin"/> does not support.
    ///
    /// <para>The operator may acquire the right input from within an advance of either kind, so each body
    /// visits the right input through both hierarchies and passes a synchronous and an awaiting opener.</para>
    /// </remarks>
    public class ClrCursorNestedLoopJoin : Join, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorNestedLoopJoin"/>, deriving its collation from its inputs.
        /// </summary>
        /// <param name="left">The left (outer) input.</param>
        /// <param name="right">The right (inner) input.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables set by the join.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>The new join.</returns>
        public static ClrCursorNestedLoopJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableNestedLoopJoin(mq, left, right, joinType)));

            return new ClrCursorNestedLoopJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> derives the trait set; this constructor takes it as
        /// given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traits">The node's traits.</param>
        /// <param name="left">The left (outer) input.</param>
        /// <param name="right">The right (inner) input.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables set by the join.</param>
        /// <param name="joinType">The join type.</param>
        public ClrCursorNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrCursorNestedLoopJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A required collation on left fields only is passed to the left input, which is the outer loop and so
        /// determines the output order. None is passed for a <c>RIGHT</c> or <c>FULL</c> join, whose unmatched
        /// right rows come at the end.
        /// </remarks>
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrCursorTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet());
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

            // a join and its flipped form often cost the same; one is made slightly more expensive so that
            // the planner's choice between them is stable
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

            // the factor of ten is EnumerableNestedLoopJoin's penalty against the other join algorithms
            return planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(10);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (joinType.name() == nameof(JoinRelType.LEFT_MARK))
                return ImplementNLMarkJoin(implementor, pref);

            return ImplementNLJoin(implementor, pref);
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (joinType.name() == nameof(JoinRelType.LEFT_MARK))
                return ImplementNLMarkJoinAsync(implementor, pref);

            return ImplementNLJoinAsync(implementor, pref);
        }

        /// <summary>
        /// Implements a <c>LEFT_MARK</c> join, which returns every left row with a marker saying whether any
        /// right row matched.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableNestedLoopJoin.implementNLMarkJoin</c>. The predicate is generated nullable, so
        /// it returns null where the condition is unknown and the marker can be null; that is what makes
        /// <c>IN</c> over a nullable column answer <c>UNKNOWN</c>.
        /// </remarks>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The awaiting open of the join, whose rows are each left row followed by its marker.</returns>
        ClrCursorAsyncResult ImplementNLMarkJoinAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);
            var rightResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition(), true);
            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.LeftMarkNestedLoopJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    implementor.Opener(rightResultSync),
                    implementor.OpenerAsync(rightResult),
                    predicate,
                    selector));
        }

        /// <summary>
        /// Implements every join type other than <c>LEFT_MARK</c>. Mirrors
        /// <c>EnumerableNestedLoopJoin.implementNLJoin</c>.
        /// </summary>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The awaiting open of the join.</returns>
        ClrCursorAsyncResult ImplementNLJoinAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);
            var rightResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition());
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.NestedLoopJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    implementor.Opener(rightResultSync),
                    implementor.OpenerAsync(rightResult),
                    selector,
                    predicate,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType))));
        }

        /// <summary>
        /// Implements a <c>LEFT_MARK</c> join, which returns every left row with a marker saying whether any
        /// right row matched.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableNestedLoopJoin.implementNLMarkJoin</c>. The predicate is generated nullable, so
        /// it returns null where the condition is unknown and the marker can be null; that is what makes
        /// <c>IN</c> over a nullable column answer <c>UNKNOWN</c>.
        /// </remarks>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The synchronous open of the join, whose rows are each left row followed by its marker.</returns>
        ClrCursorResult ImplementNLMarkJoin(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition(), true);
            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.LeftMarkNestedLoopJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    implementor.Opener(rightResult),
                    implementor.OpenerAsync(rightResultAsync),
                    predicate,
                    selector));
        }

        /// <summary>
        /// Implements every join type other than <c>LEFT_MARK</c>. Mirrors
        /// <c>EnumerableNestedLoopJoin.implementNLJoin</c>.
        /// </summary>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The synchronous open of the join.</returns>
        ClrCursorResult ImplementNLJoin(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition());
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.NestedLoopJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    implementor.Opener(rightResult),
                    implementor.OpenerAsync(rightResultAsync),
                    selector,
                    predicate,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType))));
        }

    }

}
