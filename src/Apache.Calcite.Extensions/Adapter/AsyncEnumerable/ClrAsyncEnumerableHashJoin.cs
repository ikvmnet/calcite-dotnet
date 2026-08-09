using System;
using System.Linq.Expressions;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.util;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling convention,
    /// by building a lookup of one input and probing it with the other.
    /// </summary>
    public class ClrAsyncEnumerableHashJoin : Join, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableHashJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableHashJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableHashJoin(mq, left, right, joinType)));

            return new ClrAsyncEnumerableHashJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
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
        public ClrAsyncEnumerableHashJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrAsyncEnumerableHashJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left join input
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

            // cheaper if the smaller number of rows is coming from the left, modelled by adding L log L
            var rightRowCount = mq.getRowCount(getRight()).doubleValue();
            var leftRowCount = mq.getRowCount(getLeft()).doubleValue();
            if (double.IsInfinity(leftRowCount))
                rowCount = leftRowCount;
            else
                rowCount += Util.nLogN(leftRowCount);

            if (double.IsInfinity(rightRowCount))
                rowCount = rightRowCount;
            else
                rowCount += rightRowCount;

            if (isSemiJoin())
                return planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(.01d);
            else
                return planner.getCostFactory().makeCost(rowCount, 0, 0);
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            switch (joinType.name())
            {
                case nameof(JoinRelType.SEMI):
                case nameof(JoinRelType.ANTI):
                    return ImplementHashSemiJoin(implementor, pref);
                case nameof(JoinRelType.LEFT_MARK):
                    return ImplementHashMarkJoin(implementor, pref);
                default:
                    return ImplementHashJoin(implementor, pref);
            }
        }

        /// <summary>
        /// Implements every join that returns fields of both inputs.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        ClrAsyncEnumerableResult ImplementHashJoin(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrAsyncEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrAsyncEnumerableRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var keyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var leftKey = NullAwareAccessor(leftResult.PhysType, joinInfo.leftKeys);
            var rightKey = NullAwareAccessor(rightResult.PhysType, joinInfo.rightKeys);
            var keyType = leftKey.ReturnType;

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var predicate = Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType);

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.HashJoin.MakeGenericMethod(leftType, rightType, keyType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKey,
                    rightKey,
                    selector,
                    keyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(joinType.generatesNullsOnLeft()),
                    Expression.Constant(joinType.generatesNullsOnRight()),
                    predicate));
        }

        /// <summary>
        /// Implements a semi or an anti join, which return the left input alone.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        ClrAsyncEnumerableResult ImplementHashSemiJoin(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrAsyncEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrAsyncEnumerableRel)right, pref);

            var physType = leftResult.PhysType;
            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;

            var keyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var leftKey = NullAwareAccessor(leftResult.PhysType, joinInfo.leftKeys);
            var rightKey = NullAwareAccessor(rightResult.PhysType, joinInfo.rightKeys);

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.SemiJoin.MakeGenericMethod(leftType, rightType, leftKey.ReturnType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKey,
                    rightKey,
                    keyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(joinType.name() == nameof(JoinRelType.ANTI)),
                    Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType)));
        }

        /// <summary>
        /// Implements a mark join, which returns every left row with a marker saying whether the right side
        /// had a match.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>implementHashMarkJoin</c>, statement for statement. Both predicates are the
        /// three-valued ones, because the marker is three-valued: a mark join has to tell FALSE from UNKNOWN,
        /// which is what makes <c>x IN (…)</c> over a nullable column answer UNKNOWN.
        ///
        /// <para>The join keys split in two. A null-safe key is IS NOT DISTINCT FROM and answers only
        /// TRUE or FALSE; a not null-safe one is EQUALS and answers three ways. The runtime takes both key
        /// selectors and a flag saying whether at most one key is not null-safe, because that is the case a
        /// hash lookup alone can decide.</para>
        /// </remarks>
        ClrAsyncEnumerableResult ImplementHashMarkJoin(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrAsyncEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrAsyncEnumerableRel)right, pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var rexBuilder = getCluster().getRexBuilder();

            LambdaExpression? nonEquiPredicate = null;
            if (joinInfo.nonEquiConditions.isEmpty() == false)
            {
                var nonEquiCondition = RexUtil.composeConjunction(rexBuilder, joinInfo.nonEquiConditions, true);
                if (nonEquiCondition != null)
                    nonEquiPredicate = ClrEnumUtils.GeneratePredicate(implementor, rexBuilder, left, right, leftResult.PhysType, rightResult.PhysType, nonEquiCondition, true);
            }

            var equiCondition = joinInfo.getEquiCondition(left, right, rexBuilder);
            var equiPredicate = ClrEnumUtils.GeneratePredicate(implementor, rexBuilder, left, right, leftResult.PhysType, rightResult.PhysType, equiCondition, true);

            // the null-aware accessor yields null where a not null-safe key is null, which is how the runtime
            // learns that a comparison is unknown rather than false
            var leftKeySelector = NullAwareAccessor(leftResult.PhysType, joinInfo.leftKeys);
            var rightKeySelector = NullAwareAccessor(rightResult.PhysType, joinInfo.rightKeys);

            var notNullSafeKeyCount = 0;
            var leftNullSafeKeys = new java.util.ArrayList();
            var rightNullSafeKeys = new java.util.ArrayList();

            for (int i = 0; i < joinInfo.nullExclusionFlags.size(); i++)
            {
                if (((java.lang.Boolean)joinInfo.nullExclusionFlags.get(i)).booleanValue())
                {
                    notNullSafeKeyCount++;
                }
                else
                {
                    leftNullSafeKeys.add(joinInfo.leftKeys.get(i));
                    rightNullSafeKeys.add(joinInfo.rightKeys.get(i));
                }
            }

            var leftNullSafeKeySelector = leftNullSafeKeys.isEmpty()
                ? null
                : Accessor(leftResult.PhysType, ImmutableIntList.copyOf(leftNullSafeKeys));
            var rightNullSafeKeySelector = rightNullSafeKeys.isEmpty()
                ? null
                : Accessor(rightResult.PhysType, ImmutableIntList.copyOf(rightNullSafeKeys));

            var atMostOneNotNullSafeKey = notNullSafeKeyCount <= 1;

            var nullSafeKeyPhysType = leftResult.PhysType.Project(leftNullSafeKeys, JavaRowFormat.LIST);
            var nullSafeKeyComparer = nullSafeKeyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer));
            var keyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var keyComparer = keyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer));

            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            var keyType = leftKeySelector.ReturnType;
            var nullSafeKeyType = leftNullSafeKeySelector?.ReturnType ?? typeof(object);

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.LeftMarkHashJoin.MakeGenericMethod(leftType, rightType, keyType, nullSafeKeyType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKeySelector,
                    rightKeySelector,
                    (Expression?)leftNullSafeKeySelector ?? Expression.Constant(null, typeof(Func<,>).MakeGenericType(leftType, nullSafeKeyType)),
                    (Expression?)rightNullSafeKeySelector ?? Expression.Constant(null, typeof(Func<,>).MakeGenericType(rightType, nullSafeKeyType)),
                    Expression.Constant(atMostOneNotNullSafeKey),
                    selector,
                    keyComparer,
                    nullSafeKeyComparer,
                    (Expression?)nonEquiPredicate ?? Expression.Constant(null, typeof(Func<,,>).MakeGenericType(leftType, rightType, typeof(java.lang.Boolean))),
                    equiPredicate));
        }

        /// <summary>
        /// Returns the lambda reading a join key from a row, yielding null for the whole key where a field
        /// that excludes nulls is null.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="keys"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// What the hash join, the semi join and the mark join all key on. The key is a list even for one
        /// field, so a null-safe key of one field is a list holding null — which is not null, and matches
        /// another one. That is the whole difference from <see cref="Accessor"/>, and it is what makes
        /// <c>IS NOT DISTINCT FROM</c> join a null to a null.
        /// </remarks>
        LambdaExpression NullAwareAccessor(ClrPhysType physType, java.util.List keys)
        {
            return physType.GenerateNullAwareAccessor(keys, joinInfo.nullExclusionFlags);
        }

        /// <summary>
        /// Returns the lambda reading a join key from a row.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="keys"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The plain accessor, which yields the field itself for a key of one. A mark join keys its null-safe
        /// lookup on this, as Calcite does; nothing else here does.
        /// </remarks>
        static LambdaExpression Accessor(ClrPhysType physType, java.util.List keys)
        {
            return physType.GenerateAccessor(keys);
        }

        /// <summary>
        /// Returns the lambda testing the part of the condition that is not an equality, or a null constant
        /// where the condition is entirely equalities.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="leftPhysType"></param>
        /// <param name="rightPhysType"></param>
        /// <param name="leftType"></param>
        /// <param name="rightType"></param>
        /// <returns></returns>
        Expression Predicate(ClrAsyncEnumerableRelImplementor implementor, ClrPhysType leftPhysType, ClrPhysType rightPhysType, Type leftType, Type rightType)
        {
            var type = typeof(Func<,,>).MakeGenericType(leftType, rightType, typeof(bool));
            var info = analyzeCondition();

            if (info.nonEquiConditions.isEmpty())
                return Expression.Constant(null, type);

            var nonEqui = RexUtil.composeConjunction(getCluster().getRexBuilder(), info.nonEquiConditions, true);
            if (nonEqui == null)
                return Expression.Constant(null, type);

            return ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftPhysType, rightPhysType, nonEqui);
        }

    }

}
