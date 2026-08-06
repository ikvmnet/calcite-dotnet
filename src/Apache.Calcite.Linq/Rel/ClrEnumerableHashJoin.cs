using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Tree;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.util;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrEnumerableConvention"/> calling convention,
    /// by building a lookup of one input and probing it with the other.
    /// </summary>
    public class ClrEnumerableHashJoin : Join, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableHashJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrEnumerableHashJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableHashJoin(mq, left, right, joinType)));

            return new ClrEnumerableHashJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
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
        public ClrEnumerableHashJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrEnumerableHashJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet())!;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left join input
            return ClrEnumerableTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet())!;
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            if (joinType.name() == nameof(JoinRelType.FULL) || joinType.name() == nameof(JoinRelType.RIGHT))
                return DeriveMode.PROHIBITED;

            return DeriveMode.LEFT_FIRST;
        }

        /// <inheritdoc />
        public override RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
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
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
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
        ClrEnumerableResult ImplementHashJoin(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var keyPhysType = leftResult.PhysType.project(joinInfo.leftKeys, JavaRowFormat.LIST);

            var leftSource = ClrEnumUtils.BoxRows(leftResult.PhysType, leftResult.Expression);
            var rightSource = ClrEnumUtils.BoxRows(rightResult.PhysType, rightResult.Expression);
            var leftType = leftSource.Type.GetGenericArguments()[0];
            var rightType = rightSource.Type.GetGenericArguments()[0];
            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

            var leftKey = NullAwareAccessor(implementor, leftResult.PhysType, joinInfo.leftKeys, leftType);
            var rightKey = NullAwareAccessor(implementor, rightResult.PhysType, joinInfo.rightKeys, rightType);
            var keyType = leftKey.ReturnType;

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var predicate = Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.HashJoin.MakeGenericMethod(leftType, rightType, keyType, rowType),
                    leftSource,
                    rightSource,
                    leftKey,
                    rightKey,
                    selector,
                    ClrPhysTypes.Comparer(implementor, keyPhysType),
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
        ClrEnumerableResult ImplementHashSemiJoin(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = leftResult.PhysType;
            var leftType = ClrEnumerableRelImplementor.RowType(leftResult.PhysType);
            var rightType = ClrEnumerableRelImplementor.RowType(rightResult.PhysType);

            var keyPhysType = leftResult.PhysType.project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var leftKey = NullAwareAccessor(implementor, leftResult.PhysType, joinInfo.leftKeys, leftType);
            var rightKey = NullAwareAccessor(implementor, rightResult.PhysType, joinInfo.rightKeys, rightType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.SemiJoin.MakeGenericMethod(leftType, rightType, leftKey.ReturnType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKey,
                    rightKey,
                    ClrPhysTypes.Comparer(implementor, keyPhysType),
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
        ClrEnumerableResult ImplementHashMarkJoin(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            var leftSource = ClrEnumUtils.BoxRows(leftResult.PhysType, leftResult.Expression);
            var rightSource = ClrEnumUtils.BoxRows(rightResult.PhysType, rightResult.Expression);
            var leftType = leftSource.Type.GetGenericArguments()[0];
            var rightType = rightSource.Type.GetGenericArguments()[0];
            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

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
            var leftKeySelector = NullAwareAccessor(implementor, leftResult.PhysType, joinInfo.leftKeys, leftType);
            var rightKeySelector = NullAwareAccessor(implementor, rightResult.PhysType, joinInfo.rightKeys, rightType);

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
                : Accessor(implementor, leftResult.PhysType, ImmutableIntList.copyOf(leftNullSafeKeys), leftType);
            var rightNullSafeKeySelector = rightNullSafeKeys.isEmpty()
                ? null
                : Accessor(implementor, rightResult.PhysType, ImmutableIntList.copyOf(rightNullSafeKeys), rightType);

            var atMostOneNotNullSafeKey = notNullSafeKeyCount <= 1;

            var nullSafeKeyPhysType = leftResult.PhysType.project(leftNullSafeKeys, JavaRowFormat.LIST);
            var nullSafeKeyComparer = ClrPhysTypes.Comparer(implementor, nullSafeKeyPhysType);
            var keyPhysType = leftResult.PhysType.project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var keyComparer = ClrPhysTypes.Comparer(implementor, keyPhysType);

            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            var keyType = leftKeySelector.ReturnType;
            var nullSafeKeyType = leftNullSafeKeySelector?.ReturnType ?? typeof(object);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.LeftMarkHashJoin.MakeGenericMethod(leftType, rightType, keyType, nullSafeKeyType, rowType),
                    leftSource,
                    rightSource,
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
        LambdaExpression NullAwareAccessor(ClrEnumerableRelImplementor implementor, PhysType physType, java.util.List keys, Type rowType)
        {
            return implementor.Translator.TranslateSelector(physType.generateNullAwareAccessor(keys, joinInfo.nullExclusionFlags), rowType);
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
        static LambdaExpression Accessor(ClrEnumerableRelImplementor implementor, PhysType physType, java.util.List keys, Type rowType)
        {
            return implementor.Translator.TranslateSelector(physType.generateAccessor(keys), rowType);
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
        Expression Predicate(ClrEnumerableRelImplementor implementor, PhysType leftPhysType, PhysType rightPhysType, Type leftType, Type rightType)
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
