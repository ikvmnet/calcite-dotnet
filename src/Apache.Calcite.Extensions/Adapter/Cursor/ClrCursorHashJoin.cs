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

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrCursorConvention"/> calling convention,
    /// by building a lookup of one input and probing it with the other.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableHashJoin</c>. The inner, outer and mark joins build their lookup when the node's
    /// cursor is opened and take both inputs as opens. The semi and anti joins build theirs lazily, on the first
    /// left row, as linq4j's <c>semiJoin</c> does; that happens inside an advance, so the right input is passed
    /// as an opener of each kind and each body visits it through both hierarchies.
    /// </remarks>
    public class ClrCursorHashJoin : Join, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorHashJoin"/>, deriving its collation as Calcite does.
        /// </summary>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables set by this join.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorHashJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableHashJoin(mq, left, right, joinType)));

            return new ClrCursorHashJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traits">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables set by this join.</param>
        /// <param name="joinType">The join type.</param>
        public ClrCursorHashJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrCursorHashJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left join input
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

            // as in Calcite: a join and its flipped form often cost the same, so one of them is made slightly
            // more expensive to keep the planner's choice stable
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
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
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

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            switch (joinType.name())
            {
                case nameof(JoinRelType.SEMI):
                case nameof(JoinRelType.ANTI):
                    return ImplementHashSemiJoinAsync(implementor, pref);
                case nameof(JoinRelType.LEFT_MARK):
                    return ImplementHashMarkJoinAsync(implementor, pref);
                default:
                    return ImplementHashJoinAsync(implementor, pref);
            }
        }

        /// <summary>
        /// Implements a semi or anti join, which returns left rows only.
        /// </summary>
        /// <remarks>
        /// The right input is acquired inside the advance that reads the first left row, so it is passed as an
        /// opener of each kind and is also visited through the synchronous hierarchy.
        /// </remarks>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The awaiting open of the join, whose rows are the left input's rows.</returns>
        ClrCursorAsyncResult ImplementHashSemiJoinAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);
            var rightResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);

            var physType = leftResult.PhysType;
            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;

            var keyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var leftKey = NullAwareAccessor(leftResult.PhysType, joinInfo.leftKeys);
            var rightKey = NullAwareAccessor(rightResult.PhysType, joinInfo.rightKeys);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SemiJoinAsync.MakeGenericMethod(leftType, rightType, leftKey.ReturnType),
                    leftResult.Expression,
                    implementor.Opener(rightResultSync),
                    implementor.OpenerAsync(rightResult),
                    leftKey,
                    rightKey,
                    keyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(joinType.name() == nameof(JoinRelType.ANTI)),
                    Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType)));
        }

        /// <summary>
        /// Implements a mark join, which returns every left row with a mark saying whether the right input had
        /// a match: TRUE, FALSE or UNKNOWN.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableHashJoin.implementHashMarkJoin</c>. Both predicates are three-valued, so that
        /// <c>x IN (...)</c> over a nullable column can answer UNKNOWN.
        ///
        /// <para>A null-safe key (<c>IS NOT DISTINCT FROM</c>) compares TRUE or FALSE; any other key
        /// (<c>=</c>) can compare UNKNOWN. The operator receives a selector over all keys, a selector over the
        /// null-safe keys, and whether at most one key is not null-safe.</para>
        /// </remarks>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The awaiting open of the join, whose rows are each left row followed by its marker.</returns>
        ClrCursorAsyncResult ImplementHashMarkJoinAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);

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

            // the null-aware accessor yields null where a key that is not null-safe is null, which tells the
            // operator the comparison is unknown rather than false
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

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.LeftMarkHashJoinAsync.MakeGenericMethod(leftType, rightType, keyType, nullSafeKeyType, rowType),
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
        /// Implements a join that returns fields of both inputs: inner, left, right or full.
        /// </summary>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The awaiting open of the join, whose rows are a left row and a right row combined.</returns>
        ClrCursorAsyncResult ImplementHashJoinAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);

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

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.HashJoinAsync.MakeGenericMethod(leftType, rightType, keyType, rowType),
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
        /// Implements a join that returns fields of both inputs: inner, left, right or full.
        /// </summary>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The synchronous open of the join, whose rows are a left row and a right row combined.</returns>
        ClrCursorResult ImplementHashJoin(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);

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
                Expression.Call(null,
                    ClrCursorBuiltInMethod.HashJoin.MakeGenericMethod(leftType, rightType, keyType, rowType),
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
        /// Implements a semi or anti join, which returns left rows only.
        /// </summary>
        /// <remarks>
        /// The right input is acquired inside the advance that reads the first left row, so it is passed as an
        /// opener of each kind and is also visited through the awaiting hierarchy.
        /// </remarks>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The synchronous open of the join, whose rows are the left input's rows.</returns>
        ClrCursorResult ImplementHashSemiJoin(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)right, pref);

            var physType = leftResult.PhysType;
            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;

            var keyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var leftKey = NullAwareAccessor(leftResult.PhysType, joinInfo.leftKeys);
            var rightKey = NullAwareAccessor(rightResult.PhysType, joinInfo.rightKeys);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.SemiJoin.MakeGenericMethod(leftType, rightType, leftKey.ReturnType),
                    leftResult.Expression,
                    implementor.Opener(rightResult),
                    implementor.OpenerAsync(rightResultAsync),
                    leftKey,
                    rightKey,
                    keyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(joinType.name() == nameof(JoinRelType.ANTI)),
                    Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType)));
        }

        /// <summary>
        /// Implements a mark join, which returns every left row with a mark saying whether the right input had
        /// a match: TRUE, FALSE or UNKNOWN.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableHashJoin.implementHashMarkJoin</c>. Both predicates are three-valued, so that
        /// <c>x IN (...)</c> over a nullable column can answer UNKNOWN.
        ///
        /// <para>A null-safe key (<c>IS NOT DISTINCT FROM</c>) compares TRUE or FALSE; any other key
        /// (<c>=</c>) can compare UNKNOWN. The operator receives a selector over all keys, a selector over the
        /// null-safe keys, and whether at most one key is not null-safe.</para>
        /// </remarks>
        /// <param name="implementor">The implementor, through which both inputs are visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to both inputs.</param>
        /// <returns>The synchronous open of the join, whose rows are each left row followed by its marker.</returns>
        ClrCursorResult ImplementHashMarkJoin(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)right, pref);

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

            // the null-aware accessor yields null where a key that is not null-safe is null, which tells the
            // operator the comparison is unknown rather than false
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
                Expression.Call(null,
                    ClrCursorBuiltInMethod.LeftMarkHashJoin.MakeGenericMethod(leftType, rightType, keyType, nullSafeKeyType, rowType),
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
        /// Returns a lambda reading the join key from a row, or null where a key field that is not null-safe is
        /// null.
        /// </summary>
        /// <param name="physType">The row's physical type.</param>
        /// <param name="keys">The key field ordinals.</param>
        /// <returns>The key selector.</returns>
        /// <remarks>
        /// Unlike <see cref="Accessor"/>, the key is a list even for one field, so a null in a null-safe key is a
        /// list holding null, which equals another such list; that is how <c>IS NOT DISTINCT FROM</c> matches
        /// null to null.
        /// </remarks>
        LambdaExpression NullAwareAccessor(ClrPhysType physType, java.util.List keys)
        {
            return physType.GenerateNullAwareAccessor(keys, joinInfo.nullExclusionFlags);
        }

        /// <summary>
        /// Returns a lambda reading the given fields from a row; for a single field, the field itself.
        /// </summary>
        /// <param name="physType">The row's physical type.</param>
        /// <param name="keys">The field ordinals.</param>
        /// <returns>The key selector.</returns>
        /// <remarks>
        /// Used for a mark join's null-safe keys, as Calcite does.
        /// </remarks>
        static LambdaExpression Accessor(ClrPhysType physType, java.util.List keys)
        {
            return physType.GenerateAccessor(keys);
        }

        /// <summary>
        /// Returns a lambda testing the non-equi part of the condition, or a null constant where the condition
        /// is entirely equalities.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="leftPhysType">The left input's physical type.</param>
        /// <param name="rightPhysType">The right input's physical type.</param>
        /// <param name="leftType">The left row type.</param>
        /// <param name="rightType">The right row type.</param>
        /// <returns>The predicate, typed <c>Func&lt;TLeft, TRight, bool&gt;</c>.</returns>
        Expression Predicate(ClrCursorRelImplementor implementor, ClrPhysType leftPhysType, ClrPhysType rightPhysType, Type leftType, Type rightType)
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
