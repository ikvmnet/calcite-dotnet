using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.util;
using org.apache.calcite.util.mapping;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrCursorConvention"/> calling convention that
    /// merges two inputs sorted on the join keys.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableMergeJoin</c>. Both inputs must be sorted ascending, nulls last, on their join keys.
    /// The node passes a required collation through to its inputs and derives its own collation from theirs.
    /// </remarks>
    public class ClrCursorMergeJoin : Join, ClrCursorRel
    {

        /// <summary>
        /// Returns whether a merge join supports a join type.
        /// </summary>
        /// <param name="joinType">The join type.</param>
        /// <returns><see langword="true"/> if <see cref="ClrCursorMergeJoin"/> can implement a join of that type.</returns>
        public static bool IsMergeJoinSupported(JoinRelType joinType)
        {
            return ClrCursorDefaults.IsMergeJoinSupported(ClrEnumUtils.ToLinq4jJoinType(joinType));
        }

        /// <summary>
        /// Creates a <see cref="ClrCursorMergeJoin"/>, deriving its collation from the inputs and keys.
        /// </summary>
        /// <param name="left">The left input, sorted on <paramref name="leftKeys"/>.</param>
        /// <param name="right">The right input, sorted on <paramref name="rightKeys"/>.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="leftKeys">The ordinals of the join keys in the left input.</param>
        /// <param name="rightKeys">The ordinals of the join keys in the right input.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>The new join, with no variables set.</returns>
        public static ClrCursorMergeJoin Create(RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType joinType)
        {
            var cluster = right.getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance);

            if (traitSet.isEnabled(RelCollationTraitDef.INSTANCE))
            {
                var mq = cluster.getMetadataQuery();
                var collations = RelMdCollation.mergeJoin(mq, left, right, leftKeys, rightKeys, joinType);
                traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => collations));
            }

            return new ClrCursorMergeJoin(cluster, traitSet, left, right, condition, com.google.common.collect.ImmutableSet.of(), joinType);
        }

        static RelCollation GetCollation(RelTraitSet traits)
        {
            return traits.getCollation() ?? throw new java.lang.NullPointerException($"no collation trait in {traits}");
        }

        static java.util.List GetCollations(RelTraitSet traits)
        {
            return traits.getTraits(RelCollationTraitDef.INSTANCE) ?? throw new java.lang.NullPointerException($"no collation trait in {traits}");
        }

        /// <summary>
        /// The join keys, taken from <c>=</c> conditions only; <c>IS NOT DISTINCT FROM</c> is left in the
        /// non-equi conditions.
        /// </summary>
        /// <remarks>
        /// Hides <c>Join.joinInfo</c>, as <c>EnumerableMergeJoin</c>'s field of the same name does.
        /// </remarks>
        new readonly JoinInfo joinInfo;

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> derives the trait set; this constructor takes it as
        /// given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traits">The node's traits, in <see cref="ClrCursorConvention"/> with a non-empty collation.</param>
        /// <param name="left">The left input, sorted on its join keys.</param>
        /// <param name="right">The right input, sorted on its join keys.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables set by the join.</param>
        /// <param name="joinType">The join type; see <see cref="IsMergeJoinSupported"/>.</param>
        /// <exception cref="java.lang.RuntimeException">An input is not sorted on distinct join keys, or the
        /// node's collation does not match the keys.</exception>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="traits"/> has no collation.</exception>
        /// <exception cref="java.lang.UnsupportedOperationException">The join type is not supported.</exception>
        public ClrCursorMergeJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {
            // the algorithm stops when either key is null, and IS NOT DISTINCT FROM treats two nulls as
            // equal, so it must not supply a join key
            joinInfo = JoinInfo.createWithStrictEquality(left, right, condition);

            if (getConvention() is not ClrCursorConvention)
                throw new java.lang.AssertionError();

            var leftCollations = GetCollations(left.getTraitSet());
            var rightCollations = GetCollations(right.getTraitSet());

            // where a join key is repeated the check does not apply, as in t1.a = t2.b AND t1.a = t2.c
            var isDistinct = Util.isDistinct(joinInfo.leftKeys) && Util.isDistinct(joinInfo.rightKeys);

            if (RelCollations.collationsContainKeysOrderless(leftCollations, joinInfo.leftKeys) == false
                || RelCollations.collationsContainKeysOrderless(rightCollations, joinInfo.rightKeys) == false)
            {
                if (isDistinct)
                    throw new java.lang.RuntimeException("wrong collation in left or right input");
            }

            var collations = traits.getTraits(RelCollationTraitDef.INSTANCE) ?? throw new java.lang.NullPointerException("collations");
            if (collations.isEmpty())
                throw new java.lang.IllegalArgumentException();

            var rightKeys = joinInfo.rightKeys.incr(left.getRowType().getFieldCount());

            // RelCompositeTrait cannot express that a join of foo.a = bar.c AND foo.b = bar.d is sorted on
            // [a, d] or on [b, c], so such a collation is not checked here
            if (RelCollations.collationsContainKeysOrderless(collations, joinInfo.leftKeys) == false
                && RelCollations.collationsContainKeysOrderless(collations, rightKeys) == false
                && RelCollations.keysContainCollationsOrderless(joinInfo.leftKeys, collations) == false
                && RelCollations.keysContainCollationsOrderless(rightKeys, collations) == false)
            {
                if (isDistinct)
                    throw new java.lang.RuntimeException("wrong collation for mergejoin");
            }

            if (IsMergeJoinSupported(joinType) == false)
                throw new java.lang.UnsupportedOperationException($"ClrCursorMergeJoin unsupported for join type {joinType}");
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrCursorMergeJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Mirrors <c>EnumerableMergeJoin.passThroughTraits</c>. The required collation may be a subset or a
        /// superset of the join keys of either input; each of the six cases pushes a different collation to the
        /// two inputs, and any other collation is refused.
        ///
        /// <para>The first check, refusing a required trait set of another convention, is not in Calcite.
        /// Calcite returns <c>required</c> as the node's own trait set, so a request from another convention
        /// would produce a node of this convention carrying that convention, which the planner cannot
        /// register.</para>
        /// </remarks>
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            if (required.getConvention() != getConvention())
                return null;

            var collation = GetCollation(required);
            var leftInputFieldCount = getLeft().getRowType().getFieldCount();

            var reqKeys = RelCollations.ordinals(collation);
            var leftKeys = joinInfo.leftKeys.toIntegerList();
            var rightKeys = joinInfo.rightKeys.incr(leftInputFieldCount).toIntegerList();

            var reqKeySet = ImmutableBitSet.of(reqKeys);
            var leftKeySet = ImmutableBitSet.of(joinInfo.leftKeys);
            var rightKeySet = ImmutableBitSet.of(joinInfo.rightKeys).shift(leftInputFieldCount);

            if (reqKeySet.equals(leftKeySet))
            {
                // the sort keys are exactly the left join keys: pass the collation to both sides as it is
                var mapping = BuildMapping(true);
                var rightCollation = RexUtil.apply(mapping, collation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required, required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(leftKeys, collation))
            {
                // the sort keys are a subset of the left join keys: extend the collation to all of them
                collation = ExtendCollation(collation, leftKeys);
                var mapping = BuildMapping(true);
                var rightCollation = RexUtil.apply(mapping, collation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(collation), required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(collation, leftKeys) && AllLessThan(reqKeys, leftInputFieldCount))
            {
                // the sort keys are a superset of the left join keys, which form a prefix of them in some
                // order, and every sort key is from the left input
                var mapping = BuildMapping(true);
                var rightCollation = RexUtil.apply(mapping, IntersectCollationAndJoinKey(collation, joinInfo.leftKeys));

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required, required.replace(rightCollation)));
            }

            if (reqKeySet.equals(rightKeySet))
            {
                // the sort keys are exactly the right join keys: pass the collation to both sides as it is
                var rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
                var mapping = BuildMapping(false);
                var leftCollation = RexUtil.apply(mapping, rightCollation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(leftCollation), required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(rightKeys, collation))
            {
                // the sort keys are a subset of the right join keys: extend the collation to all of them
                collation = ExtendCollation(collation, rightKeys);
                var rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
                var mapping = BuildMapping(false);
                var leftCollation = RexUtil.apply(mapping, rightCollation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(leftCollation), required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(collation, rightKeys) && AllAtLeast(reqKeys, leftInputFieldCount))
            {
                // the sort keys are a superset of the right join keys, and every sort key is from the right input
                var rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
                var mapping = BuildMapping(false);
                var leftCollation = RexUtil.apply(mapping, IntersectCollationAndJoinKey(rightCollation, joinInfo.rightKeys));

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(leftCollation), required.replace(rightCollation)));
            }

            return null;
        }

        static bool AllLessThan(java.util.List keys, int bound)
        {
            for (int i = 0; i < keys.size(); i++)
                if (((java.lang.Integer)keys.get(i)).intValue() >= bound)
                    return false;

            return true;
        }

        static bool AllAtLeast(java.util.List keys, int bound)
        {
            for (int i = 0; i < keys.size(); i++)
                if (((java.lang.Integer)keys.get(i)).intValue() < bound)
                    return false;

            return true;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            var keyCount = joinInfo.leftKeys.size();
            var collation = GetCollation(childTraits);
            var colCount = collation.getFieldCollations().size();
            if (colCount < keyCount || keyCount == 0)
                return null;

            if (colCount > keyCount)
                collation = RelCollations.of(collation.getFieldCollations().subList(0, keyCount));

            var sourceKeys = childId == 0 ? joinInfo.leftKeys : joinInfo.rightKeys;
            var keySet = ImmutableBitSet.of(sourceKeys);
            var childCollationKeys = ImmutableBitSet.of(RelCollations.ordinals(collation));
            if (childCollationKeys.equals(keySet) == false)
                return null;

            var mapping = BuildMapping(childId == 0);
            var targetCollation = RexUtil.apply(mapping, collation);

            if (childId == 0)
            {
                // traits from the left child
                var joinTraits = getTraitSet().replace(collation);

                return org.apache.calcite.util.Pair.of(joinTraits,
                    com.google.common.collect.ImmutableList.of(childTraits, getRight().getTraitSet().replace(targetCollation)));
            }
            else
            {
                // traits from the right child
                var joinTraits = getTraitSet().replace(targetCollation);

                return org.apache.calcite.util.Pair.of(joinTraits,
                    com.google.common.collect.ImmutableList.of(joinTraits, childTraits.replace(collation)));
            }
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.BOTH;
        }

        /// <summary>
        /// Returns the mapping from one input's join keys to the other's.
        /// </summary>
        /// <param name="left2Right">Whether to map from the left input's fields to the right's.</param>
        /// <returns>A target mapping from each join key field of the source input to the matching key field of the other.</returns>
        Mappings.TargetMapping BuildMapping(bool left2Right)
        {
            var sourceKeys = left2Right ? joinInfo.leftKeys : joinInfo.rightKeys;
            var targetKeys = left2Right ? joinInfo.rightKeys : joinInfo.leftKeys;

            var keyMap = new java.util.HashMap();
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
                keyMap.put(sourceKeys.get(i), targetKeys.get(i));

            return Mappings.target(keyMap,
                (left2Right ? getLeft() : getRight()).getRowType().getFieldCount(),
                (left2Right ? getRight() : getLeft()).getRowType().getFieldCount());
        }

        /// <summary>
        /// Appends to a collation, in ascending key order, the keys it does not already sort by.
        /// </summary>
        /// <param name="collation">The collation to extend.</param>
        /// <param name="keys">The join keys, as a list of <c>java.lang.Integer</c> field indexes.</param>
        /// <returns><paramref name="collation"/> followed by an ascending field collation for each key it lacks.</returns>
        static RelCollation ExtendCollation(RelCollation collation, java.util.List keys)
        {
            var fieldsForNewCollation = new java.util.ArrayList(keys.size());
            fieldsForNewCollation.addAll(collation.getFieldCollations());

            var keysBitset = ImmutableBitSet.of(keys);
            var colKeysBitset = ImmutableBitSet.of(collation.getKeys());
            var exceptBitset = keysBitset.except(colKeysBitset);

            for (var i = exceptBitset.iterator(); i.hasNext();)
                fieldsForNewCollation.add(new RelFieldCollation(((java.lang.Integer)i.next()).intValue()));

            return RelCollations.of(fieldsForNewCollation);
        }

        /// <summary>
        /// Drops from a collation the fields that are not join keys.
        /// </summary>
        /// <param name="collation">A collation on one input.</param>
        /// <param name="joinKeys">That input's join keys.</param>
        /// <remarks>
        /// Ordering a join of <c>foo.a = bar.a AND foo.c = bar.c</c> by <c>bar.a, bar.c, bar.b</c> pushes all
        /// three to <c>bar</c>, and only <c>a, c</c> to <c>foo</c>, because <c>b</c> is not a join key.
        /// </remarks>
        /// <returns>The field collations of <paramref name="collation"/> that are on join keys, in their original order.</returns>
        static RelCollation IntersectCollationAndJoinKey(RelCollation collation, ImmutableIntList joinKeys)
        {
            var fieldCollations = new java.util.ArrayList();
            for (int i = 0; i < collation.getFieldCollations().size(); i++)
            {
                var rf = (RelFieldCollation)collation.getFieldCollations().get(i);
                if (joinKeys.contains(java.lang.Integer.valueOf(rf.getFieldIndex())))
                    fieldCollations.add(rf);
            }

            return RelCollations.of(fieldCollations);
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            // the inputs are already sorted and their sort is costed below this node, so the join costs its
            // input and output rows, as in EnumerableMergeJoin
            var rightRowCount = mq.getRowCount(getRight()).doubleValue();
            var leftRowCount = mq.getRowCount(getLeft()).doubleValue();
            var rowCount = mq.getRowCount(this).doubleValue();
            var d = leftRowCount + rightRowCount + rowCount;
            return planner.getCostFactory().makeCost(d, 0, 0);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)getLeft(), pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferArray());

            // the rows are boxed: the selector and predicate are built against boxed rows, and an outer join
            // hands the selector a null row. Java needs no such decision because its sequences are always of
            // references; here the key selectors' parameters must match the cursors' element types, which
            // differ from an unboxed row type only for a one-column input
            var leftType_ = leftResult.PhysType.RowType;
            var rightType_ = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var left_ = Expression.Parameter(leftType_, "left");
            var right_ = Expression.Parameter(rightType_, "right");

            // each key field is read at the type the two sides have in common, so that one comparator can
            // order both inputs
            var leftExpressions = new List<Expression>();
            var rightExpressions = new List<Expression>();
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
            {
                var leftIndex = ((java.lang.Integer)joinInfo.leftKeys.get(i)).intValue();
                var rightIndex = ((java.lang.Integer)joinInfo.rightKeys.get(i)).intValue();

                var leftType = ((RelDataTypeField)getLeft().getRowType().getFieldList().get(leftIndex)).getType();
                var rightType = ((RelDataTypeField)getRight().getRowType().getFieldList().get(rightIndex)).getType();
                var keyType = typeFactory.leastRestrictive(com.google.common.collect.ImmutableList.of(leftType, rightType))
                    ?? throw new java.lang.NullPointerException($"leastRestrictive returns null for {leftType} and {rightType}");
                var keyClass = ClrTypes.Resolve(typeFactory.getJavaClass(keyType));

                leftExpressions.Add(ClrEnumUtils.Convert(leftResult.PhysType.FieldReference(left_, leftIndex), keyClass));
                rightExpressions.Add(ClrEnumUtils.Convert(rightResult.PhysType.FieldReference(right_, rightIndex), keyClass));
            }

            var leftKeyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var rightKeyPhysType = rightResult.PhysType.Project(joinInfo.rightKeys, JavaRowFormat.LIST);

            // Calcite forces Function1 here because linq4j would deduce Predicate1 for a single BOOLEAN key;
            // Expression.Lambda always infers a Func<>, so a bool key already binds TKey to bool
            var leftKey = Expression.Lambda(leftKeyPhysType.Record(leftExpressions), left_);
            var rightKey = Expression.Lambda(rightKeyPhysType.Record(rightExpressions), right_);

            var predicate = Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType_, rightType_);
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            // the comparator orders keys ascending, nulls last, which is the order the inputs are required to have
            var fieldCollations = new java.util.ArrayList(joinInfo.leftKeys.size());
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
                fieldCollations.add(new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));

            // the comparator's key is nullable where either side's is, so that a null from one input is
            // ordered against a value from the other
            var typeBuilder = typeFactory.builder();
            var leftFields = leftKeyPhysType.RelRowType.getFieldList();
            var rightFields = rightKeyPhysType.RelRowType.getFieldList();
            for (int i = 0; i < leftFields.size(); i++)
            {
                var leftField = (RelDataTypeField)leftFields.get(i);
                var rightField = (RelDataTypeField)rightFields.get(i);
                typeBuilder.add(leftField.getName(),
                    typeFactory.createTypeWithNullability(leftField.getType(), leftField.getType().isNullable() || rightField.getType().isNullable()));
            }

            var comparatorPhysType = ClrPhysTypeImpl.Of(typeFactory, typeBuilder.build(), JavaRowFormat.LIST);
            var comparator = comparatorPhysType.GenerateMergeJoinComparator(RelCollations.of(fieldCollations));

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.MergeJoin.MakeGenericMethod(leftType_, rightType_, leftKey.ReturnType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKey,
                    rightKey,
                    predicate,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType)),
                    comparator,
                    leftKeyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getLeft(), pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferArray());

            // the rows are boxed: the selector and predicate are built against boxed rows, and an outer join
            // hands the selector a null row. Java needs no such decision because its sequences are always of
            // references; here the key selectors' parameters must match the cursors' element types, which
            // differ from an unboxed row type only for a one-column input
            var leftType_ = leftResult.PhysType.RowType;
            var rightType_ = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            var left_ = Expression.Parameter(leftType_, "left");
            var right_ = Expression.Parameter(rightType_, "right");

            // each key field is read at the type the two sides have in common, so that one comparator can
            // order both inputs
            var leftExpressions = new List<Expression>();
            var rightExpressions = new List<Expression>();
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
            {
                var leftIndex = ((java.lang.Integer)joinInfo.leftKeys.get(i)).intValue();
                var rightIndex = ((java.lang.Integer)joinInfo.rightKeys.get(i)).intValue();

                var leftType = ((RelDataTypeField)getLeft().getRowType().getFieldList().get(leftIndex)).getType();
                var rightType = ((RelDataTypeField)getRight().getRowType().getFieldList().get(rightIndex)).getType();
                var keyType = typeFactory.leastRestrictive(com.google.common.collect.ImmutableList.of(leftType, rightType))
                    ?? throw new java.lang.NullPointerException($"leastRestrictive returns null for {leftType} and {rightType}");
                var keyClass = ClrTypes.Resolve(typeFactory.getJavaClass(keyType));

                leftExpressions.Add(ClrEnumUtils.Convert(leftResult.PhysType.FieldReference(left_, leftIndex), keyClass));
                rightExpressions.Add(ClrEnumUtils.Convert(rightResult.PhysType.FieldReference(right_, rightIndex), keyClass));
            }

            var leftKeyPhysType = leftResult.PhysType.Project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var rightKeyPhysType = rightResult.PhysType.Project(joinInfo.rightKeys, JavaRowFormat.LIST);

            // Calcite forces Function1 here because linq4j would deduce Predicate1 for a single BOOLEAN key;
            // Expression.Lambda always infers a Func<>, so a bool key already binds TKey to bool
            var leftKey = Expression.Lambda(leftKeyPhysType.Record(leftExpressions), left_);
            var rightKey = Expression.Lambda(rightKeyPhysType.Record(rightExpressions), right_);

            var predicate = Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType_, rightType_);
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            // the comparator orders keys ascending, nulls last, which is the order the inputs are required to have
            var fieldCollations = new java.util.ArrayList(joinInfo.leftKeys.size());
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
                fieldCollations.add(new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));

            // the comparator's key is nullable where either side's is, so that a null from one input is
            // ordered against a value from the other
            var typeBuilder = typeFactory.builder();
            var leftFields = leftKeyPhysType.RelRowType.getFieldList();
            var rightFields = rightKeyPhysType.RelRowType.getFieldList();
            for (int i = 0; i < leftFields.size(); i++)
            {
                var leftField = (RelDataTypeField)leftFields.get(i);
                var rightField = (RelDataTypeField)rightFields.get(i);
                typeBuilder.add(leftField.getName(),
                    typeFactory.createTypeWithNullability(leftField.getType(), leftField.getType().isNullable() || rightField.getType().isNullable()));
            }

            var comparatorPhysType = ClrPhysTypeImpl.Of(typeFactory, typeBuilder.build(), JavaRowFormat.LIST);
            var comparator = comparatorPhysType.GenerateMergeJoinComparator(RelCollations.of(fieldCollations));

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.MergeJoinAsync.MakeGenericMethod(leftType_, rightType_, leftKey.ReturnType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKey,
                    rightKey,
                    predicate,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType)),
                    comparator,
                    leftKeyPhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
        }

        /// <summary>
        /// Returns a <c>Func&lt;left, right, bool&gt;</c> lambda testing the non-equi part of the condition, or
        /// a null constant of that delegate type where there is none.
        /// </summary>
        /// <param name="implementor">The implementor that translates the condition.</param>
        /// <param name="leftPhysType">The physical type of the left input's rows.</param>
        /// <param name="rightPhysType">The physical type of the right input's rows.</param>
        /// <param name="leftType">The CLR type of a left row, the delegate's first parameter type.</param>
        /// <param name="rightType">The CLR type of a right row, the delegate's second parameter type.</param>
        /// <returns>The predicate lambda, or a null constant of the same delegate type.</returns>
        Expression Predicate(ClrCursorRelImplementor implementor, ClrPhysType leftPhysType, ClrPhysType rightPhysType, Type leftType, Type rightType)
        {
            var type = typeof(Func<,,>).MakeGenericType(leftType, rightType, typeof(bool));

            if (joinInfo.nonEquiConditions.isEmpty())
                return Expression.Constant(null, type);

            var nonEqui = RexUtil.composeConjunction(getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
            if (nonEqui == null)
                return Expression.Constant(null, type);

            return ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftPhysType, rightPhysType, nonEqui);
        }

    }

}
