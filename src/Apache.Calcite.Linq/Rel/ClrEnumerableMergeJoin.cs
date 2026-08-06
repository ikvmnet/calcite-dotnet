using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

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

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrEnumerableConvention"/> calling convention,
    /// by walking two inputs that are already sorted on the join key.
    /// </summary>
    /// <remarks>
    /// Only ever chosen where the inputs carry a collation, which is where a merge is cheaper than building a
    /// lookup. `EnumerableMergeJoin` is the same, and its trait methods are most of the class: a required
    /// collation may be a subset or a superset of the join keys, and on either side, and each case pushes a
    /// different collation down.
    /// </remarks>
    public class ClrEnumerableMergeJoin : Join, ClrEnumerableRel
    {

        /// <summary>
        /// Returns whether a merge join can answer a join of this type.
        /// </summary>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static bool IsMergeJoinSupported(JoinRelType joinType)
        {
            return ClrEnumerableDefaults.IsMergeJoinSupported(ClrEnumUtils.ToLinq4jJoinType(joinType));
        }

        /// <summary>
        /// Creates a <see cref="ClrEnumerableMergeJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="leftKeys"></param>
        /// <param name="rightKeys"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrEnumerableMergeJoin Create(RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, JoinRelType joinType)
        {
            var cluster = right.getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance);

            if (traitSet.isEnabled(RelCollationTraitDef.INSTANCE))
            {
                var mq = cluster.getMetadataQuery();
                var collations = RelMdCollation.mergeJoin(mq, left, right, leftKeys, rightKeys, joinType);
                traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => collations));
            }

            return new ClrEnumerableMergeJoin(cluster, traitSet, left, right, condition, com.google.common.collect.ImmutableSet.of(), joinType);
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
        /// The join keys, considering EQUALS alone.
        /// </summary>
        /// <remarks>
        /// Hides <c>Join.joinInfo</c>, as Calcite's field of the same name does, so every use of the name in
        /// this class is the strict-equality one.
        /// </remarks>
        new readonly JoinInfo joinInfo;

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
        public ClrEnumerableMergeJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {
            // the algorithm stops when either key is null, and IS NOT DISTINCT FROM calls two nulls equal,
            // so a condition carrying one must not become a join key
            joinInfo = JoinInfo.createWithStrictEquality(left, right, condition);

            if (getConvention() is not ClrEnumerableConvention)
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

            // RelCompositeTrait cannot yet say that a merge join has a collation on [a, d] or on [b, c], so a
            // join of foo.a = bar.c AND foo.b = bar.d is legitimate and is not represented
            if (RelCollations.collationsContainKeysOrderless(collations, joinInfo.leftKeys) == false
                && RelCollations.collationsContainKeysOrderless(collations, rightKeys) == false
                && RelCollations.keysContainCollationsOrderless(joinInfo.leftKeys, collations) == false
                && RelCollations.keysContainCollationsOrderless(rightKeys, collations) == false)
            {
                if (isDistinct)
                    throw new java.lang.RuntimeException("wrong collation for mergejoin");
            }

            if (IsMergeJoinSupported(joinType) == false)
                throw new java.lang.UnsupportedOperationException($"ClrEnumerableMergeJoin unsupported for join type {joinType}");
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrEnumerableMergeJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The required collation may be a subset or a superset of the join keys, and may be on either input.
        /// Each of the six cases pushes a different collation to the two inputs; anything else is refused.
        ///
        /// <para>One line is not Calcite's, and it is the first. `EnumerableMergeJoin` uses <c>required</c>
        /// as the node's own trait set, so a subset of another convention asking it to pass through gets a
        /// node of this convention wearing that one's — which the planner then refuses to register, and which
        /// <c>TopDownRuleDriver.convert</c> asserts cannot happen. Refusing a foreign convention outright is
        /// the guard; everything after it is the port.</para>
        /// </remarks>
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            if (required.getConvention() != getConvention())
                return null!;

            // the required collation keys can be a subset or a superset of the merge join keys
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
                // the sort keys are the left join keys, so every collation passes through as it is
                var mapping = BuildMapping(true);
                var rightCollation = RexUtil.apply(mapping, collation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required, required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(leftKeys, collation))
            {
                // the sort keys are a subset of the left join keys, so the collation is extended to sort them all
                collation = ExtendCollation(collation, leftKeys);
                var mapping = BuildMapping(true);
                var rightCollation = RexUtil.apply(mapping, collation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(collation), required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(collation, leftKeys) && AllLessThan(reqKeys, leftInputFieldCount))
            {
                // the sort keys are a superset of the left join keys, the join keys are a prefix of them
                // whatever the order, and every sort key is from the left input
                var mapping = BuildMapping(true);
                var rightCollation = RexUtil.apply(mapping, IntersectCollationAndJoinKey(collation, joinInfo.leftKeys));

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required, required.replace(rightCollation)));
            }

            if (reqKeySet.equals(rightKeySet))
            {
                // the sort keys are the right join keys, so every collation passes through as it is
                var rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
                var mapping = BuildMapping(false);
                var leftCollation = RexUtil.apply(mapping, rightCollation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(leftCollation), required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(rightKeys, collation))
            {
                // the sort keys are a subset of the right join keys
                collation = ExtendCollation(collation, rightKeys);
                var rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
                var mapping = BuildMapping(false);
                var leftCollation = RexUtil.apply(mapping, rightCollation);

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(leftCollation), required.replace(rightCollation)));
            }

            if (RelCollations.containsOrderless(collation, rightKeys) && AllAtLeast(reqKeys, leftInputFieldCount))
            {
                // the sort keys are a superset of the right join keys, and every one of them is from the right
                var rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
                var mapping = BuildMapping(false);
                var leftCollation = RexUtil.apply(mapping, IntersectCollationAndJoinKey(rightCollation, joinInfo.rightKeys));

                return org.apache.calcite.util.Pair.of(required,
                    com.google.common.collect.ImmutableList.of(required.replace(leftCollation), required.replace(rightCollation)));
            }

            return null!;
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
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            var keyCount = joinInfo.leftKeys.size();
            var collation = GetCollation(childTraits);
            var colCount = collation.getFieldCollations().size();
            if (colCount < keyCount || keyCount == 0)
                return null!;

            if (colCount > keyCount)
                collation = RelCollations.of(collation.getFieldCollations().subList(0, keyCount));

            var sourceKeys = childId == 0 ? joinInfo.leftKeys : joinInfo.rightKeys;
            var keySet = ImmutableBitSet.of(sourceKeys);
            var childCollationKeys = ImmutableBitSet.of(RelCollations.ordinals(collation));
            if (childCollationKeys.equals(keySet) == false)
                return null!;

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
        /// <param name="left2Right"></param>
        /// <returns></returns>
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
        /// Extends a collation with the keys it does not already order by.
        /// </summary>
        /// <param name="collation"></param>
        /// <param name="keys"></param>
        /// <returns></returns>
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
        /// <param name="collation">collation defined on the join</param>
        /// <param name="joinKeys">the join keys</param>
        /// <returns></returns>
        /// <remarks>
        /// Ordering a join of <c>foo.a = bar.a AND foo.c = bar.c</c> by <c>bar.a, bar.c, bar.b</c> pushes all
        /// three to <c>bar</c>, and only <c>a, c</c> to <c>foo</c>, because <c>b</c> is not a join key.
        /// </remarks>
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
        public override RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            var rowCount = mq.getRowCount(this).doubleValue();

            // a merge join is cheaper than a hash join, and the inputs are already sorted
            if (joinType.name() == nameof(JoinRelType.SEMI) || joinType.name() == nameof(JoinRelType.ANTI))
                rowCount = RelMdUtil.addEpsilon(rowCount);
            else if (RelNodes.COMPARATOR.compare(getLeft(), getRight()) > 0)
                rowCount = RelMdUtil.addEpsilon(rowCount);

            return planner.getCostFactory().makeCost(rowCount, 0, 0);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getLeft(), pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getRight(), pref);

            var physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.PreferArray());

            // Calcite types these off the physical type and does not box them. It cannot go wrong there:
            // there is no Enumerable<int> in Java, so the sequence and a parameter typed int cannot disagree.
            // Here they can. The sequences below are boxed because a join must box — the selector and
            // predicate Calcite builds are against boxed rows, and an outer join hands the selector a null —
            // and the key selector is a lambda over one row of the boxed sequence, so this has to be the same
            // decision. Only a one-column input tells the two apart, every wider row being a reference
            // already, which is why Primitive.box is a no-op for all of them.
            var left_ = J.Expressions.parameter(J.Primitive.box(leftResult.PhysType.getJavaRowType()), "left");
            var right_ = J.Expressions.parameter(J.Primitive.box(rightResult.PhysType.getJavaRowType()), "right");

            // each key field is read at the type the two sides have in common, so that one comparator can
            // order both inputs
            var leftExpressions = new java.util.ArrayList();
            var rightExpressions = new java.util.ArrayList();
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
            {
                var leftIndex = ((java.lang.Integer)joinInfo.leftKeys.get(i)).intValue();
                var rightIndex = ((java.lang.Integer)joinInfo.rightKeys.get(i)).intValue();

                var leftType = ((RelDataTypeField)getLeft().getRowType().getFieldList().get(leftIndex)).getType();
                var rightType = ((RelDataTypeField)getRight().getRowType().getFieldList().get(rightIndex)).getType();
                var keyType = typeFactory.leastRestrictive(com.google.common.collect.ImmutableList.of(leftType, rightType))
                    ?? throw new java.lang.NullPointerException($"leastRestrictive returns null for {leftType} and {rightType}");
                var keyClass = typeFactory.getJavaClass(keyType);

                leftExpressions.add(EnumUtils.convert(leftResult.PhysType.fieldReference(left_, leftIndex), keyClass));
                rightExpressions.add(EnumUtils.convert(rightResult.PhysType.fieldReference(right_, rightIndex), keyClass));
            }

            var leftKeyPhysType = leftResult.PhysType.project(joinInfo.leftKeys, JavaRowFormat.LIST);
            var rightKeyPhysType = rightResult.PhysType.project(joinInfo.rightKeys, JavaRowFormat.LIST);

            // the rows are boxed for the two reasons a hash join boxes them: the selector and the predicate
            // Calcite builds are against boxed rows, and a LEFT join hands the selector a null right row,
            // which a primitive cannot be. The element type is read off the sequence rather than off the
            // physical type, because those two part company where a one-column input is a scalar.
            var leftSource = ClrEnumUtils.BoxRows(leftResult.PhysType, leftResult.Expression);
            var rightSource = ClrEnumUtils.BoxRows(rightResult.PhysType, rightResult.Expression);
            var leftType_ = leftSource.Type.GetGenericArguments()[0];
            var rightType_ = rightSource.Type.GetGenericArguments()[0];
            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

            var leftKey = implementor.Translator.TranslateSelector(J.Expressions.lambda(leftKeyPhysType.record(leftExpressions), [left_]), leftType_);
            var rightKey = implementor.Translator.TranslateSelector(J.Expressions.lambda(rightKeyPhysType.record(rightExpressions), [right_]), rightType_);

            var predicate = Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType_, rightType_);
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            // the keys are sorted ascending with nulls last, whatever the collation the inputs carry, because
            // that is what the algorithm walks
            var fieldCollations = new java.util.ArrayList(joinInfo.leftKeys.size());
            for (int i = 0; i < joinInfo.leftKeys.size(); i++)
                fieldCollations.add(new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));

            // the comparator's key is nullable where either side's is, so that a null from one input is
            // ordered against a value from the other
            var typeBuilder = typeFactory.builder();
            var leftFields = leftKeyPhysType.getRowType().getFieldList();
            var rightFields = rightKeyPhysType.getRowType().getFieldList();
            for (int i = 0; i < leftFields.size(); i++)
            {
                var leftField = (RelDataTypeField)leftFields.get(i);
                var rightField = (RelDataTypeField)rightFields.get(i);
                typeBuilder.add(leftField.getName(),
                    typeFactory.createTypeWithNullability(leftField.getType(), leftField.getType().isNullable() || rightField.getType().isNullable()));
            }

            var comparatorPhysType = PhysTypeImpl.of(typeFactory, typeBuilder.build(), JavaRowFormat.LIST);
            var comparator = implementor.Translator.Translate(comparatorPhysType.generateMergeJoinComparator(RelCollations.of(fieldCollations)));

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.MergeJoin.MakeGenericMethod(leftType_, rightType_, leftKey.ReturnType, rowType),
                    leftSource,
                    rightSource,
                    leftKey,
                    rightKey,
                    predicate,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType)),
                    comparator,
                    ClrPhysTypes.Comparer(implementor, leftKeyPhysType)));
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

            if (joinInfo.nonEquiConditions.isEmpty())
                return Expression.Constant(null, type);

            var nonEqui = RexUtil.composeConjunction(getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
            if (nonEqui == null)
                return Expression.Constant(null, type);

            return ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftPhysType, rightPhysType, nonEqui);
        }

    }

}
