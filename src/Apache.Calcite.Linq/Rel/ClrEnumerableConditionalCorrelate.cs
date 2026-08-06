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
using org.apache.calcite.rex;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="ConditionalCorrelate"/> in the <see cref="ClrEnumerableConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// A correlate carrying a condition, which is what a correlated IN, SOME or EXISTS becomes when the
    /// sub-query rules rewrite it to a mark join rather than to a plain correlate. Its join type is always
    /// LEFT_MARK, and Calcite refuses every other, so this does too.
    ///
    /// <para>New in 1.42, along with <c>JoinRelType.LEFT_MARK</c> itself.</para>
    /// </remarks>
    public class ClrEnumerableConditionalCorrelate : ConditionalCorrelate, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableConditionalCorrelate"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="correlationId"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static ClrEnumerableConditionalCorrelate Create(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableCorrelate(mq, left, right, joinType)));

            return new ClrEnumerableConditionalCorrelate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType, condition);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="correlationId"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        /// <param name="condition"></param>
        public ClrEnumerableConditionalCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, correlationId, requiredColumns, joinType, condition)
        {

        }

        /// <inheritdoc />
        public override ConditionalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition)
        {
            return new ClrEnumerableConditionalCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType, condition);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The overload without a condition, which cannot describe this node, so Calcite refuses it and so
        /// does this.
        /// </remarks>
        public override Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            throw new java.lang.RuntimeException("This method should not be called");
        }

        /// <inheritdoc />
        /// <remarks>
        /// Only a collation on the left input passes down, because the left input is always the outer loop
        /// and only it can keep an order.
        /// </remarks>
        public Pair passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, getJoinType(), getLeft().getRowType().getFieldCount(), getTraitSet())!;
        }

        /// <inheritdoc />
        public Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrEnumerableTraitsUtils.DeriveTraitsForJoin(childTraits, childId, getJoinType(), getTraitSet(), getRight().getTraitSet())!;
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.LEFT_FIRST;
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (getJoinType().name() != nameof(JoinRelType.LEFT_MARK))
                throw new java.lang.UnsupportedOperationException($"ClrEnumerableConditionalCorrelate does not support join type: {getJoinType()}");

            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getLeft(), pref);

            // not optimising, for the reason ClrEnumerableCorrelate gives: the block is translated apart from
            // the sub-plan that reads its variables
            var corrBlock = new J.BlockBuilder(false);
            var corrVarType = leftResult.PhysType.getJavaRowType();
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, corrVarType, getCorrelVariable());

            var corrParameter = Expression.Parameter(ClrTypes.Resolve(J.Primitive.box(corrVarType)), getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftResult.PhysType);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // three-valued, because a mark join's marker is null where a comparison was unknown
            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getCondition(), true);

            var leftSource = ClrEnumUtils.BoxRows(leftResult.PhysType, leftResult.Expression);
            var rightSource = ClrEnumUtils.BoxRows(rightResult.PhysType, rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightSource);

            var leftType = leftSource.Type.GetGenericArguments()[0];
            var rightType = rightSource.Type.GetGenericArguments()[0];
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

            var inner = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(leftType, rightSource.Type),
                Expression.Block(rightSource.Type, declared, body),
                corrParameter);

            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.CorrelateLeftMarkJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftSource,
                    inner,
                    predicate,
                    selector));
        }

    }

}
