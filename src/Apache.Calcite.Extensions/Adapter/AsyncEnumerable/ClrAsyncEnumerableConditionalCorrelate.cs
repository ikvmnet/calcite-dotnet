using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="ConditionalCorrelate"/> in the <see cref="ClrAsyncEnumerableConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// A correlate carrying a condition, which is what a correlated IN, SOME or EXISTS becomes when the
    /// sub-query rules rewrite it to a mark join rather than to a plain correlate. Its join type is always
    /// LEFT_MARK, and Calcite refuses every other, so this does too.
    ///
    /// <para>New in 1.42, along with <c>JoinRelType.LEFT_MARK</c> itself.</para>
    /// </remarks>
    public class ClrAsyncEnumerableConditionalCorrelate : ConditionalCorrelate, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableConditionalCorrelate"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="correlationId"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableConditionalCorrelate Create(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableCorrelate(mq, left, right, joinType)));

            return new ClrAsyncEnumerableConditionalCorrelate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType, condition);
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
        public ClrAsyncEnumerableConditionalCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, correlationId, requiredColumns, joinType, condition)
        {

        }

        /// <inheritdoc />
        public override ConditionalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition)
        {
            return new ClrAsyncEnumerableConditionalCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType, condition);
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
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (getJoinType().name() != nameof(JoinRelType.LEFT_MARK))
                throw new java.lang.UnsupportedOperationException($"ClrAsyncEnumerableConditionalCorrelate does not support join type: {getJoinType()}");

            var leftResult = implementor.VisitChild(this, 0, (ClrAsyncEnumerableRel)getLeft(), pref);

            // not optimising, for the reason ClrAsyncEnumerableCorrelate gives: the block is translated apart from
            // the sub-plan that reads its variables
            var corrBlock = new J.BlockBuilder(false);
            // the getter registered below is one Calcite's Rex translation reads the outer row through,
            // so it is given their physical type, built here from the three values ours carries
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChild(this, 1, (ClrAsyncEnumerableRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // three-valued, because a mark join's marker is null where a comparison was unknown
            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getCondition(), true);


            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            var inner = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(leftType, rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                corrParameter);

            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.CorrelateLeftMarkJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    inner,
                    predicate,
                    selector));
        }

    }

}
