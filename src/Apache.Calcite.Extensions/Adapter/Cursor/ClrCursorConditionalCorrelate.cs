using System;
using System.Linq.Expressions;
using System.Threading;

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

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="ConditionalCorrelate"/> in the <see cref="ClrCursorConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableConditionalCorrelate</c>: a correlate with a condition, which a correlated IN, SOME
    /// or EXISTS becomes when the sub-query rules rewrite it to a mark join. Only <c>LEFT_MARK</c> is
    /// implemented; as in Calcite, any other join type throws when the node is implemented.
    ///
    /// <para>The right input is opened once per left row while the cursor advances, so, as in
    /// <see cref="ClrCursorCorrelate"/>, it is visited through both hierarchies.</para>
    /// </remarks>
    public class ClrCursorConditionalCorrelate : ConditionalCorrelate, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorConditionalCorrelate"/>, deriving its collation as Calcite does.
        /// </summary>
        /// <param name="left">The left (outer) input.</param>
        /// <param name="right">The right input, which reads the correlation variable.</param>
        /// <param name="correlationId">The correlation variable bound to each left row.</param>
        /// <param name="requiredColumns">The left fields the right input reads.</param>
        /// <param name="joinType">The join type; only <c>LEFT_MARK</c> can be implemented.</param>
        /// <param name="condition">The condition that sets the mark.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorConditionalCorrelate Create(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableCorrelate(mq, left, right, joinType)));

            return new ClrCursorConditionalCorrelate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType, condition);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traits">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="left">The left (outer) input.</param>
        /// <param name="right">The right input, which reads the correlation variable.</param>
        /// <param name="correlationId">The correlation variable bound to each left row.</param>
        /// <param name="requiredColumns">The left fields the right input reads.</param>
        /// <param name="joinType">The join type; only <c>LEFT_MARK</c> can be implemented.</param>
        /// <param name="condition">The condition that sets the mark.</param>
        public ClrCursorConditionalCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, correlationId, requiredColumns, joinType, condition)
        {

        }

        /// <inheritdoc />
        public override ConditionalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType, RexNode condition)
        {
            return new ClrCursorConditionalCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType, condition);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Always throws, as in Calcite: this overload cannot carry the condition.
        /// </remarks>
        public override Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            throw new java.lang.RuntimeException("This method should not be called");
        }

        /// <inheritdoc />
        /// <remarks>
        /// Only a collation on the left input passes down: the left input is the outer loop, so only its order
        /// is preserved.
        /// </remarks>
        public Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForJoin(required, getJoinType(), getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrCursorTraitsUtils.DeriveTraitsForJoin(childTraits, childId, getJoinType(), getTraitSet(), getRight().getTraitSet());
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.LEFT_FIRST;
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (getJoinType().name() != nameof(JoinRelType.LEFT_MARK))
                throw new java.lang.UnsupportedOperationException($"ClrCursorConditionalCorrelate does not support join type: {getJoinType()}");

            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)getLeft(), pref);

            // Calcite's Rex translation reads the correlation variable registered below, so it takes
            // Calcite's physical type of the left row
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            // not optimising: an optimising builder would inline a declaration used once, and the sub-plan
            // translated separately still refers to the variable by name
            var corrBlock = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // the right input again through the other hierarchy, with its own block
            var corrBlockAsync = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlockAsync, leftCalcite);
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // three-valued: a mark is null where the comparison is unknown
            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getCondition(), true);


            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlockAsync.toBlock(), out var declaredAsync, out var bodyAsync);
            bodyAsync.Add(rightResultAsync.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            var inner = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(leftType, rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                corrParameter);

            var innerAsync = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftType, typeof(CancellationToken), rightResultAsync.Expression.Type),
                Expression.Block(rightResultAsync.Expression.Type, declaredAsync, bodyAsync),
                corrParameter,
                implementor.CancellationToken);

            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.CorrelateLeftMarkJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    inner,
                    innerAsync,
                    predicate,
                    selector));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (getJoinType().name() != nameof(JoinRelType.LEFT_MARK))
                throw new java.lang.UnsupportedOperationException($"ClrCursorConditionalCorrelate does not support join type: {getJoinType()}");

            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getLeft(), pref);

            // Calcite's Rex translation reads the correlation variable registered below, so it takes
            // Calcite's physical type of the left row
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            // not optimising: an optimising builder would inline a declaration used once, and the sub-plan
            // translated separately still refers to the variable by name
            var corrBlock = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // the right input again through the other hierarchy, with its own block
            var corrBlockSync = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlockSync, leftCalcite);
            var rightResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // three-valued: a mark is null where the comparison is unknown
            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getCondition(), true);


            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlockSync.toBlock(), out var declaredSync, out var bodySync);
            bodySync.Add(rightResultSync.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            var inner = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftType, typeof(CancellationToken), rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                corrParameter,
                implementor.CancellationToken);

            var innerSync = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(leftType, rightResultSync.Expression.Type),
                Expression.Block(rightResultSync.Expression.Type, declaredSync, bodySync),
                corrParameter);

            var selector = ClrEnumUtils.MarkJoinSelector(implementor, physType, leftResult.PhysType);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.CorrelateLeftMarkJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    innerSync,
                    inner,
                    predicate,
                    selector));
        }

    }

}
