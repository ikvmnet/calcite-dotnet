using System;
using System.Linq.Expressions;
using System.Threading;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Correlate"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention, by running the right input once per row of the left.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableCorrelate</c>. The right input is opened once per left row while the cursor
    /// advances, synchronously or awaiting according to how that advance was called, so each body visits it
    /// through both hierarchies and passes the operator both opens as lambdas over the left row. Each visit has
    /// its own registration of the correlation variable and its own block, because the field reads declared
    /// into that block belong to the lambda that holds the sub-plan.
    /// </remarks>
    public class ClrCursorCorrelate : Correlate, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCorrelate"/>, deriving its collation as Calcite does.
        /// </summary>
        /// <param name="left">The left (outer) input.</param>
        /// <param name="right">The right input, which reads the correlation variable.</param>
        /// <param name="correlationId">The correlation variable bound to each left row.</param>
        /// <param name="requiredColumns">The left fields the right input reads.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorCorrelate Create(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => org.apache.calcite.rel.metadata.RelMdCollation.enumerableCorrelate(mq, left, right, joinType)));

            return new ClrCursorCorrelate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
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
        /// <param name="joinType">The join type.</param>
        public ClrCursorCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, correlationId, requiredColumns, joinType)
        {

        }

        /// <inheritdoc />
        public override Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            return new ClrCursorCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Only a collation on the left input passes down: the left input is the outer loop, so only its order
        /// is preserved.
        /// </remarks>
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForJoin(required, getJoinType(), getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left input
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
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)getLeft(), pref);

            // Calcite's Rex translation reads the correlation variable registered below, so it takes
            // Calcite's physical type of the left row
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            // typed as the boxed row, because JoinSelector boxes both of its parameter types; a right side of
            // one primitive column, as under EXISTS, would otherwise disagree with it
            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            // the getter Calcite installs declares the left row's field reads into this block, and the right
            // sub-plan reads them, so the block becomes the start of the lambda that holds the sub-plan. It does
            // not optimise: it is translated separately from the sub-plan, and an optimising builder would
            // inline a declaration used once, leaving the sub-plan's reference to it dangling
            var corrBlock = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // the right input again through the other hierarchy, with its own block: the operator opens it per
            // left row with whichever kind of open the current advance needs, so it takes both
            var corrBlockAsync = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlockAsync, leftCalcite);
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());


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

            // the lambda redeclares the token parameter, shadowing the root's, so a right input opened inside
            // ReadAsync runs under that advance's token
            var innerAsync = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftType, typeof(CancellationToken), rightResultAsync.Expression.Type),
                Expression.Block(rightResultAsync.Expression.Type, declaredAsync, bodyAsync),
                corrParameter,
                implementor.CancellationToken);

            var selector = ClrEnumUtils.JoinSelector(implementor, getJoinType(), physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.CorrelateJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    inner,
                    innerAsync,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(getJoinType()))));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getLeft(), pref);

            // Calcite's Rex translation reads the correlation variable registered below, so it takes
            // Calcite's physical type of the left row
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            // typed as the boxed row, because JoinSelector boxes both of its parameter types; a right side of
            // one primitive column, as under EXISTS, would otherwise disagree with it
            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            // the getter Calcite installs declares the left row's field reads into this block, and the right
            // sub-plan reads them, so the block becomes the start of the lambda that holds the sub-plan. It does
            // not optimise: it is translated separately from the sub-plan, and an optimising builder would
            // inline a declaration used once, leaving the sub-plan's reference to it dangling
            var corrBlock = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // the right input again through the other hierarchy, with its own block: the operator opens it per
            // left row with whichever kind of open the current advance needs, so it takes both
            var corrBlockSync = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlockSync, leftCalcite);
            var rightResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());


            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlockSync.toBlock(), out var declaredSync, out var bodySync);
            bodySync.Add(rightResultSync.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            // the lambda redeclares the token parameter, shadowing the root's, so a right input opened inside
            // ReadAsync runs under that advance's token
            var inner = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftType, typeof(CancellationToken), rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                corrParameter,
                implementor.CancellationToken);

            var innerSync = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(leftType, rightResultSync.Expression.Type),
                Expression.Block(rightResultSync.Expression.Type, declaredSync, bodySync),
                corrParameter);

            var selector = ClrEnumUtils.JoinSelector(implementor, getJoinType(), physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.CorrelateJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    innerSync,
                    inner,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(getJoinType()))));
        }

    }

}
