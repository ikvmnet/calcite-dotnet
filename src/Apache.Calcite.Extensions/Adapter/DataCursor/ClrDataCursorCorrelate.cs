using System;
using System.Linq.Expressions;
using System.Threading;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="Correlate"/> in the <see cref="ClrDataCursorConvention"/> calling
    /// convention, by running the right input once per row of the left.
    /// </summary>
    /// <remarks>
    /// The right input is opened per outer row inside an advance, by the open of that advance's kind, so
    /// each body visits it through both hierarchies and hands the operator both opens as lambdas over the
    /// outer row. Each visit is made under its own registration of the correlation variable and into a
    /// block of its own, because the field reads the sub-plan declares into that block belong to the lambda
    /// that holds that sub-plan.
    /// </remarks>
    public class ClrDataCursorCorrelate : Correlate, ClrDataCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorCorrelate"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="correlationId"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrDataCursorCorrelate Create(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrDataCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => org.apache.calcite.rel.metadata.RelMdCollation.enumerableCorrelate(mq, left, right, joinType)));

            return new ClrDataCursorCorrelate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
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
        public ClrDataCursorCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, correlationId, requiredColumns, joinType)
        {

        }

        /// <inheritdoc />
        public override Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            return new ClrDataCursorCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A correlate passes a collation to its left input and to no other: the left is always the outer
        /// loop, so only it can preserve an ordering.
        /// </remarks>
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, getJoinType(), getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left input
            return ClrEnumerableTraitsUtils.DeriveTraitsForJoin(childTraits, childId, getJoinType(), getTraitSet(), getRight().getTraitSet());
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.LEFT_FIRST;
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrDataCursorRel)getLeft(), pref);

            // the getter registered below is one Calcite's Rex translation reads the outer row through,
            // so it is given their physical type, built here from the three values ours carries
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            // boxed, because the selector Calcite's join builds always takes boxed rows — see JoinSelector,
            // which boxes both of its parameter types. Every other join here boxes its sequences for that
            // reason; this one did not, and a correlate whose sub-plan yields one primitive column — an
            // EXISTS, whose right side is a bare boolean — is where the two types met and disagreed.
            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            // the variables holding the fields of the outer row are declared into this block by the getter
            // Calcite installs, and the inner sub-plan reads them, so the two share one scope
            // not optimising: the block is translated apart from the sub-plan that reads its
            // variables, and an optimising builder would inline a declaration used once, leaving the
            // reference already built into that sub-plan pointing at nothing
            var corrBlock = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChild(this, 1, (ClrDataCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // and the other hierarchy's visit of the same input, into a block of its own: the inner is
            // opened inside an advance, by the open of that advance's kind, so the operator takes both
            var corrBlockAsync = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlockAsync, leftCalcite);
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrDataCursorRel)getRight(), pref);
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

            // the token is declared again as this lambda's own, shadowing the root's, so that an inner
            // opened inside ReadAsync runs under that advance's token
            var innerAsync = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftType, typeof(CancellationToken), rightResultAsync.Expression.Type),
                Expression.Block(rightResultAsync.Expression.Type, declaredAsync, bodyAsync),
                corrParameter,
                implementor.CancellationToken);

            var selector = ClrEnumUtils.JoinSelector(implementor, getJoinType(), physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrDataCursorBuiltInMethod.CorrelateJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    inner,
                    innerAsync,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(getJoinType()))));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrDataCursorRel)getLeft(), pref);

            // the getter registered below is one Calcite's Rex translation reads the outer row through,
            // so it is given their physical type, built here from the three values ours carries
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, leftCalcite.getJavaRowType(), getCorrelVariable());

            // boxed, because the selector Calcite's join builds always takes boxed rows — see JoinSelector,
            // which boxes both of its parameter types. Every other join here boxes its sequences for that
            // reason; this one did not, and a correlate whose sub-plan yields one primitive column — an
            // EXISTS, whose right side is a bare boolean — is where the two types met and disagreed.
            var corrParameter = Expression.Parameter(leftResult.PhysType.RowType, getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            // the variables holding the fields of the outer row are declared into this block by the getter
            // Calcite installs, and the inner sub-plan reads them, so the two share one scope
            // not optimising: the block is translated apart from the sub-plan that reads its
            // variables, and an optimising builder would inline a declaration used once, leaving the
            // reference already built into that sub-plan pointing at nothing
            var corrBlock = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftCalcite);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrDataCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            // and the other hierarchy's visit of the same input, into a block of its own: the inner is
            // opened inside an advance, by the open of that advance's kind, so the operator takes both
            var corrBlockSync = new J.BlockBuilder(false);
            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlockSync, leftCalcite);
            var rightResultSync = implementor.VisitChild(this, 1, (ClrDataCursorRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());


            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlockSync.toBlock(), out var declaredSync, out var bodySync);
            bodySync.Add(rightResultSync.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            // the token is declared again as this lambda's own, shadowing the root's, so that an inner
            // opened inside ReadAsync runs under that advance's token
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
                ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.CorrelateJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    innerSync,
                    inner,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(getJoinType()))));
        }

    }

}
