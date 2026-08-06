using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Tree;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Correlate"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention, by running the right input once per row of the left.
    /// </summary>
    public class ClrEnumerableCorrelate : Correlate, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCorrelate"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="correlationId"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrEnumerableCorrelate Create(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => org.apache.calcite.rel.metadata.RelMdCollation.enumerableCorrelate(mq, left, right, joinType)));

            return new ClrEnumerableCorrelate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
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
        public ClrEnumerableCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, correlationId, requiredColumns, joinType)
        {

        }

        /// <inheritdoc />
        public override Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            return new ClrEnumerableCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A correlate passes a collation to its left input and to no other: the left is always the outer
        /// loop, so only it can preserve an ordering.
        /// </remarks>
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, getJoinType(), getLeft().getRowType().getFieldCount(), getTraitSet())!;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left input
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
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getLeft(), pref);

            // the variables holding the fields of the outer row are declared into this block by the getter
            // Calcite installs, and the inner sub-plan reads them, so the two share one scope
            // not optimising: the block is translated apart from the sub-plan that reads its
            // variables, and an optimising builder would inline a declaration used once, leaving the
            // reference already built into that sub-plan pointing at nothing
            var corrBlock = new J.BlockBuilder(false);
            var corrVarType = leftResult.PhysType.getJavaRowType();
            var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, corrVarType, getCorrelVariable());

            // boxed, because the selector Calcite's join builds always takes boxed rows — see JoinSelector,
            // which boxes both of its parameter types. Every other join here boxes its sequences for that
            // reason; this one did not, and a correlate whose sub-plan yields one primitive column — an
            // EXISTS, whose right side is a bare boolean — is where the two types met and disagreed.
            var corrParameter = Expression.Parameter(ClrTypes.Resolve(J.Primitive.box(corrVarType)), getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftResult.PhysType);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

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

            var selector = ClrEnumUtils.JoinSelector(implementor, getJoinType(), physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.CorrelateJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftSource,
                    inner,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(getJoinType()))));
        }

    }

}
