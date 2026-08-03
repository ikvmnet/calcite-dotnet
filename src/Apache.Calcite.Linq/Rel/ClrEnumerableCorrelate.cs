using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.sql;
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
        /// Initializes a new instance.
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
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
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

            var corrParameter = Expression.Parameter(TypeResolver.Resolve(corrVarType), getCorrelVariable());
            implementor.Translator.Bind(corrArg, corrParameter);

            implementor.RegisterCorrelVariable(getCorrelVariable(), corrArg, corrBlock, leftResult.PhysType);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getRight(), pref);
            implementor.ClearCorrelVariable(getCorrelVariable());

            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            var leftType = TypeResolver.Resolve(leftResult.PhysType.getJavaRowType());
            var rightType = TypeResolver.Resolve(rightResult.PhysType.getJavaRowType());
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.prefer(JavaRowFormat.CUSTOM));
            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            var inner = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(leftType, rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                corrParameter);

            var selector = ClrEnumUtils.JoinSelector(implementor, getJoinType(), physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.CorrelateJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    inner,
                    selector,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(getJoinType()))));
        }

    }

}
