using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrEnumerableConvention"/> calling convention,
    /// by comparing every pair of rows.
    /// </summary>
    /// <remarks>
    /// What a join with no equality to build a lookup on becomes, and what
    /// <see cref="ClrEnumerableHashJoin"/> leaves for it.
    /// </remarks>
    public class ClrEnumerableNestedLoopJoin : Join, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        public ClrEnumerableNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrEnumerableNestedLoopJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.preferArray());

            var leftType = TypeResolver.Resolve(leftResult.PhysType.getJavaRowType());
            var rightType = TypeResolver.Resolve(rightResult.PhysType.getJavaRowType());
            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftResult.PhysType, rightResult.PhysType, getCondition());
            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.NestedLoopJoin.MakeGenericMethod(leftType, rightType, rowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    selector,
                    predicate,
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType))));
        }

    }

}
