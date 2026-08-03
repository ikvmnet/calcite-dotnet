using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.util;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrEnumerableConvention"/> calling convention,
    /// by building a lookup of one input and probing it with the other.
    /// </summary>
    public class ClrEnumerableHashJoin : Join, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableHashJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrEnumerableHashJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance);

            return new ClrEnumerableHashJoin(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

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
        public ClrEnumerableHashJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrEnumerableHashJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            switch (joinType.name())
            {
                case nameof(JoinRelType.SEMI):
                case nameof(JoinRelType.ANTI):
                    return ImplementSemiJoin(implementor, pref);
                default:
                    return ImplementHashJoin(implementor, pref);
            }
        }

        /// <summary>
        /// Implements every join that returns fields of both inputs.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        ClrEnumerableResult ImplementHashJoin(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.preferArray());
            var keyPhysType = leftResult.PhysType.project(analyzeCondition().leftKeys, JavaRowFormat.LIST);

            var leftSource = ClrEnumUtils.BoxRows(leftResult.PhysType, leftResult.Expression);
            var rightSource = ClrEnumUtils.BoxRows(rightResult.PhysType, rightResult.Expression);
            var leftType = leftSource.Type.GetGenericArguments()[0];
            var rightType = rightSource.Type.GetGenericArguments()[0];
            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            var info = analyzeCondition();
            var leftKey = Accessor(implementor, leftResult.PhysType, info.leftKeys, leftType);
            var rightKey = Accessor(implementor, rightResult.PhysType, info.rightKeys, rightType);
            var keyType = leftKey.ReturnType;

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var predicate = Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.HashJoin.MakeGenericMethod(leftType, rightType, keyType, rowType),
                    leftSource,
                    rightSource,
                    leftKey,
                    rightKey,
                    selector,
                    ClrPhysTypes.Comparer(implementor, keyPhysType),
                    Expression.Constant(joinType.generatesNullsOnLeft()),
                    Expression.Constant(joinType.generatesNullsOnRight()),
                    predicate));
        }

        /// <summary>
        /// Implements a semi or an anti join, which return the left input alone.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        ClrEnumerableResult ImplementSemiJoin(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)left, pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)right, pref);

            var physType = leftResult.PhysType;
            var leftType = TypeResolver.Resolve(leftResult.PhysType.getJavaRowType());
            var rightType = TypeResolver.Resolve(rightResult.PhysType.getJavaRowType());

            var info = analyzeCondition();
            var keyPhysType = leftResult.PhysType.project(info.leftKeys, JavaRowFormat.LIST);
            var leftKey = Accessor(implementor, leftResult.PhysType, info.leftKeys, leftType);
            var rightKey = Accessor(implementor, rightResult.PhysType, info.rightKeys, rightType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.SemiJoin.MakeGenericMethod(leftType, rightType, leftKey.ReturnType),
                    leftResult.Expression,
                    rightResult.Expression,
                    leftKey,
                    rightKey,
                    ClrPhysTypes.Comparer(implementor, keyPhysType),
                    Expression.Constant(joinType.name() == nameof(JoinRelType.ANTI)),
                    Predicate(implementor, leftResult.PhysType, rightResult.PhysType, leftType, rightType)));
        }

        /// <summary>
        /// Returns the lambda reading a join key from a row.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="keys"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// 1.41 has generateAccessor alone. The null aware variant, which yields null for the whole key when a
        /// field of it is null, arrives in 1.42; until the reference moves, a null key is excluded by the join
        /// itself rather than by the accessor.
        /// </remarks>
        static LambdaExpression Accessor(ClrEnumerableRelImplementor implementor, PhysType physType, java.util.List keys, Type rowType)
        {
            return implementor.Translator.TranslateSelector(physType.generateAccessor(keys), rowType);
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
            var info = analyzeCondition();

            if (info.nonEquiConditions.isEmpty())
                return Expression.Constant(null, type);

            var nonEqui = RexUtil.composeConjunction(getCluster().getRexBuilder(), info.nonEquiConditions, true);
            if (nonEqui == null)
                return Expression.Constant(null, type);

            return ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), left, right, leftPhysType, rightPhysType, nonEqui);
        }

    }

}
