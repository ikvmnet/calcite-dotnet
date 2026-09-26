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
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="Join"/> in the <see cref="ClrDataCursorConvention"/> calling convention,
    /// by running the right input once per batch of left rows.
    /// </summary>
    /// <remarks>
    /// The rule rewrites the right input into a filter over a disjunction of the batch's conditions, so one
    /// pass of it serves every row of the batch. A correlation variable per batch position is what carries
    /// the left rows into that filter, which is why the node declares as many as the batch is wide.
    ///
    /// <para>The right input is opened per batch inside an advance, by the open of that advance's kind, so
    /// each body visits it through both hierarchies, each visit under its own declarations of the
    /// correlation variables into a block of its own.</para>
    /// </remarks>
    public class ClrDataCursorBatchNestedLoopJoin : Join, ClrDataCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorBatchNestedLoopJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrDataCursorBatchNestedLoopJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrDataCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableBatchNestedLoopJoin(mq, left, right, joinType)));

            return new ClrDataCursorBatchNestedLoopJoin(cluster, traitSet, left, right, condition, variablesSet, requiredColumns, joinType);
        }

        readonly ImmutableBitSet requiredColumns;

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="requiredColumns"></param>
        /// <param name="joinType"></param>
        public ClrDataCursorBatchNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, ImmutableBitSet requiredColumns, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {
            this.requiredColumns = requiredColumns;
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrDataCursorBatchNestedLoopJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), requiredColumns, joinType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrEnumerableTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet());
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            if (joinType.name() == nameof(JoinRelType.FULL) || joinType.name() == nameof(JoinRelType.RIGHT))
                return DeriveMode.PROHIBITED;

            return DeriveMode.LEFT_FIRST;
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            var rowCount = mq.getRowCount(this).doubleValue();

            // the right input is read once per batch rather than once per row, which is the whole point of
            // the node, so the restart count is the left's row count divided by the batch size
            var restartCount = mq.getRowCount(getLeft()).doubleValue() / getVariablesSet().size();

            var rightCost = planner.getCost(getRight(), mq);
            var rescanCost = rightCost.multiplyBy(java.lang.Math.max(1.0, restartCount) - 1);

            return planner.getCostFactory()
                .makeCost(rowCount + mq.getRowCount(getLeft()).doubleValue(), 0, 0)
                .plus(rescanCost);
        }

        /// <inheritdoc />
        public override RelWriter explainTerms(RelWriter pw)
        {
            return base.explainTerms(pw).item("batchSize", getVariablesSet().size());
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrDataCursorRel)getLeft(), pref);

            // the getters registered below are ones Calcite's Rex translation reads the outer row through,
            // so they are given their physical type, built here from the three values ours carries
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrVarType = leftCalcite.getJavaRowType();
            var corrArgList = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, (java.lang.reflect.Type)(java.lang.Class)typeof(java.util.List), "corrList");
            var listParameter = Expression.Parameter(typeof(java.util.List), "corrList");
            implementor.Translator.Bind(corrArgList, listParameter);

            var names = new System.Collections.Generic.List<string>();
            for (var i = getVariablesSet().iterator(); i.hasNext();)
                names.Add(((CorrelationId)i.next()).getName());

            // one correlation variable per batch position, each read out of the list the batch arrives in.
            // Not optimising, for the reason ClrDataCursorCorrelate gives: the block is translated apart
            // from the sub-plan that reads its variables.
            var corrBlock = new J.BlockBuilder(false);
            for (int c = 0; c < names.Count; c++)
            {
                var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, corrVarType, names[c]);

                corrBlock.add(
                    J.Expressions.declare(java.lang.reflect.Modifier.FINAL, corrArg,
                        J.Expressions.convert_(
                            J.Expressions.call(corrArgList, ListGet, J.Expressions.constant(java.lang.Integer.valueOf(c))),
                            corrVarType)));

                implementor.RegisterCorrelVariable(names[c], corrArg, corrBlock, leftCalcite);
            }
            var rightResult = implementor.VisitChild(this, 1, (ClrDataCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

            // and the other hierarchy's visit of the same input, into a block of its own: the right side is
            // opened per batch inside an advance, by the open of that advance's kind, so the operator takes
            // both
            var corrBlockAsync = new J.BlockBuilder(false);
            for (int c = 0; c < names.Count; c++)
            {
                var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, corrVarType, names[c]);

                corrBlockAsync.add(
                    J.Expressions.declare(java.lang.reflect.Modifier.FINAL, corrArg,
                        J.Expressions.convert_(
                            J.Expressions.call(corrArgList, ListGet, J.Expressions.constant(java.lang.Integer.valueOf(c))),
                            corrVarType)));

                implementor.RegisterCorrelVariable(names[c], corrArg, corrBlockAsync, leftCalcite);
            }
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrDataCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

            // boxed, as every join here boxes: the selector takes boxed rows, and a left join hands it a null

            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlockAsync.toBlock(), out var declaredAsync, out var bodyAsync);
            bodyAsync.Add(rightResultAsync.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            var inner = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(typeof(java.util.List), rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                listParameter);

            var innerAsync = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(typeof(java.util.List), typeof(CancellationToken), rightResultAsync.Expression.Type),
                Expression.Block(rightResultAsync.Expression.Type, declaredAsync, bodyAsync),
                listParameter,
                implementor.CancellationToken);

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getCondition());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrDataCursorBuiltInMethod.CorrelateBatchJoin.MakeGenericMethod(leftType, rightType, rowType),
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType)),
                    leftResult.Expression,
                    inner,
                    innerAsync,
                    selector,
                    predicate,
                    Expression.Constant(getVariablesSet().size())));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrDataCursorRel)getLeft(), pref);

            // the getters registered below are ones Calcite's Rex translation reads the outer row through,
            // so they are given their physical type, built here from the three values ours carries
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrVarType = leftCalcite.getJavaRowType();
            var corrArgList = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, (java.lang.reflect.Type)(java.lang.Class)typeof(java.util.List), "corrList");
            var listParameter = Expression.Parameter(typeof(java.util.List), "corrList");
            implementor.Translator.Bind(corrArgList, listParameter);

            var names = new System.Collections.Generic.List<string>();
            for (var i = getVariablesSet().iterator(); i.hasNext();)
                names.Add(((CorrelationId)i.next()).getName());

            // one correlation variable per batch position, each read out of the list the batch arrives in.
            // Not optimising, for the reason ClrDataCursorCorrelate gives: the block is translated apart
            // from the sub-plan that reads its variables.
            var corrBlock = new J.BlockBuilder(false);
            for (int c = 0; c < names.Count; c++)
            {
                var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, corrVarType, names[c]);

                corrBlock.add(
                    J.Expressions.declare(java.lang.reflect.Modifier.FINAL, corrArg,
                        J.Expressions.convert_(
                            J.Expressions.call(corrArgList, ListGet, J.Expressions.constant(java.lang.Integer.valueOf(c))),
                            corrVarType)));

                implementor.RegisterCorrelVariable(names[c], corrArg, corrBlock, leftCalcite);
            }
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrDataCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

            // and the other hierarchy's visit of the same input, into a block of its own: the right side is
            // opened per batch inside an advance, by the open of that advance's kind, so the operator takes
            // both
            var corrBlockSync = new J.BlockBuilder(false);
            for (int c = 0; c < names.Count; c++)
            {
                var corrArg = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, corrVarType, names[c]);

                corrBlockSync.add(
                    J.Expressions.declare(java.lang.reflect.Modifier.FINAL, corrArg,
                        J.Expressions.convert_(
                            J.Expressions.call(corrArgList, ListGet, J.Expressions.constant(java.lang.Integer.valueOf(c))),
                            corrVarType)));

                implementor.RegisterCorrelVariable(names[c], corrArg, corrBlockSync, leftCalcite);
            }
            var rightResultSync = implementor.VisitChild(this, 1, (ClrDataCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

            // boxed, as every join here boxes: the selector takes boxed rows, and a left join hands it a null

            implementor.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var body);
            body.Add(rightResult.Expression);

            implementor.Translator.TranslateStatements(corrBlockSync.toBlock(), out var declaredSync, out var bodySync);
            bodySync.Add(rightResultSync.Expression);

            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));
            var rowType = physType.RowType;

            var inner = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(typeof(java.util.List), typeof(CancellationToken), rightResult.Expression.Type),
                Expression.Block(rightResult.Expression.Type, declared, body),
                listParameter,
                implementor.CancellationToken);

            var innerSync = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(typeof(java.util.List), rightResultSync.Expression.Type),
                Expression.Block(rightResultSync.Expression.Type, declaredSync, bodySync),
                listParameter);

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var predicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getCondition());

            return implementor.ResultAsync(physType,
                ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.CorrelateBatchJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType)),
                    leftResult.Expression,
                    innerSync,
                    inner,
                    selector,
                    predicate,
                    Expression.Constant(getVariablesSet().size())));
        }

        static readonly java.lang.reflect.Method ListGet = ((java.lang.Class)typeof(java.util.List)).getMethod("get", [(java.lang.Class)typeof(int)]);

    }

}
