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
    /// Implementation of <see cref="Join"/> in the <see cref="ClrCursorConvention"/> calling convention,
    /// by running the right input once per batch of left rows.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableBatchNestedLoopJoin</c>. The rule rewrites the right input as a filter over a
    /// disjunction of per-row conditions, one correlation variable per batch position, so one pass of the right
    /// input serves a whole batch. The batch size is the number of correlation variables.
    ///
    /// <para>The right input is opened once per batch while the join's cursor advances, synchronously or
    /// awaiting according to how that advance was called. Each body therefore visits it through both
    /// hierarchies, each visit with its own declarations of the correlation variables.</para>
    /// </remarks>
    public class ClrCursorBatchNestedLoopJoin : Join, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorBatchNestedLoopJoin"/>, deriving its collation as Calcite does.
        /// </summary>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input, filtered on the correlation variables.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables, one per batch position.</param>
        /// <param name="requiredColumns">The left fields the right input reads.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorBatchNestedLoopJoin Create(RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, ImmutableBitSet requiredColumns, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableBatchNestedLoopJoin(mq, left, right, joinType)));

            return new ClrCursorBatchNestedLoopJoin(cluster, traitSet, left, right, condition, variablesSet, requiredColumns, joinType);
        }

        readonly ImmutableBitSet requiredColumns;

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traits">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input, filtered on the correlation variables.</param>
        /// <param name="condition">The join condition.</param>
        /// <param name="variablesSet">The correlation variables, one per batch position.</param>
        /// <param name="requiredColumns">The left fields the right input reads.</param>
        /// <param name="joinType">The join type.</param>
        public ClrCursorBatchNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, java.util.Set variablesSet, ImmutableBitSet requiredColumns, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, variablesSet, joinType)
        {
            this.requiredColumns = requiredColumns;
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            return new ClrCursorBatchNestedLoopJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), requiredColumns, joinType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrCursorTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet());
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

            var rightRowCount = mq.getRowCount(getRight()).doubleValue();
            var leftRowCount = mq.getRowCount(getLeft()).doubleValue();
            if (double.IsInfinity(leftRowCount) || double.IsInfinity(rightRowCount))
                return planner.getCostFactory().makeInfiniteCost();

            // the right input is read once per batch, so it restarts left row count / batch size times
            var restartCount = mq.getRowCount(getLeft()).doubleValue() / getVariablesSet().size();

            var rightCost = planner.getCost(getRight(), mq);
            if (rightCost == null)
                return null;

            var rescanCost = rightCost.multiplyBy(java.lang.Math.max(1.0, restartCount - 1));

            // TODO add the cost of the last loop, the one that looks for the match (as in Calcite)
            return planner.getCostFactory()
                .makeCost(rowCount + leftRowCount, 0, 0)
                .plus(rescanCost);
        }

        /// <inheritdoc />
        public override RelWriter explainTerms(RelWriter pw)
        {
            return base.explainTerms(pw).item("batchSize", getVariablesSet().size());
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)getLeft(), pref);

            // Calcite's Rex translation reads the correlation variables registered below, so they take
            // Calcite's physical type of the left row
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrVarType = leftCalcite.getJavaRowType();
            var corrArgList = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, (java.lang.reflect.Type)(java.lang.Class)typeof(java.util.List), "corrList");
            var listParameter = Expression.Parameter(typeof(java.util.List), "corrList");
            implementor.Translator.Bind(corrArgList, listParameter);

            var names = new System.Collections.Generic.List<string>();
            for (var i = getVariablesSet().iterator(); i.hasNext();)
                names.Add(((CorrelationId)i.next()).getName());

            // one correlation variable per batch position, each read from the list the batch arrives in. The
            // builder does not optimise: it would inline a declaration used once, and the sub-plan translated
            // separately still refers to the variable by name
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
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

            // the right input again through the other hierarchy, with its own declarations: the operator opens
            // it per batch with whichever kind of open the current advance needs, so it takes both
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
            var rightResultAsync = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

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
                    ClrCursorBuiltInMethod.CorrelateBatchJoin.MakeGenericMethod(leftType, rightType, rowType),
                    Expression.Constant(ClrEnumUtils.ToLinq4jJoinType(joinType)),
                    leftResult.Expression,
                    inner,
                    innerAsync,
                    selector,
                    predicate,
                    Expression.Constant(getVariablesSet().size())));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getLeft(), pref);

            // Calcite's Rex translation reads the correlation variables registered below, so they take
            // Calcite's physical type of the left row
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftResult.PhysType.RelRowType, leftResult.PhysType.Format, false);
            var corrVarType = leftCalcite.getJavaRowType();
            var corrArgList = J.Expressions.parameter(java.lang.reflect.Modifier.FINAL, (java.lang.reflect.Type)(java.lang.Class)typeof(java.util.List), "corrList");
            var listParameter = Expression.Parameter(typeof(java.util.List), "corrList");
            implementor.Translator.Bind(corrArgList, listParameter);

            var names = new System.Collections.Generic.List<string>();
            for (var i = getVariablesSet().iterator(); i.hasNext();)
                names.Add(((CorrelationId)i.next()).getName());

            // one correlation variable per batch position, each read from the list the batch arrives in. The
            // builder does not optimise: it would inline a declaration used once, and the sub-plan translated
            // separately still refers to the variable by name
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
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

            // the right input again through the other hierarchy, with its own declarations: the operator opens
            // it per batch with whichever kind of open the current advance needs, so it takes both
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
            var rightResultSync = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);

            foreach (var name in names)
                implementor.ClearCorrelVariable(name);

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
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.CorrelateBatchJoinAsync.MakeGenericMethod(leftType, rightType, rowType),
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
