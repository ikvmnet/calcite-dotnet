using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.enumerable.impl;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Window"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableWindow</c>. Calcite generates the window loop as Java source; here it is the
    /// operator <see cref="ClrCursorDefaults.Window"/>, which partitions the input, orders each partition, walks
    /// its rows, decides when the frame must be recomputed, and folds the frame's rows in. Each window group
    /// becomes one such operator over the previous group's output.
    ///
    /// <para>The node translates only what Calcite's generators produce: the window aggregate implementors'
    /// reset, add and result blocks, the frame bounds (<c>translateBound</c>, ported because it is private),
    /// the partition key, comparator and collation key, and the <c>constants</c> literals.</para>
    ///
    /// <para>Calcite keeps each aggregate's state and last result in local variables of the generated method.
    /// Here they are fields of one synthetic accumulator record, as in <see cref="ClrCursorAggregate"/>, and
    /// the loop variables the implementors read are bound to the properties of a <see cref="WindowFrame"/>.</para>
    /// </remarks>
    public class ClrCursorWindow : Window, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="constants">Literals that aggregate arguments refer to by indexes past the input's fields.</param>
        /// <param name="rowType">The output row type: the input's fields followed by one per aggregate.</param>
        /// <param name="groups">The window groups.</param>
        public ClrCursorWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, java.util.List constants, RelDataType rowType, java.util.List groups) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, constants, rowType, groups)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorWindow(getCluster(), traitSet, (RelNode)sole(inputs), constants, getRowType(), groups);
        }

        /// <inheritdoc />
        public override Window copy(java.util.List constants)
        {
            return new ClrCursorWindow(getCluster(), getTraitSet(), getInput(), constants, getRowType(), groups);
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);
            if (cost == null)
                return null;

            return cost.multiplyBy(ClrCursorConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            // an aggregate argument may refer to a constant by an index past the input's fields; the constants
            // are translated once and the input getter returns them by index
            var translatedConstants = new java.util.ArrayList(constants.size());
            for (int i = 0; i < constants.size(); i++)
            {
                var constant = (RexLiteral)constants.get(i);
                translatedConstants.add(RexToLixTranslator.translateLiteral(constant, constant.getType(), typeFactory, RexImpTable.NullAs.NULL));
            }

            // the block's variables hold values evaluated once, ahead of the loops, and read by each group's
            // lambdas, as Calcite declares "final Comparator comparator = ..." ahead of its loop
            var variables = new List<ParameterExpression>();
            var body = new List<Expression>();

            var physType = result.PhysType;
            var source = result.Expression;

            for (int windowIdx = 0; windowIdx < groups.size(); windowIdx++)
            {
                var group = (Group)groups.get(windowIdx);
                source = ImplementGroup(implementor, result.PhysType, result.Format, group, windowIdx, physType, source, pref, translatedConstants, variables, body, out physType);

                // one variable per group, as Calcite declares one "source" per group; the next group reads it
                var sourceVariable = Expression.Variable(source.Type, $"source{windowIdx}");
                variables.Add(sourceVariable);
                body.Add(Expression.Assign(sourceVariable, source));
                source = sourceVariable;
            }

            body.Add(source);

            return implementor.Result(physType, Expression.Block(source.Type, variables, body));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);

            // an aggregate argument may refer to a constant by an index past the input's fields; the constants
            // are translated once and the input getter returns them by index
            var translatedConstants = new java.util.ArrayList(constants.size());
            for (int i = 0; i < constants.size(); i++)
            {
                var constant = (RexLiteral)constants.get(i);
                translatedConstants.add(RexToLixTranslator.translateLiteral(constant, constant.getType(), typeFactory, RexImpTable.NullAs.NULL));
            }

            // the block's variables hold values evaluated once, ahead of the loops, and read by each group's
            // lambdas, as Calcite declares "final Comparator comparator = ..." ahead of its loop
            var variables = new List<ParameterExpression>();
            var body = new List<Expression>();

            var physType = result.PhysType;
            var source = result.Expression;

            for (int windowIdx = 0; windowIdx < groups.size(); windowIdx++)
            {
                var group = (Group)groups.get(windowIdx);
                source = ImplementGroupAsync(implementor, result.PhysType, result.Format, group, windowIdx, physType, source, pref, translatedConstants, variables, body, out physType);

                // one variable per group, as Calcite declares one "source" per group; the next group reads it
                var sourceVariable = Expression.Variable(source.Type, $"source{windowIdx}");
                variables.Add(sourceVariable);
                body.Add(Expression.Assign(sourceVariable, source));
                source = sourceVariable;
            }

            body.Add(source);

            return implementor.ResultAsync(physType, Expression.Block(source.Type, variables, body));
        }

        /// <summary>
        /// Implements one window group over the output of the group before it.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="inputResultPhysType">The physical type of the node's input. Aggregate arguments index
        /// its fields, whichever group is being implemented.</param>
        /// <param name="inputResultFormat">The format of the node's input.</param>
        /// <param name="group">The window group.</param>
        /// <param name="windowIdx">The group's index, used to name its variables.</param>
        /// <param name="inputPhysType">The physical type of this group's input.</param>
        /// <param name="source">This group's input.</param>
        /// <param name="pref">The preferred row format.</param>
        /// <param name="translatedConstants">The node's constants, translated.</param>
        /// <param name="variables">Receives variables of the block the whole node becomes.</param>
        /// <param name="body">Receives statements of that block.</param>
        /// <param name="outputPhysType">The physical type of this group's output.</param>
        /// <returns>The expression opening this group's output.</returns>
        Expression ImplementGroup(
            ClrCursorRelImplementor implementor,
            ClrPhysType inputResultPhysType,
            JavaRowFormat inputResultFormat,
            Group group,
            int windowIdx,
            ClrPhysType inputPhysType,
            Expression source,
            ClrCursorPrefer pref,
            java.util.List translatedConstants,
            List<ParameterExpression> variables,
            List<Expression> body,
            out ClrPhysType outputPhysType)
        {
            var typeFactory = implementor.TypeFactory;
            var translator = implementor.Translator;

            // the aggregate implementors write linq4j blocks, the frame reads rows through Rex, and the output
            // row is built with record, so the input and output each also have a Calcite physical type for the
            // field reads inside those blocks. A partition is an Object[], as in Calcite, so its rows are boxed
            // Java values, which Calcite's comparator expects
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);

            var sourceType = inputPhysType.RowType;
            source = source;

            // orders a partition's rows; EXCLUDE and RANK also use it to decide whether two rows are peers
            var comparator_ = J.Expressions.parameter((java.lang.Class)typeof(java.util.Comparator), $"comparator{windowIdx}");
            var comparator = Hoist(translator, variables, body, comparator_,
                inputPhysType.GenerateComparator(group.collation()), typeof(java.util.Comparator));

            var aggs = new java.util.ArrayList();
            var aggregateCalls = group.getAggregateCalls(this);
            for (int aggIdx = 0; aggIdx < aggregateCalls.size(); aggIdx++)
            {
                var call = (AggregateCall)aggregateCalls.get(aggIdx);
                if (call.ignoreNulls())
                {
                    switch (call.getAggregation().getKind().name())
                    {
                        case nameof(SqlKind.FIRST_VALUE):
                        case nameof(SqlKind.LAST_VALUE):
                            // their implementors read IGNORE NULLS through ClrWinAggContext.ignoreNulls
                            break;
                        default:
                            throw new java.lang.UnsupportedOperationException("IGNORE NULLS not supported");
                    }
                }

                aggs.add(new ClrAggImpState(aggIdx, call, true, RexImplementorTables.of(getCluster())));
            }

            // the output of this group is its input plus one field per aggregate
            var typeBuilder = typeFactory.builder();
            typeBuilder.addAll(inputPhysType.RelRowType.getFieldList());
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);

                // an aggregate call's name may be null; Calcite fails with the same message
                typeBuilder.add(agg.call.name ?? throw new java.lang.NullPointerException($"agg.call.name for {agg.call}"), agg.call.type);
            }

            outputPhysType = ClrPhysTypeImpl.Of(typeFactory, typeBuilder.build(), pref.Prefer(inputResultFormat));
            var outputCalcite = PhysTypeImpl.of(typeFactory, outputPhysType.RelRowType, outputPhysType.Format, false);

            // a bounded RANGE frame finds its bounds by binary search over the collation key
            J.ParameterExpression? keySelector_ = null;
            J.ParameterExpression? keyComparator_ = null;
            if ((group.isRows || (group.upperBound.isUnbounded() && group.lowerBound.isUnbounded())) == false)
            {
                var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(group.collation().getFieldCollations());

                keySelector_ = J.Expressions.parameter((java.lang.Class)typeof(Function1), $"keySelector{windowIdx}");
                keyComparator_ = J.Expressions.parameter((java.lang.Class)typeof(java.util.Comparator), $"keyComparator{windowIdx}");

                Hoist(translator, variables, body, keySelector_, keySelector, typeof(Function1));
                Hoist(translator, variables, body, keyComparator_, collationComparator ?? throw new java.lang.NullPointerException("keyComparator"), typeof(java.util.Comparator));
            }

            var loop = new WindowLoop(inputCalcite);
            var inputGetter = new WindowRelInputGetter(loop.Row, inputCalcite, inputResultPhysType.RelRowType.getFieldCount(), translatedConstants);

            // the output row is every input field, then each aggregate's last result
            var inputFieldCount = inputPhysType.RelRowType.getFieldCount();
            var outputRow = new java.util.ArrayList();
            for (int i = 0; i < inputFieldCount; i++)
                outputRow.add(inputCalcite.fieldReference(loop.Row, i, outputCalcite.getJavaFieldType(i)));

            // as Calcite's declareAndResetState: each aggregate's state, the variable holding its last result,
            // and the block that initializes both
            var initBlock = new J.BlockBuilder();
            var stateTypes = new java.util.ArrayList();
            var initExpressions = new java.util.ArrayList();
            DeclareAndResetState(typeFactory, inputResultPhysType, constants, windowIdx, aggs, outputCalcite, outputRow, group.exclude, initBlock, stateTypes, initExpressions);

            var accPhysType = PhysTypeImplWorkaround.Of(typeFactory, typeFactory.createSyntheticType(stateTypes));
            ClrCursorAggregateBase.DeclareParentAccumulator(initExpressions, initBlock, accPhysType);

            var accType = ClrTypes.Resolve(accPhysType.getJavaRowType());
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            loop.Accumulator = acc_;

            // Calcite's local variables become fields of the accumulator, which each lambda takes and returns
            for (int i = 0, slot = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);

                var state = new java.util.ArrayList(agg.state.size());
                for (int j = 0; j < agg.state.size(); j++)
                    state.add(accPhysType.fieldReference(acc_, slot++));

                agg.state = state;
                agg.result = accPhysType.fieldReference(acc_, slot++);
                outputRow.set(inputFieldCount + i, agg.result);
            }

            var initializer = Expression.Lambda(
                typeof(Func<>).MakeGenericType(accType),
                translator.TranslateBody(initBlock.toBlock(), accType));

            // the frame bounds, each in its own block because each becomes its own lambda
            var min_ = J.Expressions.constant(0);

            var lowerBuilder = new J.BlockBuilder();
            var startUnchecked = TranslateBound(
                RexToLixTranslator.forAggregation(typeFactory, lowerBuilder, inputGetter, implementor.Conformance),
                typeFactory, loop.Index, loop.Row, min_, loop.MaxX, loop.Rows, group, true, inputCalcite, keySelector_, keyComparator_);
            lowerBuilder.add(J.Expressions.return_(null, startUnchecked));

            var upperBuilder = new J.BlockBuilder();
            var endUnchecked = TranslateBound(
                RexToLixTranslator.forAggregation(typeFactory, upperBuilder, inputGetter, implementor.Conformance),
                typeFactory, loop.Index, loop.Row, min_, loop.MaxX, loop.Rows, group, false, inputCalcite, keySelector_, keyComparator_);
            upperBuilder.add(J.Expressions.return_(null, endUnchecked));

            // a bound that is unbounded or the current row is already inside the partition and needs no clamp
            var clampStart = group.lowerBound.isUnbounded() == false && ReferenceEquals(startUnchecked, loop.Index) == false;
            var clampEnd = group.upperBound.isUnbounded() == false && ReferenceEquals(endUnchecked, loop.Index) == false;

            var lowerBound = loop.Lambda(translator, lowerBuilder.toBlock(), typeof(int), null);
            var upperBound = loop.Lambda(translator, upperBuilder.toBlock(), typeof(int), null);

            var frame = new java.util.function.DelegateFunction<J.BlockBuilder, WinAggFrameResultContext>(
                block => new ClrWinAggFrameResultContext(
                    block, typeFactory, implementor.Conformance, inputCalcite, inputResultPhysType.RelRowType.getFieldCount(),
                    translatedConstants, comparator_, loop.Rows, loop.Index, loop.Start, loop.End, min_, loop.MaxX,
                    loop.HasRows, loop.FrameRowCount, loop.PartitionRowCount, loop.Position));

            // reset, add, and the two halves of result, in the order Calcite calls them: the implementors keep
            // state of their own across the calls
            var resetBuilder = new J.BlockBuilder();
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                agg.Implementor.implementReset(agg.context,
                    new WinAggResetContextImpl(resetBuilder, agg.state, loop.Index, loop.Start, loop.End, loop.HasRows, loop.FrameRowCount, loop.PartitionRowCount));
            }

            var addBuilder = new J.BlockBuilder();
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                agg.Implementor.implementAdd(agg.context,
                    new ClrWinAggAddContext(addBuilder, agg.state, frame, loop.Position, RexArguments(agg, inputResultPhysType.RelRowType, constants), RexFilterArgument(agg, inputResultPhysType.RelRowType)));
            }

            var cachedBuilder = new J.BlockBuilder();
            var cached = ImplementResult(aggs, cachedBuilder, frame, true, inputResultPhysType.RelRowType, constants);

            var uncachedBuilder = new J.BlockBuilder();
            var uncached = ImplementResult(aggs, uncachedBuilder, frame, false, inputResultPhysType.RelRowType, constants);

            var resetBlock = resetBuilder.toBlock();
            var addBlock = addBuilder.toBlock();

            var accumulatorType = typeof(Func<,,>).MakeGenericType(typeof(WindowFrame), accType, accType);
            var reset = resetBlock.statements.size() == 0 ? null : loop.Lambda(translator, WithReturn(resetBlock, acc_), accType, accType);
            var adder = addBlock.statements.size() == 0 ? null : loop.Lambda(translator, WithReturn(addBlock, acc_), accType, accType);
            var cachedResult = cached == false ? null : loop.Lambda(translator, WithReturn(cachedBuilder.toBlock(), acc_), accType, accType);
            var uncachedResult = uncached == false ? null : loop.Lambda(translator, WithReturn(uncachedBuilder.toBlock(), acc_), accType, accType);

            var selectorBuilder = new J.BlockBuilder();
            selectorBuilder.add(J.Expressions.return_(null, outputCalcite.record(outputRow)));

            var outputType = outputPhysType.RowType;
            var selector = loop.Lambda(translator, selectorBuilder.toBlock(), outputType, accType);

            // the partition key, the only piece that reads a row outside the loop
            var partitionSelector = PartitionSelector(translator, inputCalcite, group, sourceType);
            var keyType = partitionSelector?.ReturnType ?? typeof(object);

            return Expression.Call(null,
                ClrCursorBuiltInMethod.Window.MakeGenericMethod(sourceType, keyType, accType, outputType),
                source,
                (Expression?)partitionSelector ?? Expression.Constant(null, typeof(Func<,>).MakeGenericType(sourceType, keyType)),
                comparator,
                Expression.Constant(group.exclude, typeof(RexWindowExclusion)),
                lowerBound,
                upperBound,
                Expression.Constant(group.isAlwaysNonEmpty()),
                Expression.Constant(clampStart),
                Expression.Constant(clampEnd),
                Expression.Constant(group.lowerBound.isUnboundedPreceding() == false),
                initializer,
                (Expression?)reset ?? Expression.Constant(null, accumulatorType),
                (Expression?)adder ?? Expression.Constant(null, accumulatorType),
                (Expression?)cachedResult ?? Expression.Constant(null, accumulatorType),
                (Expression?)uncachedResult ?? Expression.Constant(null, accumulatorType),
                selector);
        }

        /// <summary>
        /// Implements one window group over the output of the group before it.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="inputResultPhysType">The physical type of the node's input. Aggregate arguments index
        /// its fields, whichever group is being implemented.</param>
        /// <param name="inputResultFormat">The format of the node's input.</param>
        /// <param name="group">The window group.</param>
        /// <param name="windowIdx">The group's index, used to name its variables.</param>
        /// <param name="inputPhysType">The physical type of this group's input.</param>
        /// <param name="source">This group's input.</param>
        /// <param name="pref">The preferred row format.</param>
        /// <param name="translatedConstants">The node's constants, translated.</param>
        /// <param name="variables">Receives variables of the block the whole node becomes.</param>
        /// <param name="body">Receives statements of that block.</param>
        /// <param name="outputPhysType">The physical type of this group's output.</param>
        /// <returns>The expression opening this group's output.</returns>
        Expression ImplementGroupAsync(
            ClrCursorRelImplementor implementor,
            ClrPhysType inputResultPhysType,
            JavaRowFormat inputResultFormat,
            Group group,
            int windowIdx,
            ClrPhysType inputPhysType,
            Expression source,
            ClrCursorPrefer pref,
            java.util.List translatedConstants,
            List<ParameterExpression> variables,
            List<Expression> body,
            out ClrPhysType outputPhysType)
        {
            var typeFactory = implementor.TypeFactory;
            var translator = implementor.Translator;

            // the aggregate implementors write linq4j blocks, the frame reads rows through Rex, and the output
            // row is built with record, so the input and output each also have a Calcite physical type for the
            // field reads inside those blocks. A partition is an Object[], as in Calcite, so its rows are boxed
            // Java values, which Calcite's comparator expects
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);

            var sourceType = inputPhysType.RowType;
            source = source;

            // orders a partition's rows; EXCLUDE and RANK also use it to decide whether two rows are peers
            var comparator_ = J.Expressions.parameter((java.lang.Class)typeof(java.util.Comparator), $"comparator{windowIdx}");
            var comparator = Hoist(translator, variables, body, comparator_,
                inputPhysType.GenerateComparator(group.collation()), typeof(java.util.Comparator));

            var aggs = new java.util.ArrayList();
            var aggregateCalls = group.getAggregateCalls(this);
            for (int aggIdx = 0; aggIdx < aggregateCalls.size(); aggIdx++)
            {
                var call = (AggregateCall)aggregateCalls.get(aggIdx);
                if (call.ignoreNulls())
                {
                    switch (call.getAggregation().getKind().name())
                    {
                        case nameof(SqlKind.FIRST_VALUE):
                        case nameof(SqlKind.LAST_VALUE):
                            // their implementors read IGNORE NULLS through ClrWinAggContext.ignoreNulls
                            break;
                        default:
                            throw new java.lang.UnsupportedOperationException("IGNORE NULLS not supported");
                    }
                }

                aggs.add(new ClrAggImpState(aggIdx, call, true, RexImplementorTables.of(getCluster())));
            }

            // the output of this group is its input plus one field per aggregate
            var typeBuilder = typeFactory.builder();
            typeBuilder.addAll(inputPhysType.RelRowType.getFieldList());
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);

                // an aggregate call's name may be null; Calcite fails with the same message
                typeBuilder.add(agg.call.name ?? throw new java.lang.NullPointerException($"agg.call.name for {agg.call}"), agg.call.type);
            }

            outputPhysType = ClrPhysTypeImpl.Of(typeFactory, typeBuilder.build(), pref.Prefer(inputResultFormat));
            var outputCalcite = PhysTypeImpl.of(typeFactory, outputPhysType.RelRowType, outputPhysType.Format, false);

            // a bounded RANGE frame finds its bounds by binary search over the collation key
            J.ParameterExpression? keySelector_ = null;
            J.ParameterExpression? keyComparator_ = null;
            if ((group.isRows || (group.upperBound.isUnbounded() && group.lowerBound.isUnbounded())) == false)
            {
                var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(group.collation().getFieldCollations());

                keySelector_ = J.Expressions.parameter((java.lang.Class)typeof(Function1), $"keySelector{windowIdx}");
                keyComparator_ = J.Expressions.parameter((java.lang.Class)typeof(java.util.Comparator), $"keyComparator{windowIdx}");

                Hoist(translator, variables, body, keySelector_, keySelector, typeof(Function1));
                Hoist(translator, variables, body, keyComparator_, collationComparator ?? throw new java.lang.NullPointerException("keyComparator"), typeof(java.util.Comparator));
            }

            var loop = new WindowLoop(inputCalcite);
            var inputGetter = new WindowRelInputGetter(loop.Row, inputCalcite, inputResultPhysType.RelRowType.getFieldCount(), translatedConstants);

            // the output row is every input field, then each aggregate's last result
            var inputFieldCount = inputPhysType.RelRowType.getFieldCount();
            var outputRow = new java.util.ArrayList();
            for (int i = 0; i < inputFieldCount; i++)
                outputRow.add(inputCalcite.fieldReference(loop.Row, i, outputCalcite.getJavaFieldType(i)));

            // as Calcite's declareAndResetState: each aggregate's state, the variable holding its last result,
            // and the block that initializes both
            var initBlock = new J.BlockBuilder();
            var stateTypes = new java.util.ArrayList();
            var initExpressions = new java.util.ArrayList();
            DeclareAndResetState(typeFactory, inputResultPhysType, constants, windowIdx, aggs, outputCalcite, outputRow, group.exclude, initBlock, stateTypes, initExpressions);

            var accPhysType = PhysTypeImplWorkaround.Of(typeFactory, typeFactory.createSyntheticType(stateTypes));
            ClrCursorAggregateBase.DeclareParentAccumulator(initExpressions, initBlock, accPhysType);

            var accType = ClrTypes.Resolve(accPhysType.getJavaRowType());
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            loop.Accumulator = acc_;

            // Calcite's local variables become fields of the accumulator, which each lambda takes and returns
            for (int i = 0, slot = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);

                var state = new java.util.ArrayList(agg.state.size());
                for (int j = 0; j < agg.state.size(); j++)
                    state.add(accPhysType.fieldReference(acc_, slot++));

                agg.state = state;
                agg.result = accPhysType.fieldReference(acc_, slot++);
                outputRow.set(inputFieldCount + i, agg.result);
            }

            var initializer = Expression.Lambda(
                typeof(Func<>).MakeGenericType(accType),
                translator.TranslateBody(initBlock.toBlock(), accType));

            // the frame bounds, each in its own block because each becomes its own lambda
            var min_ = J.Expressions.constant(0);

            var lowerBuilder = new J.BlockBuilder();
            var startUnchecked = TranslateBound(
                RexToLixTranslator.forAggregation(typeFactory, lowerBuilder, inputGetter, implementor.Conformance),
                typeFactory, loop.Index, loop.Row, min_, loop.MaxX, loop.Rows, group, true, inputCalcite, keySelector_, keyComparator_);
            lowerBuilder.add(J.Expressions.return_(null, startUnchecked));

            var upperBuilder = new J.BlockBuilder();
            var endUnchecked = TranslateBound(
                RexToLixTranslator.forAggregation(typeFactory, upperBuilder, inputGetter, implementor.Conformance),
                typeFactory, loop.Index, loop.Row, min_, loop.MaxX, loop.Rows, group, false, inputCalcite, keySelector_, keyComparator_);
            upperBuilder.add(J.Expressions.return_(null, endUnchecked));

            // a bound that is unbounded or the current row is already inside the partition and needs no clamp
            var clampStart = group.lowerBound.isUnbounded() == false && ReferenceEquals(startUnchecked, loop.Index) == false;
            var clampEnd = group.upperBound.isUnbounded() == false && ReferenceEquals(endUnchecked, loop.Index) == false;

            var lowerBound = loop.Lambda(translator, lowerBuilder.toBlock(), typeof(int), null);
            var upperBound = loop.Lambda(translator, upperBuilder.toBlock(), typeof(int), null);

            var frame = new java.util.function.DelegateFunction<J.BlockBuilder, WinAggFrameResultContext>(
                block => new ClrWinAggFrameResultContext(
                    block, typeFactory, implementor.Conformance, inputCalcite, inputResultPhysType.RelRowType.getFieldCount(),
                    translatedConstants, comparator_, loop.Rows, loop.Index, loop.Start, loop.End, min_, loop.MaxX,
                    loop.HasRows, loop.FrameRowCount, loop.PartitionRowCount, loop.Position));

            // reset, add, and the two halves of result, in the order Calcite calls them: the implementors keep
            // state of their own across the calls
            var resetBuilder = new J.BlockBuilder();
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                agg.Implementor.implementReset(agg.context,
                    new WinAggResetContextImpl(resetBuilder, agg.state, loop.Index, loop.Start, loop.End, loop.HasRows, loop.FrameRowCount, loop.PartitionRowCount));
            }

            var addBuilder = new J.BlockBuilder();
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                agg.Implementor.implementAdd(agg.context,
                    new ClrWinAggAddContext(addBuilder, agg.state, frame, loop.Position, RexArguments(agg, inputResultPhysType.RelRowType, constants), RexFilterArgument(agg, inputResultPhysType.RelRowType)));
            }

            var cachedBuilder = new J.BlockBuilder();
            var cached = ImplementResult(aggs, cachedBuilder, frame, true, inputResultPhysType.RelRowType, constants);

            var uncachedBuilder = new J.BlockBuilder();
            var uncached = ImplementResult(aggs, uncachedBuilder, frame, false, inputResultPhysType.RelRowType, constants);

            var resetBlock = resetBuilder.toBlock();
            var addBlock = addBuilder.toBlock();

            var accumulatorType = typeof(Func<,,>).MakeGenericType(typeof(WindowFrame), accType, accType);
            var reset = resetBlock.statements.size() == 0 ? null : loop.Lambda(translator, WithReturn(resetBlock, acc_), accType, accType);
            var adder = addBlock.statements.size() == 0 ? null : loop.Lambda(translator, WithReturn(addBlock, acc_), accType, accType);
            var cachedResult = cached == false ? null : loop.Lambda(translator, WithReturn(cachedBuilder.toBlock(), acc_), accType, accType);
            var uncachedResult = uncached == false ? null : loop.Lambda(translator, WithReturn(uncachedBuilder.toBlock(), acc_), accType, accType);

            var selectorBuilder = new J.BlockBuilder();
            selectorBuilder.add(J.Expressions.return_(null, outputCalcite.record(outputRow)));

            var outputType = outputPhysType.RowType;
            var selector = loop.Lambda(translator, selectorBuilder.toBlock(), outputType, accType);

            // the partition key, the only piece that reads a row outside the loop
            var partitionSelector = PartitionSelector(translator, inputCalcite, group, sourceType);
            var keyType = partitionSelector?.ReturnType ?? typeof(object);

            return ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.WindowAsync.MakeGenericMethod(sourceType, keyType, accType, outputType),
                source,
                (Expression?)partitionSelector ?? Expression.Constant(null, typeof(Func<,>).MakeGenericType(sourceType, keyType)),
                comparator,
                Expression.Constant(group.exclude, typeof(RexWindowExclusion)),
                lowerBound,
                upperBound,
                Expression.Constant(group.isAlwaysNonEmpty()),
                Expression.Constant(clampStart),
                Expression.Constant(clampEnd),
                Expression.Constant(group.lowerBound.isUnboundedPreceding() == false),
                initializer,
                (Expression?)reset ?? Expression.Constant(null, accumulatorType),
                (Expression?)adder ?? Expression.Constant(null, accumulatorType),
                (Expression?)cachedResult ?? Expression.Constant(null, accumulatorType),
                (Expression?)uncachedResult ?? Expression.Constant(null, accumulatorType),
                selector);
        }

        /// <summary>
        /// Evaluates a value once into a variable of the node's block, and binds a linq4j parameter to that
        /// variable so the blocks Calcite's generators write for the group can refer to it.
        /// </summary>
        /// <param name="translator">The translator to bind the parameter in.</param>
        /// <param name="variables">Receives the new variable.</param>
        /// <param name="body">Receives the assignment.</param>
        /// <param name="parameter">The linq4j parameter the generated blocks refer to.</param>
        /// <param name="value">The value to evaluate.</param>
        /// <param name="type">The variable's type.</param>
        /// <returns>The variable.</returns>
        static ParameterExpression Hoist(LixToClrTranslator translator, List<ParameterExpression> variables, List<Expression> body, J.ParameterExpression parameter, Expression value, Type type)
        {
            // Calcite's runtime reads some of these as linq4j functional interfaces (BinarySearch takes the key
            // selector as a Function1), and a delegate cannot be converted to one, so a lambda is wrapped
            if (value is LambdaExpression lambda && AnonymousClasses.Handles(type))
                value = AnonymousClasses.Wrap(type, lambda);

            var variable = Expression.Variable(type, parameter.name);
            variables.Add(variable);
            body.Add(Expression.Assign(variable, ClrEnumUtils.Convert(value, type)));
            translator.Bind(parameter, variable);

            return variable;
        }

        /// <summary>
        /// Returns a lambda giving a row's partition key, or null where the group does not partition.
        /// </summary>
        /// <param name="translator">The translator.</param>
        /// <param name="inputPhysType">Calcite's physical type of the group's input.</param>
        /// <param name="group">The window group.</param>
        /// <param name="sourceType">The input row type.</param>
        /// <returns>The key selector, or null.</returns>
        /// <remarks>
        /// Builds the key <c>EnumerableWindow.getPartitionIterator</c> builds: a synthetic record for several
        /// key fields, the field itself for one.
        /// </remarks>
        static LambdaExpression? PartitionSelector(LixToClrTranslator translator, PhysType inputPhysType, Group group, Type sourceType)
        {
            if (group.keys.isEmpty())
                return null;

            var v_ = J.Expressions.parameter(inputPhysType.getJavaRowType(), "v");
            var selector = inputPhysType.selector(v_, group.keys.asList(), JavaRowFormat.CUSTOM);

            var builder = new J.BlockBuilder();
            J.ParameterExpression key_;

            if (selector.getKey() is J.Types.RecordType keyJavaType)
            {
                var initExpressions = (java.util.List)selector.getValue();
                key_ = J.Expressions.parameter(keyJavaType, "key");
                builder.add(J.Expressions.declare(0, key_, null));
                builder.add(J.Expressions.statement(J.Expressions.assign(key_, J.Expressions.new_(keyJavaType))));

                var fieldList = keyJavaType.getRecordFields();
                for (int i = 0; i < initExpressions.size(); i++)
                    builder.add(J.Expressions.statement(J.Expressions.assign(J.Expressions.field(key_, (J.Types.RecordField)fieldList.get(i)), (J.Expression)initExpressions.get(i))));
            }
            else
            {
                var declare = J.Expressions.declare(0, "key", (J.Expression)((java.util.List)selector.getValue()).get(0));
                builder.add(declare);
                key_ = declare.parameter;
            }

            builder.add(J.Expressions.return_(null, key_));

            // the parameter has the boxed row type; the block reads the row at Calcite's Java row type, so it is
            // converted first
            var parameter = Expression.Parameter(sourceType, "v");
            var row = Expression.Variable(ClrTypes.Resolve(inputPhysType.getJavaRowType()), "v");
            translator.Bind(v_, row);

            // the key goes into a java.util.HashMap, so a primitive key is boxed as a Java value: a CLR-boxed int
            // does not equal a java.lang.Integer
            var keyType = ClrTypes.Resolve(J.Primitive.box(key_.getType()));

            return Expression.Lambda(
                typeof(Func<,>).MakeGenericType(sourceType, keyType),
                Expression.Block(keyType, [row],
                    Expression.Assign(row, ClrEnumUtils.Convert(parameter, row.Type)),
                    translator.TranslateBody(builder.toBlock(), keyType)),
                parameter);
        }

        /// <summary>
        /// Returns a copy of a block with a return of <paramref name="value"/> appended.
        /// </summary>
        /// <param name="block">The block.</param>
        /// <param name="value">The value to return.</param>
        /// <returns>The new block.</returns>
        /// <remarks>
        /// The implementors' blocks have no return, because Calcite inlines them into a larger method. Here each
        /// becomes a lambda that returns the accumulator.
        /// </remarks>
        static J.BlockStatement WithReturn(J.BlockStatement block, J.Expression value)
        {
            var statements = new java.util.ArrayList(block.statements);
            statements.add(J.Expressions.return_(null, value));

            return J.Expressions.block(statements);
        }

        /// <summary>
        /// Declares each aggregate's state variables and the variable holding its last result, and writes the
        /// block that initializes them.
        /// </summary>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="inputResultPhysType">The physical type of the node's input.</param>
        /// <param name="constants">The node's constants.</param>
        /// <param name="windowIdx">The group's index, used to name the variables.</param>
        /// <param name="aggs">The <see cref="ClrAggImpState"/> of each call; each gets its context, state and result set.</param>
        /// <param name="outputPhysType">Calcite's physical type of the group's output.</param>
        /// <param name="outputRow">The output row's field expressions; receives each result variable.</param>
        /// <param name="exclusion">The group's EXCLUDE clause.</param>
        /// <param name="initBlock">The block that initializes the accumulator.</param>
        /// <param name="stateTypes">Receives the Java type of every declared variable, in order.</param>
        /// <param name="initExpressions">Receives every declared variable, in order.</param>
        /// <remarks>
        /// Mirrors <c>EnumerableWindow.declareAndResetState</c>, which is private. Calcite leaves the variables as
        /// locals of the generated method; here they are collected to become the accumulator's fields.
        /// </remarks>
        static void DeclareAndResetState(
            JavaTypeFactory typeFactory,
            ClrPhysType inputResultPhysType,
            java.util.List constants,
            int windowIdx,
            java.util.List aggs,
            PhysType outputPhysType,
            java.util.List outputRow,
            RexWindowExclusion exclusion,
            J.BlockBuilder initBlock,
            java.util.List stateTypes,
            java.util.List initExpressions)
        {
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                agg.context = new ClrWinAggContext(agg, typeFactory, inputResultPhysType.RelRowType, constants, exclusion);

                var aggName = ClrCursorAggregateBase.AggName(agg);
                var state = agg.Implementor.getStateType(agg.context);

                var decls = new java.util.ArrayList(state.size());
                for (int j = 0; j < state.size(); j++)
                {
                    var type = (java.lang.reflect.Type)state.get(j);
                    var pe = J.Expressions.parameter(type, initBlock.newName($"{aggName}s{j}w{windowIdx}"));
                    initBlock.add(J.Expressions.declare(0, pe, null));

                    decls.add(pe);
                    stateTypes.add(type);
                    initExpressions.add(pe);
                }

                agg.state = decls;

                var aggHolderType = agg.context.returnType();
                var aggStorageType = outputPhysType.getJavaFieldType(outputRow.size());
                if (J.Primitive.@is(aggHolderType) && J.Primitive.@is(aggStorageType) == false)
                    aggHolderType = J.Primitive.box(aggHolderType);

                var aggRes = J.Expressions.parameter(0, aggHolderType, initBlock.newName($"{aggName}w{windowIdx}"));
                var primitive = J.Primitive.of(aggRes.getType());
                initBlock.add(J.Expressions.declare(0, aggRes, J.Expressions.constant(primitive?.defaultValue, aggRes.getType())));

                agg.result = aggRes;
                outputRow.add(aggRes);
                stateTypes.add(aggHolderType);
                initExpressions.add(aggRes);

                agg.Implementor.implementReset(agg.context, new WinAggResetContextImpl(initBlock, agg.state, null, null, null, null, null, null));
            }
        }

        /// <summary>
        /// Writes the block computing the results of one half of the aggregates.
        /// </summary>
        /// <param name="aggs">The <see cref="ClrAggImpState"/> of each call.</param>
        /// <param name="builder">The block to write into.</param>
        /// <param name="frame">Creates the frame context for a block.</param>
        /// <param name="cachedBlock">Whether this is the half computed only when the frame has changed.</param>
        /// <param name="inputRowType">The node's input row type.</param>
        /// <param name="constants">The node's constants.</param>
        /// <returns>Whether any aggregate belongs to this half.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableWindow.implementResult</c>, which is private. An aggregate whose result cannot
        /// change while the frame is unchanged is computed in the cached half; the others are computed for every
        /// row.
        /// </remarks>
        static bool ImplementResult(java.util.List aggs, J.BlockBuilder builder, java.util.function.Function frame, bool cachedBlock, RelDataType inputRowType, java.util.List constants)
        {
            var nonEmpty = false;

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);

                var needCache = agg.Implementor is not WinAggImplementor implementor || implementor.needCacheWhenFrameIntact();
                if (needCache ^ cachedBlock)
                    continue;

                nonEmpty = true;

                var res = agg.Implementor.implementResult(agg.context,
                    new ClrWinAggResultContext(builder, agg.state, frame, RexArguments(agg, inputRowType, constants)));

                // several count(a) and count(b) might share the result
                var aggRes = builder.append($"a{agg.aggIdx}res", EnumUtils.convert(res, agg.result.getType()));
                builder.add(J.Expressions.statement(J.Expressions.assign(agg.result, aggRes)));
            }

            return nonEmpty;
        }

        /// <summary>
        /// Returns the arguments of an aggregate call as input references, typed from the input row or, for an
        /// index past its fields, from the constants.
        /// </summary>
        /// <param name="agg">The call's state.</param>
        /// <param name="inputRowType">The node's input row type.</param>
        /// <param name="constants">The node's constants.</param>
        /// <returns>The argument expressions.</returns>
        static java.util.List RexArguments(AggImpState agg, RelDataType inputRowType, java.util.List constants)
        {
            var argList = agg.call.getArgList();
            var inputTypes = ClrEnumUtils.FieldRowTypes(inputRowType, constants, argList);

            var args = new java.util.ArrayList(argList.size());
            for (int i = 0; i < argList.size(); i++)
                args.add(new RexInputRef(((java.lang.Integer)argList.get(i)).intValue(), (RelDataType)inputTypes.get(i)));

            return args;
        }

        /// <summary>
        /// Returns a reference to the field a window aggregate's FILTER reads, or null where the call has no
        /// FILTER.
        /// </summary>
        /// <param name="agg">The call's state.</param>
        /// <param name="inputRowType">The node's input row type.</param>
        /// <returns>The filter reference, or null.</returns>
        /// <remarks>
        /// Mirrors <c>rexFilterArgument</c> of the anonymous <c>WinAggAddContext</c> in
        /// <c>EnumerableWindow.implementAdd</c>. From SQL the call has no filter argument:
        /// <c>SqlToRelConverter</c> rewrites <c>COUNT(*) FILTER (WHERE p) OVER w</c> as
        /// <c>COUNT(CASE WHEN p THEN 0 END) OVER w</c>. A window built another way may carry one.
        /// </remarks>
        static RexNode? RexFilterArgument(AggImpState agg, RelDataType inputRowType)
        {
            return agg.call.filterArg < 0 ? null : RexInputRef.of(agg.call.filterArg, inputRowType);
        }

        /// <summary>
        /// Returns the linq4j expression giving the index of one bound of the frame, before clamping to the
        /// partition.
        /// </summary>
        /// <param name="translator">The Rex translator for the bound's block.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="i_">The current row's index.</param>
        /// <param name="row_">The current row.</param>
        /// <param name="min_">The partition's first index.</param>
        /// <param name="max_">The partition's last index.</param>
        /// <param name="rows_">The partition's rows.</param>
        /// <param name="group">The window group.</param>
        /// <param name="lower">Whether this is the lower bound.</param>
        /// <param name="physType">Calcite's physical type of the input.</param>
        /// <param name="keySelector">The collation key selector, required for a bounded RANGE frame.</param>
        /// <param name="keyComparator">The collation key comparator, required for a bounded RANGE frame.</param>
        /// <returns>The bound expression.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableWindow.translateBound</c>, which is private. A ROWS bound is an offset from the
        /// current index; a RANGE bound is a binary search over the collation key.
        /// </remarks>
        static J.Expression TranslateBound(
            RexToLixTranslator translator,
            JavaTypeFactory typeFactory,
            J.ParameterExpression i_,
            J.Expression row_,
            J.Expression min_,
            J.Expression max_,
            J.Expression rows_,
            Group group,
            bool lower,
            PhysType physType,
            J.Expression? keySelector,
            J.Expression? keyComparator)
        {
            var bound = lower ? group.lowerBound : group.upperBound;
            if (bound.isUnbounded())
                return bound.isPreceding() ? min_ : max_;

            if (group.isRows)
            {
                if (bound.isCurrentRow())
                    return i_;

                // the offset is converted to int, as it indexes the partition array
                var offs = EnumUtils.convert(ClrEnumUtils.Translate(translator, bound.getOffset(), null), J.Primitive.INT.primitiveClass);

                return bound.isFollowing()
                    ? J.Expressions.add(i_, offs)
                    : J.Expressions.subtract(i_, offs);
            }

            var searchLower = min_;
            var searchUpper = max_;
            if (bound.isCurrentRow())
            {
                if (lower)
                    searchUpper = i_;
                else
                    searchLower = i_;
            }

            var fieldCollations = group.collation().getFieldCollations();
            if (bound.isCurrentRow() && fieldCollations.size() != 1)
                return J.Expressions.call(
                    (lower ? BuiltInMethod.BINARY_SEARCH5_LOWER : BuiltInMethod.BINARY_SEARCH5_UPPER).method,
                    rows_, row_, searchLower, searchUpper,
                    keySelector ?? throw new java.lang.NullPointerException("keySelector"),
                    keyComparator ?? throw new java.lang.NullPointerException("keyComparator"));

            var orderKey = ((RelFieldCollation)fieldCollations.get(0)).getFieldIndex();
            var keyType = ((RelDataTypeField)physType.getRowType().getFieldList().get(orderKey)).getType();

            var desiredKeyType = typeFactory.getJavaClass(keyType);
            if (bound.getOffset() == null)
                desiredKeyType = J.Primitive.box(desiredKeyType);

            var val = ClrEnumUtils.Translate(translator, new RexInputRef(orderKey, keyType), desiredKeyType);
            if (bound.isCurrentRow() == false)
            {
                var offs = ClrEnumUtils.Translate(translator, bound.getOffset(), null);
                val = bound.isFollowing() ? J.Expressions.add(val, offs) : J.Expressions.subtract(val, offs);
            }

            return J.Expressions.call(
                (lower ? BuiltInMethod.BINARY_SEARCH6_LOWER : BuiltInMethod.BINARY_SEARCH6_UPPER).method,
                rows_, val, searchLower, searchUpper,
                keySelector ?? throw new java.lang.NullPointerException("keySelector"),
                keyComparator ?? throw new java.lang.NullPointerException("keyComparator"));
        }


        /// <summary>
        /// The loop variables a window group's generated blocks read, and the construction of the lambdas those
        /// blocks become.
        /// </summary>
        /// <param name="inputPhysType">Calcite's physical type of the group's input.</param>
        /// <remarks>
        /// Calcite declares these as locals of the generated method. Here each is a linq4j parameter which, when a
        /// block is translated, is bound to a variable the lambda assigns from its <see cref="WindowFrame"/>
        /// argument.
        /// </remarks>
        sealed class WindowLoop(PhysType inputPhysType)
        {

            static readonly java.lang.reflect.Type IntType = J.Primitive.INT.primitiveClass;
            static readonly java.lang.reflect.Type BooleanType = J.Primitive.BOOLEAN.primitiveClass;

            /// <summary>The rows of the partition.</summary>
            public J.ParameterExpression Rows { get; } = J.Expressions.parameter((java.lang.Class)typeof(object[]), "rows");

            /// <summary>The index of the row being evaluated.</summary>
            public J.ParameterExpression Index { get; } = J.Expressions.parameter(IntType, "i");

            /// <summary>The first index of the frame.</summary>
            public J.ParameterExpression Start { get; } = J.Expressions.parameter(IntType, "startChecked");

            /// <summary>The last index of the frame.</summary>
            public J.ParameterExpression End { get; } = J.Expressions.parameter(IntType, "endChecked");

            /// <summary>Whether the frame holds any row.</summary>
            public J.ParameterExpression HasRows { get; } = J.Expressions.parameter(BooleanType, "hasRows");

            /// <summary>How many rows the frame holds.</summary>
            public J.ParameterExpression FrameRowCount { get; } = J.Expressions.parameter(IntType, "totalRows");

            /// <summary>How many rows the partition holds.</summary>
            public J.ParameterExpression PartitionRowCount { get; } = J.Expressions.parameter(IntType, "partRows");

            /// <summary>The last index of the partition.</summary>
            public J.ParameterExpression MaxX { get; } = J.Expressions.parameter(IntType, "maxX");

            /// <summary>The index of the row being folded in.</summary>
            public J.ParameterExpression Position { get; } = J.Expressions.parameter(IntType, "j");

            /// <summary>The row being evaluated.</summary>
            public J.ParameterExpression Row { get; } = J.Expressions.parameter(inputPhysType.getJavaRowType(), "row");

            /// <summary>The accumulator; set once its record type has been built.</summary>
            public J.ParameterExpression? Accumulator { get; set; }

            /// <summary>
            /// Translates a generated block into a lambda over a <see cref="WindowFrame"/> and, optionally, the
            /// accumulator.
            /// </summary>
            /// <param name="translator">The translator.</param>
            /// <param name="block">The block.</param>
            /// <param name="returnType">The lambda's return type.</param>
            /// <param name="accumulatorType">The accumulator's type, or null where the lambda does not take one.</param>
            /// <returns>The lambda.</returns>
            /// <exception cref="InvalidOperationException"><paramref name="accumulatorType"/> is given before
            /// <see cref="Accumulator"/> is set.</exception>
            public LambdaExpression Lambda(LixToClrTranslator translator, J.BlockStatement block, Type returnType, Type? accumulatorType)
            {
                var frame = Expression.Parameter(typeof(WindowFrame), "frame");
                var rowType = ClrTypes.Resolve(inputPhysType.getJavaRowType());

                // fresh variables for each lambda, since each declares its own
                var rows = Expression.Variable(typeof(object[]), "rows");
                var index = Expression.Variable(typeof(int), "i");
                var start = Expression.Variable(typeof(int), "startChecked");
                var end = Expression.Variable(typeof(int), "endChecked");
                var hasRows = Expression.Variable(typeof(bool), "hasRows");
                var frameRowCount = Expression.Variable(typeof(int), "totalRows");
                var partitionRowCount = Expression.Variable(typeof(int), "partRows");
                var maxX = Expression.Variable(typeof(int), "maxX");
                var position = Expression.Variable(typeof(int), "j");
                var row = Expression.Variable(rowType, "row");

                translator.Bind(Rows, rows);
                translator.Bind(Index, index);
                translator.Bind(Start, start);
                translator.Bind(End, end);
                translator.Bind(HasRows, hasRows);
                translator.Bind(FrameRowCount, frameRowCount);
                translator.Bind(PartitionRowCount, partitionRowCount);
                translator.Bind(MaxX, maxX);
                translator.Bind(Position, position);
                translator.Bind(Row, row);

                ParameterExpression? accumulator = null;
                if (accumulatorType != null)
                {
                    accumulator = Expression.Parameter(accumulatorType, "acc");
                    translator.Bind(Accumulator ?? throw new InvalidOperationException("The accumulator has not been built."), accumulator);
                }

                var prologue = new List<Expression>
                {
                    Expression.Assign(rows, Expression.Property(frame, nameof(WindowFrame.Rows))),
                    Expression.Assign(index, Expression.Property(frame, nameof(WindowFrame.Index))),
                    Expression.Assign(start, Expression.Property(frame, nameof(WindowFrame.Start))),
                    Expression.Assign(end, Expression.Property(frame, nameof(WindowFrame.End))),
                    Expression.Assign(hasRows, Expression.Property(frame, nameof(WindowFrame.HasRows))),
                    Expression.Assign(frameRowCount, Expression.Property(frame, nameof(WindowFrame.FrameRowCount))),
                    Expression.Assign(partitionRowCount, Expression.Property(frame, nameof(WindowFrame.PartitionRowCount))),
                    Expression.Assign(maxX, Expression.Subtract(partitionRowCount, Expression.Constant(1))),
                    Expression.Assign(position, Expression.Property(frame, nameof(WindowFrame.Position))),
                    Expression.Assign(row, ClrEnumUtils.Convert(Expression.ArrayAccess(rows, index), rowType)),
                    translator.TranslateBody(block, returnType),
                };

                var body = Expression.Block(returnType, [rows, index, start, end, hasRows, frameRowCount, partitionRowCount, maxX, position, row], prologue);

                return accumulator == null
                    ? Expression.Lambda(typeof(Func<,>).MakeGenericType(typeof(WindowFrame), returnType), body, frame)
                    : Expression.Lambda(typeof(Func<,,>).MakeGenericType(typeof(WindowFrame), accumulatorType!, returnType), body, frame, accumulator);
            }

        }

        /// <summary>
        /// Reads a field of the row a window aggregate is evaluated against, or a constant where the index is
        /// past the input's fields.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="rowPhysType">Calcite's physical type of the row.</param>
        /// <param name="actualInputFieldCount">The number of fields of the node's input.</param>
        /// <param name="constants">The translated constants.</param>
        /// <remarks>
        /// Mirrors <c>EnumerableWindow.WindowRelInputGetter</c>, which is private.
        /// </remarks>
        sealed class WindowRelInputGetter(J.Expression row, PhysType rowPhysType, int actualInputFieldCount, java.util.List constants) : RexToLixTranslator.InputGetter
        {

            /// <inheritdoc />
            public J.Expression field(J.BlockBuilder list, int index, java.lang.reflect.Type storageType)
            {
                if (index < actualInputFieldCount)
                    return rowPhysType.fieldReference(list.append("current", row), index, storageType);

                return (J.Expression)constants.get(index - actualInputFieldCount);
            }

        }

        /// <summary>
        /// The <see cref="WinAggContext"/> a window aggregate implementor is given for its call.
        /// </summary>
        /// <param name="agg">The call's state.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="inputRowType">The node's input row type.</param>
        /// <param name="constants">The node's constants.</param>
        /// <param name="exclusion">The group's EXCLUDE clause.</param>
        /// <remarks>
        /// Mirrors the anonymous <c>WinAggContext</c> in <c>EnumerableWindow.declareAndResetState</c>. A window
        /// has no grouping, so the four grouping members throw, as Calcite's do.
        /// </remarks>
        sealed class ClrWinAggContext(AggImpState agg, JavaTypeFactory typeFactory, RelDataType inputRowType, java.util.List constants, RexWindowExclusion exclusion) : WinAggContext
        {

            /// <inheritdoc />
            public SqlAggFunction aggregation() => agg.call.getAggregation();

            /// <inheritdoc />
            public RelDataType returnRelType() => agg.call.type;

            /// <inheritdoc />
            public java.lang.reflect.Type returnType() => ClrEnumUtils.JavaClass(typeFactory, returnRelType());

            /// <inheritdoc />
            public java.util.List parameterRelTypes() => ClrEnumUtils.FieldRowTypes(inputRowType, constants, agg.call.getArgList());

            /// <inheritdoc />
            public java.util.List parameterTypes() => ClrEnumUtils.FieldTypes(typeFactory, parameterRelTypes());

            /// <inheritdoc />
            public java.util.List groupSets() => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public java.util.List keyOrdinals() => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public java.util.List keyRelTypes() => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public java.util.List keyTypes() => throw new java.lang.UnsupportedOperationException();

            /// <inheritdoc />
            public RexWindowExclusion getExclude() => exclusion;

            /// <summary>
            /// Returns whether the call carries IGNORE NULLS.
            /// </summary>
            /// <returns>True if null values are ignored.</returns>
            /// <remarks>
            /// Implements <c>WinAggContext.ignoreNulls</c>, which the FIRST_VALUE and LAST_VALUE implementors read.
            /// </remarks>
            public bool ignoreNulls() => agg.call.ignoreNulls();

        }

        /// <summary>
        /// The <see cref="WinAggFrameResultContext"/> through which a window aggregate implementor reads the
        /// frame and partition while writing into a block.
        /// </summary>
        /// <remarks>
        /// Mirrors the anonymous <c>WinAggFrameResultContext</c> of
        /// <c>EnumerableWindow.getBlockBuilderWinAggFrameResultContextFunction</c>. One is made per block, which
        /// is why the contexts take a function rather than an instance.
        /// </remarks>
        sealed class ClrWinAggFrameResultContext(
            J.BlockBuilder block,
            JavaTypeFactory typeFactory,
            SqlConformance conformance,
            PhysType inputPhysType,
            int actualInputFieldCount,
            java.util.List constants,
            J.Expression comparator,
            J.Expression rows,
            J.ParameterExpression currentIndex,
            J.Expression start,
            J.Expression end,
            J.Expression min,
            J.Expression max,
            J.Expression anyRows,
            J.Expression frameRows,
            J.Expression partitionRows,
            J.ParameterExpression position) : WinAggFrameResultContext
        {

            /// <inheritdoc />
            public RexToLixTranslator rowTranslator(J.Expression rowIndex)
            {
                return RexToLixTranslator.forAggregation(
                    typeFactory,
                    block,
                    new WindowRelInputGetter(GetRow(rowIndex), inputPhysType, actualInputFieldCount, constants),
                    conformance);
            }

            /// <inheritdoc />
            public J.Expression computeIndex(J.Expression offset, WinAggImplementor.SeekType seekType)
            {
                var absolute = seekType.name() switch
                {
                    nameof(WinAggImplementor.SeekType.AGG_INDEX) => (J.Expression)position,
                    nameof(WinAggImplementor.SeekType.SET) => currentIndex,
                    nameof(WinAggImplementor.SeekType.START) => start,
                    nameof(WinAggImplementor.SeekType.END) => end,
                    _ => throw new java.lang.IllegalArgumentException($"SeekSet {seekType} is not supported")
                };

                if (java.util.Objects.equals(J.Expressions.constant(java.lang.Integer.valueOf(0)), offset) == false)
                    absolute = block.append("idx", J.Expressions.add(absolute, offset));

                return absolute;
            }

            /// <inheritdoc />
            public J.Expression rowInFrame(J.Expression rowIndex) => CheckBounds(rowIndex, start, end);

            /// <inheritdoc />
            public J.Expression rowInPartition(J.Expression rowIndex) => CheckBounds(rowIndex, min, max);

            /// <inheritdoc />
            public J.Expression compareRows(J.Expression a, J.Expression b)
            {
                return J.Expressions.call(comparator, BuiltInMethod.COMPARATOR_COMPARE.method, GetRow(a), GetRow(b));
            }

            /// <inheritdoc />
            public J.Expression index() => currentIndex;

            /// <inheritdoc />
            public J.Expression startIndex() => start;

            /// <inheritdoc />
            public J.Expression endIndex() => end;

            /// <inheritdoc />
            public J.Expression hasRows() => anyRows;

            /// <inheritdoc />
            public J.Expression getFrameRowCount() => frameRows;

            /// <inheritdoc />
            public J.Expression getPartitionRowCount() => partitionRows;

            /// <summary>
            /// Returns an expression reading the partition's row at an index, declared in the block.
            /// </summary>
            /// <param name="rowIndex">The index.</param>
            /// <returns>The row expression.</returns>
            J.Expression GetRow(J.Expression rowIndex)
            {
                return block.append("jRow", EnumUtils.convert(J.Expressions.arrayIndex(rows, rowIndex), inputPhysType.getJavaRowType()));
            }

            /// <summary>
            /// Returns an expression testing that the frame has rows and an index lies between two others,
            /// inclusive.
            /// </summary>
            /// <param name="rowIndex">The index to test.</param>
            /// <param name="minIndex">The lowest valid index.</param>
            /// <param name="maxIndex">The highest valid index.</param>
            /// <returns>The test expression.</returns>
            J.Expression CheckBounds(J.Expression rowIndex, J.Expression minIndex, J.Expression maxIndex)
            {
                // the current, start and end indexes are within bounds whenever the frame has rows
                if (ReferenceEquals(rowIndex, currentIndex) || ReferenceEquals(rowIndex, start) || ReferenceEquals(rowIndex, end))
                    return anyRows;

                var conditions = new java.util.ArrayList(3);
                conditions.add(anyRows);
                conditions.add(J.Expressions.greaterThanOrEqual(rowIndex, minIndex));
                conditions.add(J.Expressions.lessThanOrEqual(rowIndex, maxIndex));

                return block.append("rowInFrame", J.Expressions.foldAnd(conditions));
            }

        }

        /// <summary>
        /// The <see cref="WinAggAddContext"/> a window aggregate implementor is given to fold one row in.
        /// </summary>
        /// <param name="block">The block to write into.</param>
        /// <param name="accumulator">The call's state fields.</param>
        /// <param name="frame">Creates the frame context for a block.</param>
        /// <param name="position">The index of the row being folded in.</param>
        /// <param name="rexArgs">The call's arguments.</param>
        /// <param name="filterArg">The call's FILTER reference, or null.</param>
        sealed class ClrWinAggAddContext(J.BlockBuilder block, java.util.List accumulator, java.util.function.Function frame, J.ParameterExpression position, java.util.List rexArgs, RexNode? filterArg) :
            WinAggAddContextImpl(block, accumulator, frame)
        {

            /// <inheritdoc />
            public override J.Expression currentPosition() => position;

            /// <inheritdoc />
            public override java.util.List rexArguments() => rexArgs;

            /// <inheritdoc />
            public override RexNode? rexFilterArgument() => filterArg;

        }

        /// <summary>
        /// The <see cref="WinAggResultContext"/> a window aggregate implementor is given to write its result.
        /// </summary>
        /// <param name="block">The block to write into.</param>
        /// <param name="accumulator">The call's state fields.</param>
        /// <param name="frame">Creates the frame context for a block.</param>
        /// <param name="rexArgs">The call's arguments.</param>
        sealed class ClrWinAggResultContext(J.BlockBuilder block, java.util.List accumulator, java.util.function.Function frame, java.util.List rexArgs) :
            WinAggResultContextImpl(block, accumulator, frame)
        {

            /// <inheritdoc />
            public override java.util.List rexArguments() => rexArgs;

        }

    }

}
