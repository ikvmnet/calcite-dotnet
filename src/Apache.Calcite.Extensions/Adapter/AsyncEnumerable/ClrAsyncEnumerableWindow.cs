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

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Window"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The loop is <see cref="ClrAsyncEnumerableDefaults.Window"/>, written once, exactly as the loop of a calc is
    /// <see cref="ClrAsyncEnumerableDefaults.Calc"/>: partitioning the input, ordering each partition, walking its rows,
    /// deciding when the frame has changed enough to be computed again, and folding the rows of the frame in.
    /// Generated Java source is the only place Calcite can put a loop, which is why its own is generated.
    ///
    /// <para>What this node translates is what a generator of Calcite's produced: the window aggregate
    /// implementors' reset, add and result blocks; <c>translateBound</c>, ported here because it is private;
    /// the partition key, the comparator and the collation key from <c>PhysType</c>; and the literals of
    /// <c>constants</c>. Each is translated where it is produced.</para>
    ///
    /// <para>Calcite declares each aggregate's state, and its last result, as local variables of the generated
    /// method and mutates them in place, which no lambda can carry. They become the fields of one synthetic
    /// record instead — the same answer <see cref="ClrAsyncEnumerableAggregate"/> gives to the same problem — and
    /// the loop variables the implementors read become parameters bound to the fields of a
    /// <see cref="WindowFrame"/>.</para>
    /// </remarks>
    public class ClrAsyncEnumerableWindow : Window, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="constants"></param>
        /// <param name="rowType"></param>
        /// <param name="groups"></param>
        public ClrAsyncEnumerableWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, java.util.List constants, RelDataType rowType, java.util.List groups) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, constants, rowType, groups)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrAsyncEnumerableWindow(getCluster(), traitSet, (RelNode)sole(inputs), constants, getRowType(), groups);
        }

        /// <inheritdoc />
        public override Window copy(java.util.List constants)
        {
            return new ClrAsyncEnumerableWindow(getCluster(), getTraitSet(), getInput(), constants, getRowType(), groups);
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);
            if (cost == null)
                return null;

            return cost.multiplyBy(ClrAsyncEnumerableConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrAsyncEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            // a window aggregate may name a constant past the input's own fields, so they are translated once
            // and the input getter hands them out by index
            var translatedConstants = new java.util.ArrayList(constants.size());
            for (int i = 0; i < constants.size(); i++)
            {
                var constant = (RexLiteral)constants.get(i);
                translatedConstants.add(RexToLixTranslator.translateLiteral(constant, constant.getType(), typeFactory, RexImpTable.NullAs.NULL));
            }

            // the comparators and collation keys are evaluated once and read by the lambdas of their group,
            // which is what Calcite's "final Comparator comparator = ..." ahead of the loop is
            var variables = new List<ParameterExpression>();
            var body = new List<Expression>();

            var physType = result.PhysType;
            var source = result.Expression;

            for (int windowIdx = 0; windowIdx < groups.size(); windowIdx++)
            {
                var group = (Group)groups.get(windowIdx);
                source = ImplementGroup(implementor, result, group, windowIdx, physType, source, pref, translatedConstants, variables, body, out physType);

                // one variable per group, as Calcite makes one "source" per group, so a later group reads what
                // an earlier one produced rather than building it again
                var sourceVariable = Expression.Variable(source.Type, $"source{windowIdx}");
                variables.Add(sourceVariable);
                body.Add(Expression.Assign(sourceVariable, source));
                source = sourceVariable;
            }

            body.Add(source);

            return implementor.Result(physType, Expression.Block(source.Type, variables, body));
        }

        /// <summary>
        /// Implements one window group over the sequence the group before it produced.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="result">What the input produced, which is what the aggregate arguments are indexed against however many groups have run.</param>
        /// <param name="group"></param>
        /// <param name="windowIdx"></param>
        /// <param name="inputPhysType"></param>
        /// <param name="source"></param>
        /// <param name="pref"></param>
        /// <param name="translatedConstants"></param>
        /// <param name="variables">Variables of the block the whole node becomes.</param>
        /// <param name="body">Statements of that block.</param>
        /// <param name="outputPhysType"></param>
        /// <returns></returns>
        Expression ImplementGroup(
            ClrAsyncEnumerableRelImplementor implementor,
            ClrAsyncEnumerableResult result,
            Group group,
            int windowIdx,
            ClrPhysType inputPhysType,
            Expression source,
            ClrEnumerablePrefer pref,
            java.util.List translatedConstants,
            List<ParameterExpression> variables,
            List<Expression> body,
            out ClrPhysType outputPhysType)
        {
            var typeFactory = implementor.TypeFactory;
            var translator = implementor.Translator;

            // a partition is an Object[], as Calcite's is, so a row that is a primitive is boxed to go in it —
            // and boxed the way the type factory says, because the comparator ordering the partition is
            // Calcite's and takes a java.lang.Integer where the row type is an int
            // the loop is this convention's, but everything inside it is Calcite's: the aggregate
            // implementors write linq4j into blocks of Calcite's, the frame reads a row through Rex, and the
            // output row is built with record. So the input and the output are each held twice -- ours for the
            // sequence, the comparator and the collation key, all of which are delegates, and Calcite's for
            // every field read that ends up inside one of those blocks.
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);

            var sourceType = inputPhysType.RowType;
            source = source;

            // orders the rows of a partition, and is what EXCLUDE and RANK ask whether two rows are peers
            var comparator_ = J.Expressions.parameter((java.lang.Class)typeof(java.util.Comparator), $"comparator{windowIdx}");
            var comparator = Hoist(translator, variables, body, comparator_,
                inputPhysType.GenerateComparator(group.collation()), typeof(java.util.Comparator));

            var aggs = new java.util.ArrayList();
            var aggregateCalls = group.getAggregateCalls(this);
            for (int aggIdx = 0; aggIdx < aggregateCalls.size(); aggIdx++)
            {
                var call = (AggregateCall)aggregateCalls.get(aggIdx);
                if (call.ignoreNulls())
                    throw new java.lang.UnsupportedOperationException("IGNORE NULLS not supported");

                aggs.add(new AggImpState(aggIdx, call, true));
            }

            // the output of this group is its input plus one field per aggregate
            var typeBuilder = typeFactory.builder();
            typeBuilder.addAll(inputPhysType.RelRowType.getFieldList());
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);

                // [CALCITE-4326] NullPointerException possible in EnumerableWindow when agg.call.name is null
                typeBuilder.add(agg.call.name ?? throw new java.lang.NullPointerException($"agg.call.name for {agg.call}"), agg.call.type);
            }

            outputPhysType = ClrPhysTypeImpl.Of(typeFactory, typeBuilder.build(), pref.Prefer(result.Format));
            var outputCalcite = PhysTypeImpl.of(typeFactory, outputPhysType.RelRowType, outputPhysType.Format, false);

            // a RANGE frame finds its bounds by binary search over the collation key rather than by counting
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

            var loop = new WindowLoop(inputCalcite, inputPhysType.JavaRowType);
            var inputGetter = new WindowRelInputGetter(loop.Row, inputCalcite, inputPhysType.JavaRowType, result.PhysType.RelRowType.getFieldCount(), translatedConstants);

            // the output row is every input field, and then whatever each aggregate last computed
            var inputFieldCount = inputPhysType.RelRowType.getFieldCount();
            var outputRow = new java.util.ArrayList();
            for (int i = 0; i < inputFieldCount; i++)
                outputRow.add(inputCalcite.fieldReference(loop.Row, i, outputCalcite.getJavaFieldType(i)));

            // declareAndResetState: the state each aggregate accumulates in, the variable holding its last
            // result, and the block that gives both their starting values
            var initBlock = new J.BlockBuilder();
            var stateTypes = new java.util.ArrayList();
            var initExpressions = new java.util.ArrayList();
            DeclareAndResetState(typeFactory, result, constants, windowIdx, aggs, outputCalcite, outputRow, group.exclude, initBlock, stateTypes, initExpressions);

            var accPhysType = PhysTypeImplWorkaround.Of(typeFactory, typeFactory.createSyntheticType(stateTypes));
            ClrAsyncEnumerableAggregateBase.DeclareParentAccumulator(initExpressions, initBlock, accPhysType);

            var accType = ClrTypes.Resolve(accPhysType.getJavaRowType());
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            loop.Accumulator = acc_;

            // what were local variables become fields of the accumulator, so the state is a value passed in and
            // out of a lambda rather than something mutated where it was declared
            for (int i = 0, slot = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);

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

            // the frame bounds, each its own block because each is its own lambda
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

            // a bound that is unbounded, or that is the current row, is already inside the partition
            var clampStart = group.lowerBound.isUnbounded() == false && ReferenceEquals(startUnchecked, loop.Index) == false;
            var clampEnd = group.upperBound.isUnbounded() == false && ReferenceEquals(endUnchecked, loop.Index) == false;

            var lowerBound = loop.Lambda(translator, lowerBuilder.toBlock(), typeof(int), null);
            var upperBound = loop.Lambda(translator, upperBuilder.toBlock(), typeof(int), null);

            var frame = new java.util.function.DelegateFunction<J.BlockBuilder, WinAggFrameResultContext>(
                block => new ClrWinAggFrameResultContext(
                    block, typeFactory, implementor.Conformance, inputCalcite, inputPhysType.JavaRowType, result.PhysType.RelRowType.getFieldCount(),
                    translatedConstants, comparator_, loop.Rows, loop.Index, loop.Start, loop.End, min_, loop.MaxX,
                    loop.HasRows, loop.FrameRowCount, loop.PartitionRowCount, loop.Position));

            // reset, add, and the two halves of result, in the order Calcite calls the implementors in: the
            // implementors keep state of their own across the calls
            var resetBuilder = new J.BlockBuilder();
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);
                agg.implementor.implementReset(agg.context,
                    new WinAggResetContextImpl(resetBuilder, agg.state, loop.Index, loop.Start, loop.End, loop.HasRows, loop.FrameRowCount, loop.PartitionRowCount));
            }

            var addBuilder = new J.BlockBuilder();
            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);
                agg.implementor.implementAdd(agg.context,
                    new ClrWinAggAddContext(addBuilder, agg.state, frame, loop.Position, RexArguments(agg, result.PhysType.RelRowType, constants)));
            }

            var cachedBuilder = new J.BlockBuilder();
            var cached = ImplementResult(aggs, cachedBuilder, frame, true, result.PhysType.RelRowType, constants);

            var uncachedBuilder = new J.BlockBuilder();
            var uncached = ImplementResult(aggs, uncachedBuilder, frame, false, result.PhysType.RelRowType, constants);

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

            // the partition key, which is the one thing here that reads a row outside the loop
            var partitionSelector = PartitionSelector(translator, inputCalcite, inputPhysType.JavaRowType, group, sourceType);
            var keyType = partitionSelector?.ReturnType ?? typeof(object);

            return ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Window.MakeGenericMethod(sourceType, keyType, accType, outputType),
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
        /// Evaluates an expression once into a variable of the enclosing block, and binds a linq4j parameter
        /// to it so the blocks Calcite's generators write for this group can name it.
        /// </summary>
        /// <param name="translator"></param>
        /// <param name="variables"></param>
        /// <param name="body"></param>
        /// <param name="parameter"></param>
        /// <param name="expression"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        static ParameterExpression Hoist(LixToClrTranslator translator, List<ParameterExpression> variables, List<Expression> body, J.ParameterExpression parameter, Expression value, Type type)
        {
            // a lambda declared against one of linq4j's functional interfaces is one, and what reads this
            // variable is Calcite's own runtime — BinarySearch takes the key selector as a Function1. A
            // delegate is not one however it is converted, so it is wrapped where it is built.
            if (value is LambdaExpression lambda && AnonymousClasses.Handles(type))
                value = AnonymousClasses.Wrap(type, lambda);

            var variable = Expression.Variable(type, parameter.name);
            variables.Add(variable);
            body.Add(Expression.Assign(variable, ClrEnumUtils.Convert(value, type)));
            translator.Bind(parameter, variable);

            return variable;
        }

        /// <summary>
        /// Returns the lambda giving a row's partition key, or null where the window does not partition.
        /// </summary>
        /// <param name="translator"></param>
        /// <param name="inputPhysType"></param>
        /// <param name="group"></param>
        /// <param name="sourceType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The key <c>getPartitionIterator</c> builds, which is a synthetic record for several keys and the
        /// field itself for one.
        /// </remarks>
        static LambdaExpression? PartitionSelector(LixToClrTranslator translator, PhysType inputPhysType, java.lang.reflect.Type javaRowType, Group group, Type sourceType)
        {
            if (group.keys.isEmpty())
                return null;

            var v_ = J.Expressions.parameter(javaRowType, "v");
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

            // the rows arrive boxed, because the partition they go into is an Object[], so the row is unboxed
            // on the way in exactly as a join's predicate unboxes its two
            var parameter = Expression.Parameter(sourceType, "v");
            var row = Expression.Variable(ClrTypes.Resolve(javaRowType), "v");
            translator.Bind(v_, row);

            // the key is a map's, so a key that is a primitive is boxed the way the type factory says: what
            // hashes it is a java.util.HashMap, and a boxed CLR int is not the same object as a java.lang.Integer
            var keyType = ClrTypes.Resolve(J.Primitive.box(key_.getType()));

            return Expression.Lambda(
                typeof(Func<,>).MakeGenericType(sourceType, keyType),
                Expression.Block(keyType, [row],
                    Expression.Assign(row, ClrEnumUtils.Convert(parameter, row.Type)),
                    translator.TranslateBody(builder.toBlock(), keyType)),
                parameter);
        }

        /// <summary>
        /// Returns a block with a value returned from the end of it.
        /// </summary>
        /// <param name="block"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// The blocks the implementors write have no return of their own, because Calcite drops them into a
        /// method that goes on afterwards. Each is a lambda here, and every one of them yields the accumulator.
        /// </remarks>
        static J.BlockStatement WithReturn(J.BlockStatement block, J.Expression value)
        {
            var statements = new java.util.ArrayList(block.statements);
            statements.add(J.Expressions.return_(null, value));

            return J.Expressions.block(statements);
        }

        /// <summary>
        /// Declares the state each aggregate accumulates in and the variable holding its last result, and
        /// writes the block that gives both their starting values.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="result"></param>
        /// <param name="constants"></param>
        /// <param name="windowIdx"></param>
        /// <param name="aggs"></param>
        /// <param name="outputPhysType"></param>
        /// <param name="outputRow"></param>
        /// <param name="exclusion"></param>
        /// <param name="initBlock"></param>
        /// <param name="stateTypes"></param>
        /// <param name="initExpressions"></param>
        /// <remarks>
        /// <c>EnumerableWindow.declareAndResetState</c>, which is private. The one difference is where the
        /// variables end up: Calcite leaves them as locals of the generated method, and they are collected here
        /// to become the fields of one accumulator.
        /// </remarks>
        static void DeclareAndResetState(
            JavaTypeFactory typeFactory,
            ClrAsyncEnumerableResult result,
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
                var agg = (AggImpState)aggs.get(i);
                agg.context = new ClrWinAggContext(agg, typeFactory, result.PhysType.RelRowType, constants, exclusion);

                var aggName = ClrAsyncEnumerableAggregateBase.AggName(agg);
                var state = agg.implementor.getStateType(agg.context);

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

                agg.implementor.implementReset(agg.context, new WinAggResetContextImpl(initBlock, agg.state, null, null, null, null, null, null));
            }
        }

        /// <summary>
        /// Writes the block computing the results of the aggregates of one half.
        /// </summary>
        /// <param name="aggs"></param>
        /// <param name="builder"></param>
        /// <param name="frame"></param>
        /// <param name="cachedBlock">Whether this is the half computed only when the frame has changed.</param>
        /// <param name="inputRowType"></param>
        /// <param name="constants"></param>
        /// <returns>Whether any aggregate belongs to this half.</returns>
        /// <remarks>
        /// <c>EnumerableWindow.implementResult</c>, which is private. An aggregate whose value does not change
        /// while the frame is intact is computed in the block guarded by that, and read again otherwise.
        /// </remarks>
        static bool ImplementResult(java.util.List aggs, J.BlockBuilder builder, java.util.function.Function frame, bool cachedBlock, RelDataType inputRowType, java.util.List constants)
        {
            var nonEmpty = false;

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);

                var needCache = agg.implementor is not WinAggImplementor implementor || implementor.needCacheWhenFrameIntact();
                if (needCache ^ cachedBlock)
                    continue;

                nonEmpty = true;

                var res = agg.implementor.implementResult(agg.context,
                    new ClrWinAggResultContext(builder, agg.state, frame, RexArguments(agg, inputRowType, constants)));

                // several count(a) and count(b) might share the result
                var aggRes = builder.append($"a{agg.aggIdx}res", EnumUtils.convert(res, agg.result.getType()));
                builder.add(J.Expressions.statement(J.Expressions.assign(agg.result, aggRes)));
            }

            return nonEmpty;
        }

        /// <summary>
        /// Returns the arguments of an aggregate call as row expressions.
        /// </summary>
        /// <param name="agg"></param>
        /// <param name="inputRowType"></param>
        /// <param name="constants"></param>
        /// <returns></returns>
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
        /// Returns the expression giving one bound of the frame.
        /// </summary>
        /// <param name="translator"></param>
        /// <param name="typeFactory"></param>
        /// <param name="i_"></param>
        /// <param name="row_"></param>
        /// <param name="min_"></param>
        /// <param name="max_"></param>
        /// <param name="rows_"></param>
        /// <param name="group"></param>
        /// <param name="lower"></param>
        /// <param name="physType"></param>
        /// <param name="keySelector"></param>
        /// <param name="keyComparator"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableWindow.translateBound</c>, which is private. A ROWS bound counts from the current row;
        /// a RANGE bound is a binary search over the collation key, because the frame is a range of values
        /// rather than of positions.
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

                // a floating offset makes no sense against an array index, and neither does a null
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
        /// The variables the pieces of a window read the loop by, and the lambdas they end up as.
        /// </summary>
        /// <param name="inputPhysType"></param>
        /// <remarks>
        /// Calcite declares these as locals of the generated method, so an implementor's expression can simply
        /// name one. Each is a linq4j parameter here, bound where a block is translated to a variable the
        /// lambda assigns from the <see cref="WindowFrame"/> it is given. That is the same thing a row
        /// parameter is everywhere else in this convention.
        /// </remarks>
        sealed class WindowLoop(PhysType inputPhysType, java.lang.reflect.Type javaRowType)
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
            public J.ParameterExpression Row { get; } = J.Expressions.parameter(javaRowType, "row");

            /// <summary>The accumulator, which is not known until its record type has been built.</summary>
            public J.ParameterExpression? Accumulator { get; set; }

            /// <summary>
            /// Turns a block of Calcite's making into the lambda that runs it.
            /// </summary>
            /// <param name="translator"></param>
            /// <param name="block"></param>
            /// <param name="returnType"></param>
            /// <param name="accumulatorType">The accumulator's type, or null where the lambda does not take one.</param>
            /// <returns></returns>
            public LambdaExpression Lambda(LixToClrTranslator translator, J.BlockStatement block, Type returnType, Type? accumulatorType)
            {
                var frame = Expression.Parameter(typeof(WindowFrame), "frame");
                var rowType = ClrTypes.Resolve(javaRowType);

                // fresh variables each time, because a lambda declares its own and two of them are siblings
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
        /// Reads a field of the row a window aggregate is being evaluated against, or a constant where the
        /// index is past the input's own fields.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="rowPhysType"></param>
        /// <param name="actualInputFieldCount"></param>
        /// <param name="constants"></param>
        /// <remarks>
        /// <c>EnumerableWindow.WindowRelInputGetter</c>, which is private.
        /// </remarks>
        sealed class WindowRelInputGetter(J.Expression row, PhysType rowPhysType, java.lang.reflect.Type javaRowType, int actualInputFieldCount, java.util.List constants) : RexToLixTranslator.InputGetter
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
        /// What a window aggregate implementor is told about the call it is implementing.
        /// </summary>
        /// <param name="agg"></param>
        /// <param name="typeFactory"></param>
        /// <param name="inputRowType"></param>
        /// <param name="constants"></param>
        /// <param name="exclusion"></param>
        /// <remarks>
        /// The anonymous <c>WinAggContext</c> of <c>declareAndResetState</c>. A window has no grouping, so the
        /// four members about one refuse, exactly as Calcite's does.
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

        }

        /// <summary>
        /// What a window aggregate implementor asks about the frame while it writes its result.
        /// </summary>
        /// <remarks>
        /// The anonymous <c>WinAggFrameResultContext</c> of
        /// <c>getBlockBuilderWinAggFrameResultContextFunction</c>. One is made per block the implementors write
        /// into, which is why Calcite passes a function rather than an instance.
        /// </remarks>
        sealed class ClrWinAggFrameResultContext(
            J.BlockBuilder block,
            JavaTypeFactory typeFactory,
            SqlConformance conformance,
            PhysType inputPhysType,
            java.lang.reflect.Type javaRowType,
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
                    new WindowRelInputGetter(GetRow(rowIndex), inputPhysType, javaRowType, actualInputFieldCount, constants),
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
            /// Returns the expression reading one row of the partition.
            /// </summary>
            /// <param name="rowIndex"></param>
            /// <returns></returns>
            J.Expression GetRow(J.Expression rowIndex)
            {
                // EnumUtils.convert declines a row of a CLR class, and quietly: Types.needTypeCast answers
                // false for any RecordType, so the operand comes back as it went in and the row stays an
                // Object -- which then reaches CUSTOM.field and asks Object.class for its fields. The rule is
                // right where a record type is the declared type of a variable, a synthetic record carrying
                // its own and a cast being noise; it is wrong here, where the value is an element of an
                // Object[] and there is nothing else to say what it is. Calcite never meets it, a row of a
                // class handing it a Class and needTypeCast then answering true.
                //
                // So the cast is written for our record type and for nothing else. A synthetic record still
                // goes through EnumUtils and still gets no cast, which is Calcite's behaviour whether or not
                // it is Calcite's intent.
                var element = (J.Expression)J.Expressions.arrayIndex(rows, rowIndex);
                return block.append("jRow", javaRowType is Clr.ClrRecordType
                    ? J.Expressions.convert_(element, javaRowType)
                    : EnumUtils.convert(element, javaRowType));
            }

            /// <summary>
            /// Returns the expression testing that an index is between two others.
            /// </summary>
            /// <param name="rowIndex"></param>
            /// <param name="minIndex"></param>
            /// <param name="maxIndex"></param>
            /// <returns></returns>
            J.Expression CheckBounds(J.Expression rowIndex, J.Expression minIndex, J.Expression maxIndex)
            {
                // an index that is one of the loop's own is already known to be inside
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
        /// What a window aggregate implementor is given to fold one row in with.
        /// </summary>
        /// <param name="block"></param>
        /// <param name="accumulator"></param>
        /// <param name="frame"></param>
        /// <param name="position"></param>
        /// <param name="rexArgs"></param>
        sealed class ClrWinAggAddContext(J.BlockBuilder block, java.util.List accumulator, java.util.function.Function frame, J.ParameterExpression position, java.util.List rexArgs) :
            WinAggAddContextImpl(block, accumulator, frame)
        {

            /// <inheritdoc />
            public override J.Expression currentPosition() => position;

            /// <inheritdoc />
            public override java.util.List rexArguments() => rexArgs;

            /// <inheritdoc />
            public override RexNode? rexFilterArgument() => null;

        }

        /// <summary>
        /// What a window aggregate implementor is given to write its result with.
        /// </summary>
        /// <param name="block"></param>
        /// <param name="accumulator"></param>
        /// <param name="frame"></param>
        /// <param name="rexArgs"></param>
        sealed class ClrWinAggResultContext(J.BlockBuilder block, java.util.List accumulator, java.util.function.Function frame, java.util.List rexArgs) :
            WinAggResultContextImpl(block, accumulator, frame)
        {

            /// <inheritdoc />
            public override java.util.List rexArguments() => rexArgs;

        }

    }

}
