using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.enumerable.impl;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Aggregate"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableAggregate</c>; what it shares with a sorted aggregate is on
    /// <see cref="ClrCursorAggregateBase"/>, as Calcite splits it. The accumulation is written by Calcite's
    /// aggregate implementors in linq4j; each block they produce is translated where it is produced, and the
    /// resulting lambdas are handed to Calcite's <c>AggregateLambdaFactory</c>.
    ///
    /// <para>The input is drained, and the groups accumulated, when the node's cursor is opened, as linq4j's
    /// <c>groupBy</c>, <c>distinct</c> and <c>aggregate</c> do. The awaiting body awaits the same work inside its
    /// open.</para>
    /// </remarks>
    public class ClrCursorAggregate : ClrCursorAggregateBase, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="groupSet">The fields to group by.</param>
        /// <param name="groupSets">The grouping sets, or null for the group set alone.</param>
        /// <param name="aggCalls">The aggregate calls.</param>
        /// <exception cref="InvalidRelException">
        /// A call is <c>DISTINCT</c> or <c>WITHIN DISTINCT</c>, which this node does not implement.
        /// </exception>
        public ClrCursorAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, groupSet, groupSets, aggCalls)
        {
            for (int i = 0; i < aggCalls.size(); i++)
            {
                var call = (AggregateCall)aggCalls.get(i);

                if (call.isDistinct())
                    throw new InvalidRelException("distinct aggregation not supported");
                if (call.distinctKeys != null)
                    throw new InvalidRelException("within-distinct aggregation not supported");

                // whether the function has an implementor is checked by ClrCursorAggregateRule against the
                // cluster's implementor table, which is the one the node is implemented against
            }
        }

        /// <inheritdoc />
        public override Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls)
        {
            return new ClrCursorAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = inputPhysType.RowType;
            var rowType = physType.RowType;

            // the accumulator, the group key and the output row are written by Calcite's aggregate implementors
            // into linq4j blocks, so they take Calcite's physical types
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var keyPhysType = inputCalcite.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);
            // the key in both conventions: Calcite's for AggResultContextImpl, through which the implementors read
            // it, and ours for the key selector and comparer, which are expression trees
            var keyClr = ClrPhysTypeImpl.Of(typeFactory, keyPhysType.getRowType(), keyPhysType.getFormat(), false);

            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new ClrAggImpState(i, (AggregateCall)getAggCallList().get(i), false, RexImplementorTables.of(getCluster())));

            // the accumulator's state, and the block that sets it to its starting value
            var initExpressions = new java.util.ArrayList();
            var initBlock = new J.BlockBuilder();
            var aggStateTypes = CreateAggStateTypes(initExpressions, initBlock, aggs, typeFactory, getInput().getRowType(), groupSet, getGroupSets());

            var accPhysType = PhysTypeImplWorkaround.Of(typeFactory, typeFactory.createSyntheticType(aggStateTypes));
            DeclareParentAccumulator(initExpressions, initBlock, accPhysType);

            var accType = ClrTypes.Resolve(accPhysType.getJavaRowType());
            var accumulatorInitializer = Function0Of(
                Expression.Lambda(
                    typeof(Func<>).MakeGenericType(accType),
                    implementor.Translator.TranslateBody(initBlock.toBlock(), accType)),
                accType);

            var in_ = J.Expressions.parameter(inputCalcite.getJavaRowType(), "in");
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            var inParameter = Expression.Parameter(sourceType, "in");
            var accParameter = Expression.Parameter(accType, "acc");
            implementor.Translator.Bind(in_, inParameter);
            implementor.Translator.Bind(acc_, accParameter);

            var adders = CreateAccumulatorAdders(implementor, in_, inParameter, aggs, accPhysType, acc_, accParameter, inputCalcite, typeFactory, accType, sourceType);
            var lambdaFactory = ImplementLambdaFactory(implementor, inputPhysType, aggs, adders, accumulatorInitializer, HasOrderedCall(aggs), sourceType);

            // the block that turns a key and a finished accumulator into an output row
            var resultBlock = new J.BlockBuilder();
            var results = new java.util.ArrayList();

            J.ParameterExpression? key_ = null;
            ParameterExpression? keyParameter = null;
            if (groupCount > 0)
            {
                key_ = J.Expressions.parameter(keyPhysType.getJavaRowType(), "key");
                keyParameter = Expression.Parameter(ClrTypes.Resolve(keyPhysType.getJavaRowType()), "key");
                implementor.Translator.Bind(key_, keyParameter);

                for (int j = 0; j < groupCount; j++)
                {
                    var reference = keyPhysType.fieldReference(key_, j);

                    if (getGroupType() == Group.SIMPLE)
                    {
                        results.add(reference);
                        continue;
                    }

                    // a grouping-set key carries an indicator per field, set where the current set does not group
                    // by that field; the output value is then null
                    results.add(
                        J.Expressions.condition(
                            keyPhysType.fieldReference(key_, groupCount + j),
                            J.Expressions.constant(null),
                            J.Expressions.box(reference)));
                }
            }

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                results.add(agg.Implementor.implementResult(agg.context, new AggResultContextImpl(resultBlock, agg.call, agg.state, key_, keyPhysType)));
            }

            resultBlock.add(J.Expressions.return_(null, outputCalcite.record(results)));

            if (getGroupType() != Group.SIMPLE)
            {
                // one key selector per grouping set, keying on the fields that set groups by and marking the
                // rest; each row is folded into one group per selector, so ROLLUP and CUBE read the input once
                var sets = getGroupSets();
                var selectors = new Expression[sets.size()];
                Type? keyType = null;

                for (int i = 0; i < sets.size(); i++)
                {
                    var set = (ImmutableBitSet)sets.get(i);
                    var selector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), set.asList(), keyClr.Format);

                    keyType ??= selector.ReturnType;
                    if (selector.ReturnType != keyType)
                        throw new java.lang.IllegalStateException($"grouping set key types differ: {selector.ReturnType} against {keyType}");

                    selectors[i] = selector;
                }

                var setsResultSelector = Expression.Lambda(
                    typeof(Func<,,>).MakeGenericType(keyParameter!.Type, accType, rowType),
                    implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                    keyParameter,
                    accParameter);

                return implementor.Result(physType,
                    Expression.Call(null,
                        ClrCursorBuiltInMethod.GroupByMultiple.MakeGenericMethod(sourceType, keyType!, rowType),
                        result.Expression,
                        Expression.NewArrayInit(typeof(Func<,>).MakeGenericType(sourceType, keyType!), selectors),
                        Expression.Call(lambdaFactory, AccInitializer),
                        Expression.Call(lambdaFactory, AccAdder),
                        Expression.Call(lambdaFactory, ResultSelector, Function2Of(setsResultSelector, keyParameter.Type, accType, rowType)),
                        keyClr.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
            }

            if (groupCount == 0)
            {
                var resultSelector = Expression.Lambda(
                    typeof(Func<,>).MakeGenericType(accType, rowType),
                    implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                    accParameter);

                return implementor.Result(physType,
                    Expression.Call(null,
                        ClrCursorBuiltInMethod.Singleton.MakeGenericMethod(rowType),
                        Expression.Call(null,
                            ClrCursorBuiltInMethod.Aggregate.MakeGenericMethod(sourceType, rowType),
                            result.Expression,
                            Expression.Call(Expression.Call(lambdaFactory, AccInitializer), Function0Apply),
                            Expression.Call(lambdaFactory, AccAdder),
                            Expression.Call(lambdaFactory, SingleGroupResultSelector, Function1Of(resultSelector, accType, rowType)))));
            }

            // grouping by every input field with no aggregate calls is a DISTINCT, which Calcite implements as
            // one: the input rows, converted to the output format, deduplicated
            if (getAggCallList().isEmpty() && groupSet.equals(ImmutableBitSet.range(getInput().getRowType().getFieldCount())))
            {
                var source = inputPhysType.ConvertTo(result.Expression, physType.Format);

                return implementor.Result(physType,
                    Expression.Call(null,
                        ClrCursorBuiltInMethod.Distinct.MakeGenericMethod(source.Type.GetGenericArguments()[0]),
                        source,
                        physType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
            }

            var keySelector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), keyClr.Format);

            var groupResultSelector = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(keyParameter!.Type, accType, rowType),
                implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                keyParameter,
                accParameter);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.GroupBy.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    keyClr.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = inputPhysType.RowType;
            var rowType = physType.RowType;

            // the accumulator, the group key and the output row are written by Calcite's aggregate implementors
            // into linq4j blocks, so they take Calcite's physical types
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var keyPhysType = inputCalcite.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);
            // the key in both conventions: Calcite's for AggResultContextImpl, through which the implementors read
            // it, and ours for the key selector and comparer, which are expression trees
            var keyClr = ClrPhysTypeImpl.Of(typeFactory, keyPhysType.getRowType(), keyPhysType.getFormat(), false);

            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new ClrAggImpState(i, (AggregateCall)getAggCallList().get(i), false, RexImplementorTables.of(getCluster())));

            // the accumulator's state, and the block that sets it to its starting value
            var initExpressions = new java.util.ArrayList();
            var initBlock = new J.BlockBuilder();
            var aggStateTypes = CreateAggStateTypes(initExpressions, initBlock, aggs, typeFactory, getInput().getRowType(), groupSet, getGroupSets());

            var accPhysType = PhysTypeImplWorkaround.Of(typeFactory, typeFactory.createSyntheticType(aggStateTypes));
            DeclareParentAccumulator(initExpressions, initBlock, accPhysType);

            var accType = ClrTypes.Resolve(accPhysType.getJavaRowType());
            var accumulatorInitializer = Function0Of(
                Expression.Lambda(
                    typeof(Func<>).MakeGenericType(accType),
                    implementor.Translator.TranslateBody(initBlock.toBlock(), accType)),
                accType);

            var in_ = J.Expressions.parameter(inputCalcite.getJavaRowType(), "in");
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            var inParameter = Expression.Parameter(sourceType, "in");
            var accParameter = Expression.Parameter(accType, "acc");
            implementor.Translator.Bind(in_, inParameter);
            implementor.Translator.Bind(acc_, accParameter);

            var adders = CreateAccumulatorAdders(implementor, in_, inParameter, aggs, accPhysType, acc_, accParameter, inputCalcite, typeFactory, accType, sourceType);
            var lambdaFactory = ImplementLambdaFactory(implementor, inputPhysType, aggs, adders, accumulatorInitializer, HasOrderedCall(aggs), sourceType);

            // the block that turns a key and a finished accumulator into an output row
            var resultBlock = new J.BlockBuilder();
            var results = new java.util.ArrayList();

            J.ParameterExpression? key_ = null;
            ParameterExpression? keyParameter = null;
            if (groupCount > 0)
            {
                key_ = J.Expressions.parameter(keyPhysType.getJavaRowType(), "key");
                keyParameter = Expression.Parameter(ClrTypes.Resolve(keyPhysType.getJavaRowType()), "key");
                implementor.Translator.Bind(key_, keyParameter);

                for (int j = 0; j < groupCount; j++)
                {
                    var reference = keyPhysType.fieldReference(key_, j);

                    if (getGroupType() == Group.SIMPLE)
                    {
                        results.add(reference);
                        continue;
                    }

                    // a grouping-set key carries an indicator per field, set where the current set does not group
                    // by that field; the output value is then null
                    results.add(
                        J.Expressions.condition(
                            keyPhysType.fieldReference(key_, groupCount + j),
                            J.Expressions.constant(null),
                            J.Expressions.box(reference)));
                }
            }

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                results.add(agg.Implementor.implementResult(agg.context, new AggResultContextImpl(resultBlock, agg.call, agg.state, key_, keyPhysType)));
            }

            resultBlock.add(J.Expressions.return_(null, outputCalcite.record(results)));

            if (getGroupType() != Group.SIMPLE)
            {
                // one key selector per grouping set, keying on the fields that set groups by and marking the
                // rest; each row is folded into one group per selector, so ROLLUP and CUBE read the input once
                var sets = getGroupSets();
                var selectors = new Expression[sets.size()];
                Type? keyType = null;

                for (int i = 0; i < sets.size(); i++)
                {
                    var set = (ImmutableBitSet)sets.get(i);
                    var selector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), set.asList(), keyClr.Format);

                    keyType ??= selector.ReturnType;
                    if (selector.ReturnType != keyType)
                        throw new java.lang.IllegalStateException($"grouping set key types differ: {selector.ReturnType} against {keyType}");

                    selectors[i] = selector;
                }

                var setsResultSelector = Expression.Lambda(
                    typeof(Func<,,>).MakeGenericType(keyParameter!.Type, accType, rowType),
                    implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                    keyParameter,
                    accParameter);

                return implementor.ResultAsync(physType,
                    ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.GroupByMultipleAsync.MakeGenericMethod(sourceType, keyType!, rowType),
                        result.Expression,
                        Expression.NewArrayInit(typeof(Func<,>).MakeGenericType(sourceType, keyType!), selectors),
                        Expression.Call(lambdaFactory, AccInitializer),
                        Expression.Call(lambdaFactory, AccAdder),
                        Expression.Call(lambdaFactory, ResultSelector, Function2Of(setsResultSelector, keyParameter.Type, accType, rowType)),
                        keyClr.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
            }

            if (groupCount == 0)
            {
                var resultSelector = Expression.Lambda(
                    typeof(Func<,>).MakeGenericType(accType, rowType),
                    implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                    accParameter);

                // one operator where the synchronous body nests Aggregate inside Singleton: the fold has to be
                // awaited, which an expression tree cannot do. It still folds once, at the open
                return implementor.ResultAsync(physType,
                    ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SingletonAggregateAsync.MakeGenericMethod(sourceType, rowType),
                        result.Expression,
                        Expression.Call(Expression.Call(lambdaFactory, AccInitializer), Function0Apply),
                        Expression.Call(lambdaFactory, AccAdder),
                        Expression.Call(lambdaFactory, SingleGroupResultSelector, Function1Of(resultSelector, accType, rowType))));
            }

            // grouping by every input field with no aggregate calls is a DISTINCT, which Calcite implements as
            // one: the input rows, converted to the output format, deduplicated
            if (getAggCallList().isEmpty() && groupSet.equals(ImmutableBitSet.range(getInput().getRowType().getFieldCount())))
            {
                var source = inputPhysType.ConvertToAsync(implementor, result.Expression, physType.Format);

                // an awaiting open is a ValueTask of the cursor, so the row type is one generic level deeper
                return implementor.ResultAsync(physType,
                    ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.DistinctAsync.MakeGenericMethod(source.Type.GetGenericArguments()[0].GetGenericArguments()[0]),
                        source,
                        physType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
            }

            var keySelector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), keyClr.Format);

            var groupResultSelector = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(keyParameter!.Type, accType, rowType),
                implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                keyParameter,
                accParameter);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.GroupByAsync.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    keyClr.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
        }
    }

}
