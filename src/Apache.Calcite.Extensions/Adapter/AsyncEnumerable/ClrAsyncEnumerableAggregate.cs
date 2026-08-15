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

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Aggregate"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The aggregate implementors are Calcite's and work in linq4j, so each lambda they build is translated
    /// where it is produced and then handed back to <c>AggregateLambdaFactory</c>, which is also Calcite's and
    /// takes Calcite's functional interfaces. Nothing about how an aggregate accumulates is decided here.
    ///
    /// <para>What this class holds is what <c>EnumerableAggregate</c> holds; everything shared with a windowed
    /// aggregate is on <see cref="ClrAsyncEnumerableAggregateBase"/>, exactly as Calcite splits the two.</para>
    /// </remarks>
    public class ClrAsyncEnumerableAggregate : ClrAsyncEnumerableAggregateBase, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="groupSet"></param>
        /// <param name="groupSets"></param>
        /// <param name="aggCalls"></param>
        /// <exception cref="InvalidRelException"></exception>
        public ClrAsyncEnumerableAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, groupSet, groupSets, aggCalls)
        {
            for (int i = 0; i < aggCalls.size(); i++)
            {
                var call = (AggregateCall)aggCalls.get(i);

                if (call.isDistinct())
                    throw new InvalidRelException("distinct aggregation not supported");
                if (call.distinctKeys != null)
                    throw new InvalidRelException("within-distinct aggregation not supported");
                if (RexImpTable.INSTANCE.get(call.getAggregation(), false) == null)
                    throw new InvalidRelException($"aggregation {call.getAggregation()} not supported");
            }
        }

        /// <inheritdoc />
        public override Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls)
        {
            return new ClrAsyncEnumerableAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrAsyncEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = inputPhysType.RowType;
            var rowType = physType.RowType;

            // the accumulator, the group key and the output row are all written by Calcite's aggregate
            // implementors, into blocks of Calcite's, so each of those takes a physical type of Calcite's.
            // Only the sorter's key selector below is a delegate, and only that one is ours.
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var keyPhysType = inputCalcite.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);
            // the same key twice over: Calcite's, because its aggregate implementors read the key through
            // AggResultContextImpl, and ours, because the selector and the comparer are delegates
            var keyClr = ClrPhysTypeImpl.Of(typeFactory, keyPhysType.getRowType(), keyPhysType.getFormat(), false);

            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new ClrAggImpState(i, (AggregateCall)getAggCallList().get(i), false));

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

                    // a key of a grouping set carries an indicator per field, set where the field is not one
                    // this set groups by; the value is then null however the row read
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
                // one key selector per grouping set, each keying on the fields that set groups by and marking
                // the rest; every row is folded into one group per selector, which is what makes a ROLLUP or a
                // CUBE one pass over the input
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
                    ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.GroupByMultiple.MakeGenericMethod(sourceType, keyType!, rowType),
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

                // one operator where the synchronous convention nests two. There the fold returns the row
                // and Singleton wraps it; here the fold has to be awaited and an expression tree cannot
                // await, so the composition is an operator rather than a tree. See
                // ClrAsyncEnumerableDefaults.SingletonAggregate.
                return implementor.Result(physType,
                    ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.SingletonAggregate.MakeGenericMethod(sourceType, rowType),
                        result.Expression,
                        Expression.Call(Expression.Call(lambdaFactory, AccInitializer), Function0Apply),
                        Expression.Call(lambdaFactory, AccAdder),
                        Expression.Call(lambdaFactory, SingleGroupResultSelector, Function1Of(resultSelector, accType, rowType))));
            }

            // grouping by every field of the input with nothing to accumulate is a DISTINCT, and Calcite says
            // so rather than grouping: the rows are the input's, so the accumulator machinery is skipped and
            // the physical type is reached by conversion
            if (getAggCallList().isEmpty() && groupSet.equals(ImmutableBitSet.range(getInput().getRowType().getFieldCount())))
            {
                var source = inputPhysType.ConvertToAsync(result.Expression, physType.Format);

                return implementor.Result(physType,
                    ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Distinct.MakeGenericMethod(source.Type.GetGenericArguments()[0]),
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
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.GroupBy.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    keyClr.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer))));
        }
    }

}
