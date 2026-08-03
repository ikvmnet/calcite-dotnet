using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.enumerable.impl;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Aggregate"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The aggregate implementors are Calcite's and work in linq4j, so each lambda they build is translated
    /// where it is produced and then handed back to <c>AggregateLambdaFactory</c>, which is also Calcite's and
    /// takes Calcite's functional interfaces. Nothing about how an aggregate accumulates is decided here.
    ///
    /// <para><c>EnumerableAggregateBase</c> keeps <c>createAggStateTypes</c>, <c>declareParentAccumulator</c>,
    /// <c>createAccumulatorAdders</c> and <c>implementLambdaFactory</c> protected, so like
    /// <c>EnumUtils.joinSelector</c> they are ported rather than reused.</para>
    /// </remarks>
    public class ClrEnumerableAggregate : Aggregate, ClrEnumerableRel
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
        public ClrEnumerableAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
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
            return new ClrEnumerableAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            var physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.preferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = TypeResolver.Resolve(inputPhysType.getJavaRowType());
            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            var keyPhysType = inputPhysType.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);
            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new AggImpState(i, (AggregateCall)getAggCallList().get(i), false));

            // the accumulator's state, and the block that sets it to its starting value
            var initExpressions = new java.util.ArrayList();
            var initBlock = new J.BlockBuilder();
            var aggStateTypes = CreateAggStateTypes(initExpressions, initBlock, aggs, typeFactory, getInput().getRowType(), groupSet, getGroupSets());

            var accPhysType = ClrEnumUtils.AccumulatorPhysType(typeFactory, typeFactory.createSyntheticType(aggStateTypes));
            DeclareParentAccumulator(initExpressions, initBlock, accPhysType);

            var accType = TypeResolver.Resolve(accPhysType.getJavaRowType());
            var accumulatorInitializer = Function0Of(
                Expression.Lambda(
                    typeof(Func<>).MakeGenericType(accType),
                    implementor.Translator.TranslateBody(initBlock.toBlock(), accType)),
                accType);

            var in_ = J.Expressions.parameter(inputPhysType.getJavaRowType(), "in");
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            var inParameter = Expression.Parameter(sourceType, "in");
            var accParameter = Expression.Parameter(accType, "acc");
            implementor.Translator.Bind(in_, inParameter);
            implementor.Translator.Bind(acc_, accParameter);

            var adders = CreateAccumulatorAdders(implementor, in_, inParameter, aggs, accPhysType, acc_, accParameter, inputPhysType, typeFactory, accType, sourceType);
            var lambdaFactory = LambdaFactory(accumulatorInitializer, adders);

            // the block that turns a key and a finished accumulator into an output row
            var resultBlock = new J.BlockBuilder();
            var results = new java.util.ArrayList();

            J.ParameterExpression? key_ = null;
            ParameterExpression? keyParameter = null;
            if (groupCount > 0)
            {
                key_ = J.Expressions.parameter(keyPhysType.getJavaRowType(), "key");
                keyParameter = Expression.Parameter(TypeResolver.Resolve(keyPhysType.getJavaRowType()), "key");
                implementor.Translator.Bind(key_, keyParameter);

                for (int j = 0; j < groupCount; j++)
                    results.add(keyPhysType.fieldReference(key_, j));
            }

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);
                results.add(agg.implementor.implementResult(agg.context, new AggResultContextImpl(resultBlock, agg.call, agg.state, key_, keyPhysType)));
            }

            resultBlock.add(J.Expressions.return_(null, physType.record(results)));

            if (groupCount == 0)
            {
                var resultSelector = Expression.Lambda(
                    typeof(Func<,>).MakeGenericType(accType, rowType),
                    implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                    accParameter);

                return implementor.Result(physType,
                    Expression.Call(null,
                        ClrBuiltInMethod.Singleton.MakeGenericMethod(rowType),
                        Expression.Call(null,
                            ClrBuiltInMethod.Aggregate.MakeGenericMethod(sourceType, rowType),
                            result.Expression,
                            Expression.Call(Expression.Call(lambdaFactory, AccInitializer), Function0Apply),
                            Expression.Call(lambdaFactory, AccAdder),
                            Expression.Call(lambdaFactory, SingleGroupResultSelector, Function1Of(resultSelector, accType, rowType)))));
            }

            var keySelector = implementor.Translator.TranslateSelector(
                inputPhysType.generateSelector(in_, groupSet.asList(), keyPhysType.getFormat()),
                sourceType);

            var groupResultSelector = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(keyParameter!.Type, accType, rowType),
                implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                keyParameter,
                accParameter);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.GroupBy.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    ClrPhysTypes.Comparer(implementor, keyPhysType)));
        }

        /// <summary>
        /// Declares the state each aggregate accumulates in, and the block that resets it.
        /// </summary>
        /// <param name="initExpressions"></param>
        /// <param name="initBlock"></param>
        /// <param name="aggs"></param>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        static java.util.List CreateAggStateTypes(java.util.List initExpressions, J.BlockBuilder initBlock, java.util.List aggs, JavaTypeFactory typeFactory, RelDataType inputRowType, ImmutableBitSet groupSet, java.util.List groupSets)
        {
            var aggStateTypes = new java.util.ArrayList();

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);
                agg.context = new ClrAggContext(agg, typeFactory, inputRowType, groupSet, groupSets);

                var state = agg.implementor.getStateType(agg.context);
                if (state.isEmpty())
                {
                    agg.state = com.google.common.collect.ImmutableList.of();
                    continue;
                }

                aggStateTypes.addAll(state);

                var decls = new java.util.ArrayList(state.size());
                for (int j = 0; j < state.size(); j++)
                {
                    var pe = J.Expressions.parameter((java.lang.reflect.Type)state.get(j), initBlock.newName($"a{agg.aggIdx}s{j}"));
                    initBlock.add(J.Expressions.declare(0, pe, null));
                    decls.add(pe);
                }

                agg.state = decls;
                initExpressions.addAll(decls);
                agg.implementor.implementReset(agg.context, new AggResultContextImpl(initBlock, agg.call, decls, null, null));
            }

            return aggStateTypes;
        }

        /// <summary>
        /// Adds the statement that builds the accumulator from its parts.
        /// </summary>
        /// <param name="initExpressions"></param>
        /// <param name="initBlock"></param>
        /// <param name="accPhysType"></param>
        static void DeclareParentAccumulator(java.util.List initExpressions, J.BlockBuilder initBlock, PhysType accPhysType)
        {
            if (accPhysType.getJavaRowType() is org.apache.calcite.jdbc.JavaTypeFactoryImpl.SyntheticRecordType synType)
            {
                // built a field at a time rather than through a constructor, which is what Calcite settled on
                // under CALCITE-1097 when a record of many fields exceeded what Janino would take
                var record0_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "record0");
                initBlock.add(J.Expressions.declare(0, record0_, null));
                initBlock.add(J.Expressions.statement(J.Expressions.assign(record0_, J.Expressions.new_(accPhysType.getJavaRowType()))));

                var fieldList = synType.getRecordFields();
                for (int i = 0; i < initExpressions.size(); i++)
                    initBlock.add(J.Expressions.statement(J.Expressions.assign(J.Expressions.field(record0_, (J.Types.RecordField)fieldList.get(i)), (J.Expression)initExpressions.get(i))));

                initBlock.add(J.Expressions.return_(null, record0_));
                return;
            }

            initBlock.add(J.Expressions.return_(null, accPhysType.record(initExpressions)));
        }

        /// <summary>
        /// Builds the lambda that folds one row into the accumulator, for each aggregate.
        /// </summary>
        static java.util.List CreateAccumulatorAdders(
            ClrEnumerableRelImplementor implementor,
            J.ParameterExpression in_,
            ParameterExpression inParameter,
            java.util.List aggs,
            PhysType accPhysType,
            J.ParameterExpression acc_,
            ParameterExpression accParameter,
            PhysType inputPhysType,
            JavaTypeFactory typeFactory,
            Type accType,
            Type sourceType)
        {
            var adders = new java.util.ArrayList();

            for (int i = 0, stateOffset = 0; i < aggs.size(); i++)
            {
                var builder = new J.BlockBuilder();
                var agg = (AggImpState)aggs.get(i);

                var stateSize = agg.state.size();
                var accumulator = new java.util.ArrayList(stateSize);
                for (int j = 0; j < stateSize; j++)
                    accumulator.add(accPhysType.fieldReference(acc_, j + stateOffset));

                agg.state = accumulator;
                stateOffset += stateSize;

                agg.implementor.implementAdd(agg.context, new ClrAggAddContext(builder, accumulator, agg, inputPhysType, in_, typeFactory, implementor.Conformance));
                builder.add(J.Expressions.return_(null, acc_));

                adders.add(
                    Function2Of(
                        Expression.Lambda(
                            typeof(Func<,,>).MakeGenericType(accType, sourceType, accType),
                            implementor.Translator.TranslateBody(builder.toBlock(), accType),
                            accParameter,
                            inParameter),
                        accType,
                        sourceType,
                        accType));
            }

            return adders;
        }

        /// <summary>
        /// Builds the factory the three lambdas an aggregate runs on are taken from.
        /// </summary>
        /// <param name="accumulatorInitializer"></param>
        /// <param name="adders"></param>
        /// <returns></returns>
        /// <remarks>
        /// Only the unordered factory is built. An aggregate call carrying its own ordering needs
        /// <c>LazyAggregateLambdaFactory</c> and a <c>SourceSorter</c> per call, which is not here yet.
        /// </remarks>
        static Expression LambdaFactory(Expression accumulatorInitializer, java.util.List adders)
        {
            var list = Expression.Variable(typeof(java.util.List), "accumulatorAdders");
            var body = new System.Collections.Generic.List<Expression>
            {
                Expression.Assign(list, Expression.New(typeof(java.util.LinkedList).GetConstructor([])!)),
            };

            for (int i = 0; i < adders.size(); i++)
                body.Add(Expression.Call(list, CollectionAdd, Expression.Convert((Expression)adders.get(i), typeof(object))));

            body.Add(Expression.New(BasicFactory, accumulatorInitializer, list));

            return Expression.Block(typeof(AggregateLambdaFactory), [list], body);
        }

        /// <summary>
        /// Wraps a lambda as a linq4j <see cref="Function0"/>.
        /// </summary>
        static Expression Function0Of(LambdaExpression lambda, Type result)
        {
            return Expression.New(typeof(DelegateFunction0<>).MakeGenericType(result).GetConstructors()[0], lambda);
        }

        /// <summary>
        /// Wraps a lambda as a linq4j <see cref="Function1"/>.
        /// </summary>
        static Expression Function1Of(LambdaExpression lambda, Type arg0, Type result)
        {
            return Expression.New(typeof(DelegateFunction1Of<,>).MakeGenericType(arg0, result).GetConstructors()[0], lambda);
        }

        /// <summary>
        /// Wraps a lambda as a linq4j <see cref="Function2"/>.
        /// </summary>
        static Expression Function2Of(LambdaExpression lambda, Type arg0, Type arg1, Type result)
        {
            return Expression.New(typeof(DelegateFunction2<,,>).MakeGenericType(arg0, arg1, result).GetConstructors()[0], lambda);
        }

        static readonly System.Reflection.ConstructorInfo BasicFactory = typeof(BasicAggregateLambdaFactory).GetConstructors()[0];
        static readonly System.Reflection.MethodInfo CollectionAdd = typeof(java.util.List).GetMethod("add", [typeof(object)])!;
        static readonly System.Reflection.MethodInfo AccInitializer = typeof(AggregateLambdaFactory).GetMethod("accumulatorInitializer")!;
        static readonly System.Reflection.MethodInfo AccAdder = typeof(AggregateLambdaFactory).GetMethod("accumulatorAdder")!;
        static readonly System.Reflection.MethodInfo ResultSelector = typeof(AggregateLambdaFactory).GetMethod("resultSelector")!;
        static readonly System.Reflection.MethodInfo SingleGroupResultSelector = typeof(AggregateLambdaFactory).GetMethod("singleGroupResultSelector")!;
        static readonly System.Reflection.MethodInfo Function0Apply = typeof(Function0).GetMethod("apply")!;

        /// <summary>
        /// What an aggregate implementor is told about the call it is implementing.
        /// </summary>
        /// <param name="agg"></param>
        /// <param name="typeFactory"></param>
        sealed class ClrAggContext(AggImpState agg, JavaTypeFactory typeFactory, RelDataType inputRowType, ImmutableBitSet groupSet, java.util.List sets) : AggContext
        {

            /// <inheritdoc />
            public SqlAggFunction aggregation() => agg.call.getAggregation();

            /// <inheritdoc />
            public RelDataType returnRelType() => agg.call.type;

            /// <inheritdoc />
            public java.lang.reflect.Type returnType() => ClrEnumUtils.JavaClass(typeFactory, returnRelType());

            /// <inheritdoc />
            public java.util.List parameterRelTypes() => ClrEnumUtils.FieldRowTypes(inputRowType, agg.call.getArgList());

            /// <inheritdoc />
            public java.util.List parameterTypes() => ClrEnumUtils.FieldTypes(typeFactory, parameterRelTypes());

            /// <inheritdoc />
            public java.util.List groupSets() => sets;

            /// <inheritdoc />
            public java.util.List keyOrdinals() => groupSet.asList();

            /// <inheritdoc />
            public java.util.List keyRelTypes() => ClrEnumUtils.FieldRowTypes(inputRowType, groupSet.asList());

            /// <inheritdoc />
            public java.util.List keyTypes() => ClrEnumUtils.FieldTypes(typeFactory, keyRelTypes());

        }

        /// <summary>
        /// What an aggregate implementor is given to fold one row in with.
        /// </summary>
        sealed class ClrAggAddContext(J.BlockBuilder block, java.util.List accumulator, AggImpState agg, PhysType inputPhysType, J.ParameterExpression in_, JavaTypeFactory typeFactory, org.apache.calcite.sql.validate.SqlConformance conformance) :
            AggAddContextImpl(block, accumulator)
        {

            /// <inheritdoc />
            public override java.util.List rexArguments()
            {
                var inputTypes = inputPhysType.getRowType().getFieldList();
                var args = new java.util.ArrayList();

                for (int i = 0; i < agg.call.getArgList().size(); i++)
                    args.add(RexInputRef.of(((java.lang.Integer)agg.call.getArgList().get(i)).intValue(), inputTypes));

                return args;
            }

            /// <inheritdoc />
            public override RexNode rexFilterArgument()
            {
                return agg.call.filterArg < 0 ? null! : RexInputRef.of(agg.call.filterArg, inputPhysType.getRowType());
            }

            /// <inheritdoc />
            public override RexToLixTranslator rowTranslator()
            {
                return RexToLixTranslator.forAggregation(typeFactory, currentBlock(), new RexToLixTranslator.InputGetterImpl(in_, inputPhysType), conformance);
            }

        }

    }

}
