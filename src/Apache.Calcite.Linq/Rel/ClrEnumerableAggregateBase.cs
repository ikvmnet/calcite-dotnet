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
    /// Base class for an aggregate of the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableAggregateBase</c>, holding what that holds: the state each aggregate
    /// accumulates in, the adder per aggregate, the lambda factory, and the two contexts an implementor is
    /// told about its call through. Every one of them is protected in Calcite, so like
    /// <c>EnumUtils.joinSelector</c> they are ported rather than reused.
    ///
    /// <para>They are static here where Calcite's are instance methods that never read <c>this</c>, because
    /// <see cref="ClrEnumerableWindow"/> needs two of them and is not an aggregate. Calcite's window declares
    /// each aggregate's state as locals of the method it generates; an expression tree has no method to
    /// declare them in, so a window folds its state into one synthetic record exactly as an aggregate does.
    /// That is the whole reason the two accumulator helpers are reachable from outside.</para>
    /// </remarks>
    public abstract class ClrEnumerableAggregateBase : Aggregate
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="hints"></param>
        /// <param name="input"></param>
        /// <param name="groupSet"></param>
        /// <param name="groupSets"></param>
        /// <param name="aggCalls"></param>
        protected ClrEnumerableAggregateBase(RelOptCluster cluster, RelTraitSet traitSet, java.util.List hints, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
            base(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls)
        {

        }

        /// <summary>
        /// Returns whether any of the calls carries an ordering of its own.
        /// </summary>
        /// <param name="aggs"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite answers a call that has one with <c>LazyAggregateLambdaFactory</c> and a
        /// <c>SourceSorter</c> per call. Neither is written here, so such a call is refused where the node is
        /// built rather than answered wrongly.
        /// </remarks>
        protected static bool HasOrderedCall(java.util.List aggs)
        {
            for (int i = 0; i < aggs.size(); i++)
                if (((AggImpState)aggs.get(i)).call.collation.equals(RelCollations.EMPTY) == false)
                    return true;

            return false;
        }

        /// <summary>
        /// Returns the physical type of an accumulator, whose Java type is a synthetic record rather than a
        /// relational type.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="javaRowClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// PhysTypeImpl has this and keeps it package private. The row type is rebuilt from the fields of the
        /// record and the format is left unoptimised, exactly as it does, because an accumulator of one field
        /// is still a record.
        /// </remarks>
        internal static PhysType AccumulatorPhysType(JavaTypeFactory typeFactory, java.lang.reflect.Type javaRowClass)
        {
            var builder = typeFactory.builder();

            if (javaRowClass is J.Types.RecordType recordType)
            {
                var fields = recordType.getRecordFields();
                for (int i = 0; i < fields.size(); i++)
                {
                    var field = (J.Types.RecordField)fields.get(i);
                    builder.add(field.getName(), typeFactory.createType(field.getType()));
                }
            }

            return PhysTypeImpl.of(typeFactory, builder.build(), JavaRowFormat.CUSTOM, false);
        }

        /// <summary>
        /// Adds the statement that builds an accumulator from its parts.
        /// </summary>
        /// <param name="initExpressions"></param>
        /// <param name="initBlock"></param>
        /// <param name="accPhysType"></param>
        /// <remarks>
        /// <c>EnumerableAggregateBase.declareParentAccumulator</c>, which is protected.
        /// </remarks>
        internal static void DeclareParentAccumulator(java.util.List initExpressions, J.BlockBuilder initBlock, PhysType accPhysType)
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
        /// Declares the state each aggregate accumulates in, and the block that resets it.
        /// </summary>
        /// <param name="initExpressions"></param>
        /// <param name="initBlock"></param>
        /// <param name="aggs"></param>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        protected static java.util.List CreateAggStateTypes(java.util.List initExpressions, J.BlockBuilder initBlock, java.util.List aggs, JavaTypeFactory typeFactory, RelDataType inputRowType, ImmutableBitSet groupSet, java.util.List groupSets)
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
        /// Builds the lambda that folds one row into the accumulator, for each aggregate.
        /// </summary>
        protected static java.util.List CreateAccumulatorAdders(
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
        /// <param name="implementor"></param>
        /// <param name="inputPhysType"></param>
        /// <param name="aggs"></param>
        /// <param name="adders"></param>
        /// <param name="accumulatorInitializer"></param>
        /// <param name="hasOrderedCall"></param>
        /// <param name="sourceType"></param>
        /// <returns></returns>
        /// <remarks>
        /// Two factories, as Calcite has two. Where no call carries an ordering the adders fold each row as
        /// it arrives. Where one does, the rows of a group are held instead and folded at the end, once the
        /// call's own ordering has been applied to them — a <c>SourceSorter</c> per ordered call, a
        /// <c>BasicLazyAccumulator</c> per unordered one, and <c>LazyAggregateLambdaFactory</c> over the
        /// list. All three are Calcite's, and public, so they are used rather than written again.
        /// </remarks>
        protected static Expression ImplementLambdaFactory(
            ClrEnumerableRelImplementor implementor,
            PhysType inputPhysType,
            java.util.List aggs,
            java.util.List adders,
            Expression accumulatorInitializer,
            bool hasOrderedCall,
            Type sourceType)
        {
            if (hasOrderedCall == false)
            {
                var adderList = Expression.Variable(typeof(java.util.List), "accumulatorAdders");
                var adderBody = new System.Collections.Generic.List<Expression>
                {
                    Expression.Assign(adderList, Expression.New(typeof(java.util.LinkedList).GetConstructor([])!)),
                };

                for (int i = 0; i < adders.size(); i++)
                    adderBody.Add(Expression.Call(adderList, CollectionAdd, Expression.Convert((Expression)adders.get(i), typeof(object))));

                adderBody.Add(Expression.New(BasicFactory, accumulatorInitializer, adderList));

                return Expression.Block(typeof(AggregateLambdaFactory), [adderList], adderBody);
            }

            var lazyList = Expression.Variable(typeof(java.util.List), "lazyAccumulators");
            var body = new System.Collections.Generic.List<Expression>
            {
                Expression.Assign(lazyList, Expression.New(typeof(java.util.LinkedList).GetConstructor([])!)),
            };

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (AggImpState)aggs.get(i);
                var adder = (Expression)adders.get(i);

                if (agg.call.collation.equals(RelCollations.EMPTY))
                {
                    // a call with no ordering of its own still folds a row at a time, once the rows are held
                    body.Add(Expression.Call(lazyList, CollectionAdd,
                        Expression.Convert(Expression.New(BasicLazyAccumulator, adder), typeof(object))));

                    continue;
                }

                var pair = inputPhysType.generateCollationKey(agg.call.collation.getFieldCollations());
                var keySelector = implementor.Translator.TranslateSelector((J.Expression)pair.getKey(), sourceType);
                var comparator = pair.getValue() == null
                    ? Expression.Constant(null, typeof(java.util.Comparator))
                    : implementor.Translator.Translate((J.Expression)pair.getValue());

                body.Add(Expression.Call(lazyList, CollectionAdd,
                    Expression.Convert(
                        Expression.New(SourceSorter, adder, Function1Of(keySelector, sourceType, keySelector.ReturnType), comparator),
                        typeof(object))));
            }

            body.Add(Expression.New(LazyFactory, accumulatorInitializer, lazyList));

            return Expression.Block(typeof(AggregateLambdaFactory), [lazyList], body);
        }

        /// <summary>
        /// Wraps a lambda as a linq4j <see cref="Function0"/>.
        /// </summary>
        protected static Expression Function0Of(LambdaExpression lambda, Type result)
        {
            return Expression.New(typeof(DelegateFunction0<>).MakeGenericType(result).GetConstructors()[0], lambda);
        }

        /// <summary>
        /// Wraps a lambda as a linq4j <see cref="Function1"/>.
        /// </summary>
        protected static Expression Function1Of(LambdaExpression lambda, Type arg0, Type result)
        {
            return Expression.New(typeof(DelegateFunction1Of<,>).MakeGenericType(arg0, result).GetConstructors()[0], lambda);
        }

        /// <summary>
        /// Wraps a lambda as a linq4j <see cref="Function2"/>.
        /// </summary>
        protected static Expression Function2Of(LambdaExpression lambda, Type arg0, Type arg1, Type result)
        {
            return Expression.New(typeof(DelegateFunction2<,,>).MakeGenericType(arg0, arg1, result).GetConstructors()[0], lambda);
        }

        protected static readonly System.Reflection.ConstructorInfo BasicFactory = typeof(BasicAggregateLambdaFactory).GetConstructors()[0];
        protected static readonly System.Reflection.ConstructorInfo LazyFactory = typeof(LazyAggregateLambdaFactory).GetConstructors()[0];
        protected static readonly System.Reflection.ConstructorInfo BasicLazyAccumulator = typeof(BasicLazyAccumulator).GetConstructors()[0];
        protected static readonly System.Reflection.ConstructorInfo SourceSorter = typeof(SourceSorter).GetConstructors()[0];
        protected static readonly System.Reflection.MethodInfo CollectionAdd = typeof(java.util.List).GetMethod("add", [typeof(object)])!;
        protected static readonly System.Reflection.MethodInfo AccInitializer = typeof(AggregateLambdaFactory).GetMethod("accumulatorInitializer")!;
        protected static readonly System.Reflection.MethodInfo AccAdder = typeof(AggregateLambdaFactory).GetMethod("accumulatorAdder")!;
        protected static readonly System.Reflection.MethodInfo ResultSelector = typeof(AggregateLambdaFactory).GetMethod("resultSelector")!;
        protected static readonly System.Reflection.MethodInfo SingleGroupResultSelector = typeof(AggregateLambdaFactory).GetMethod("singleGroupResultSelector")!;
        protected static readonly System.Reflection.MethodInfo Function0Apply = typeof(Function0).GetMethod("apply")!;
        /// <summary>
        /// What an aggregate implementor is told about the call it is implementing.
        /// </summary>
        /// <param name="agg"></param>
        /// <param name="typeFactory"></param>
        protected sealed class ClrAggContext(AggImpState agg, JavaTypeFactory typeFactory, RelDataType inputRowType, ImmutableBitSet groupSet, java.util.List sets) : AggContext
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
        protected sealed class ClrAggAddContext(J.BlockBuilder block, java.util.List accumulator, AggImpState agg, PhysType inputPhysType, J.ParameterExpression in_, JavaTypeFactory typeFactory, org.apache.calcite.sql.validate.SqlConformance conformance) :
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
