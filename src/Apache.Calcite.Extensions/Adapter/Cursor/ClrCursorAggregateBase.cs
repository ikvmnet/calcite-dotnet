using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Function;
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
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Base class for an aggregate of the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableAggregateBase</c>: the accumulator state, the per-call adders, the lambda factory,
    /// and the contexts an aggregate implementor is given. Calcite declares these protected, so they are ported.
    ///
    /// <para>They are static, where Calcite's are instance methods that do not read <c>this</c>, so that
    /// <see cref="ClrCursorWindow"/>, which is not an aggregate, can use them. Calcite's window declares each
    /// aggregate's state as locals of the method it generates; an expression tree has no such method, so the
    /// window keeps its state in one synthetic record, as an aggregate does.</para>
    /// </remarks>
    public abstract class ClrCursorAggregateBase : Aggregate
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set.</param>
        /// <param name="hints">The hints.</param>
        /// <param name="input">The input.</param>
        /// <param name="groupSet">The fields to group by.</param>
        /// <param name="groupSets">The grouping sets, or null for the group set alone.</param>
        /// <param name="aggCalls">The aggregate calls.</param>
        protected ClrCursorAggregateBase(RelOptCluster cluster, RelTraitSet traitSet, java.util.List hints, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
            base(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls)
        {

        }

        /// <summary>
        /// Returns whether any of the calls carries an ordering of its own (<c>WITHIN GROUP</c>).
        /// </summary>
        /// <param name="aggs">The <see cref="ClrAggImpState"/> of each call.</param>
        /// <returns>True if any call has a non-empty collation.</returns>
        /// <remarks>
        /// Decides which factory <see cref="ImplementLambdaFactory"/> builds.
        /// </remarks>
        protected static bool HasOrderedCall(java.util.List aggs)
        {
            for (int i = 0; i < aggs.size(); i++)
                if (((ClrAggImpState)aggs.get(i)).call.collation.equals(RelCollations.EMPTY) == false)
                    return true;

            return false;
        }

        /// <summary>
        /// Returns the prefix for the names of one aggregate's state variables.
        /// </summary>
        /// <param name="agg">The aggregate call's state.</param>
        /// <returns><c>a</c> and the call's index, preceded by the function's name under
        /// <c>CalciteSystemProperty.DEBUG</c>, as Calcite names them.</returns>
        internal static string AggName(AggImpState agg)
        {
            var name = $"a{agg.aggIdx}";

            if (org.apache.calcite.config.CalciteSystemProperty.DEBUG.value() is java.lang.Boolean debug && debug.booleanValue())
                name = Util.toJavaId(agg.call.getAggregation().getName(), 0).Substring("ID$0$".Length) + name;

            return name;
        }

        /// <summary>
        /// Adds to <paramref name="initBlock"/> the statements that build the accumulator record from the
        /// initial state values and return it.
        /// </summary>
        /// <param name="initExpressions">The initial value of each state field, in order.</param>
        /// <param name="initBlock">The block to add to.</param>
        /// <param name="accPhysType">The accumulator's physical type.</param>
        /// <remarks>
        /// Mirrors <c>EnumerableAggregateBase.declareParentAccumulator</c>.
        /// </remarks>
        internal static void DeclareParentAccumulator(java.util.List initExpressions, J.BlockBuilder initBlock, PhysType accPhysType)
        {
            if (accPhysType.getJavaRowType() is org.apache.calcite.jdbc.JavaTypeFactoryImpl.SyntheticRecordType synType)
            {
                // assigned a field at a time rather than through a constructor, as Calcite does
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
        /// Declares the state variables of each aggregate call in <paramref name="initBlock"/> and adds the
        /// statements that reset them.
        /// </summary>
        /// <param name="initExpressions">Receives the declared state variables, in order.</param>
        /// <param name="initBlock">The block that initializes the accumulator.</param>
        /// <param name="aggs">The <see cref="ClrAggImpState"/> of each call; each gets its context and state set.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="inputRowType">The input's row type.</param>
        /// <param name="groupSet">The fields grouped by.</param>
        /// <param name="groupSets">The grouping sets.</param>
        /// <returns>The Java type of every state field, in order.</returns>
        protected static java.util.List CreateAggStateTypes(java.util.List initExpressions, J.BlockBuilder initBlock, java.util.List aggs, JavaTypeFactory typeFactory, RelDataType inputRowType, ImmutableBitSet groupSet, java.util.List groupSets)
        {
            var aggStateTypes = new java.util.ArrayList();

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                agg.context = new ClrAggContext(agg, typeFactory, inputRowType, groupSet, groupSets);

                var state = agg.Implementor.getStateType(agg.context);
                if (state.isEmpty())
                {
                    agg.state = com.google.common.collect.ImmutableList.of();
                    continue;
                }

                aggStateTypes.addAll(state);

                var aggName = AggName(agg);
                var decls = new java.util.ArrayList(state.size());
                for (int j = 0; j < state.size(); j++)
                {
                    var pe = J.Expressions.parameter((java.lang.reflect.Type)state.get(j), initBlock.newName($"{aggName}s{j}"));
                    initBlock.add(J.Expressions.declare(0, pe, null));
                    decls.add(pe);
                }

                agg.state = decls;
                initExpressions.addAll(decls);
                agg.Implementor.implementReset(agg.context, new AggResultContextImpl(initBlock, agg.call, decls, null, null));
            }

            return aggStateTypes;
        }

        /// <summary>
        /// Builds, for each aggregate call, a <see cref="Function2"/> that folds one input row into the
        /// accumulator and returns it.
        /// </summary>
        /// <remarks>
        /// Repoints each call's state at the accumulator record's fields.
        /// </remarks>
        protected static java.util.List CreateAccumulatorAdders(
            ClrCursorRelImplementor implementor,
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
                var agg = (ClrAggImpState)aggs.get(i);

                var stateSize = agg.state.size();
                var accumulator = new java.util.ArrayList(stateSize);
                for (int j = 0; j < stateSize; j++)
                    accumulator.add(accPhysType.fieldReference(acc_, j + stateOffset));

                agg.state = accumulator;
                stateOffset += stateSize;

                agg.Implementor.implementAdd(agg.context, new ClrAggAddContext(builder, accumulator, agg, inputPhysType, in_, typeFactory, implementor.Conformance));
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
        /// Builds an expression that creates the <see cref="AggregateLambdaFactory"/> the aggregate's
        /// initializer, adder and result selector are taken from.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="inputPhysType">The input's physical type.</param>
        /// <param name="aggs">The <see cref="ClrAggImpState"/> of each call.</param>
        /// <param name="adders">The adders from <see cref="CreateAccumulatorAdders"/>.</param>
        /// <param name="accumulatorInitializer">A <see cref="Function0"/> that creates an accumulator.</param>
        /// <param name="hasOrderedCall">The result of <see cref="HasOrderedCall"/>.</param>
        /// <param name="sourceType">The input row type.</param>
        /// <returns>The factory expression.</returns>
        /// <remarks>
        /// As in Calcite: with no ordered call, a <c>BasicAggregateLambdaFactory</c> folds each row as it
        /// arrives. Otherwise a <c>LazyAggregateLambdaFactory</c> holds a group's rows and folds them at the end,
        /// through a <c>SourceSorter</c> for each ordered call and a <c>BasicLazyAccumulator</c> for each other.
        /// </remarks>
        protected static Expression ImplementLambdaFactory(
            ClrCursorRelImplementor implementor,
            ClrPhysType inputPhysType,
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
                    Expression.Assign(adderList, Expression.New(LinkedListConstructor)),
                };

                for (int i = 0; i < adders.size(); i++)
                    adderBody.Add(Expression.Call(adderList, CollectionAdd, Expression.Convert((Expression)adders.get(i), typeof(object))));

                adderBody.Add(Expression.New(BasicFactory, accumulatorInitializer, adderList));

                return Expression.Block(typeof(AggregateLambdaFactory), [adderList], adderBody);
            }

            var lazyList = Expression.Variable(typeof(java.util.List), "lazyAccumulators");
            var body = new System.Collections.Generic.List<Expression>
            {
                Expression.Assign(lazyList, Expression.New(LinkedListConstructor)),
            };

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                var adder = (Expression)adders.get(i);

                if (agg.call.collation.equals(RelCollations.EMPTY))
                {
                    // an unordered call folds the held rows one at a time, in arrival order
                    body.Add(Expression.Call(lazyList, CollectionAdd,
                        Expression.Convert(Expression.New(BasicLazyAccumulator, adder), typeof(object))));

                    continue;
                }

                var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(agg.call.collation.getFieldCollations());
                var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));

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

        /// <summary>
        /// A constructor or method that <see cref="ImplementLambdaFactory"/> or an aggregate's implementation
        /// calls in the expression tree, where Calcite writes the same call in linq4j.
        /// </summary>
        protected static readonly System.Reflection.ConstructorInfo BasicFactory = typeof(BasicAggregateLambdaFactory).GetConstructors()[0];

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.ConstructorInfo LazyFactory = typeof(LazyAggregateLambdaFactory).GetConstructors()[0];

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.ConstructorInfo BasicLazyAccumulator = typeof(BasicLazyAccumulator).GetConstructors()[0];

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.ConstructorInfo SourceSorter = typeof(SourceSorter).GetConstructors()[0];

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.ConstructorInfo LinkedListConstructor = typeof(java.util.LinkedList).GetConstructor([])
            ?? throw new InvalidOperationException("java.util.LinkedList has no no-arg constructor.");

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.MethodInfo CollectionAdd = typeof(java.util.List).GetMethod("add", [typeof(object)])
            ?? throw new InvalidOperationException("java.util.List has no add(Object).");

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.MethodInfo AccInitializer = typeof(AggregateLambdaFactory).GetMethod("accumulatorInitializer")
            ?? throw new InvalidOperationException("AggregateLambdaFactory has no accumulatorInitializer().");

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.MethodInfo AccAdder = typeof(AggregateLambdaFactory).GetMethod("accumulatorAdder")
            ?? throw new InvalidOperationException("AggregateLambdaFactory has no accumulatorAdder().");

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.MethodInfo ResultSelector = typeof(AggregateLambdaFactory).GetMethod("resultSelector")
            ?? throw new InvalidOperationException("AggregateLambdaFactory has no resultSelector().");

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.MethodInfo SingleGroupResultSelector = typeof(AggregateLambdaFactory).GetMethod("singleGroupResultSelector")
            ?? throw new InvalidOperationException("AggregateLambdaFactory has no singleGroupResultSelector().");

        /// <inheritdoc cref="BasicFactory" />
        protected static readonly System.Reflection.MethodInfo Function0Apply = typeof(Function0).GetMethod("apply")
            ?? throw new InvalidOperationException("Function0 has no apply().");

        /// <summary>
        /// The <see cref="AggContext"/> an aggregate implementor is given for its call.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableAggregateBase.AggContextImpl</c>, an inner class that reads the input row type
        /// and grouping from its enclosing node; this one takes them as parameters.
        /// </remarks>
        /// <param name="agg">The call's state.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="inputRowType">The input's row type.</param>
        /// <param name="groupSet">The fields grouped by.</param>
        /// <param name="sets">The grouping sets.</param>
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
        /// The <see cref="AggAddContext"/> an aggregate implementor is given to fold one row in.
        /// </summary>
        /// <remarks>
        /// Mirrors the anonymous <c>AggAddContextImpl</c> subclass in <c>EnumerableAggregateBase</c>.
        /// </remarks>
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

                // PERCENTILE_CONT and PERCENTILE_DISC take the fraction as their only argument but aggregate
                // over the WITHIN GROUP (ORDER BY ...) column, so that column is passed as an extra argument;
                // the source sorter has already put the rows in its order
                if (agg.call.getAggregation().isPercentile())
                {
                    var collations = agg.call.collation.getFieldCollations();
                    for (int i = 0; i < collations.size(); i++)
                        args.add(RexInputRef.of(((RelFieldCollation)collations.get(i)).getFieldIndex(), inputTypes));
                }

                return args;
            }

            /// <inheritdoc />
            public override RexNode? rexFilterArgument()
            {
                return agg.call.filterArg < 0 ? null : RexInputRef.of(agg.call.filterArg, inputPhysType.getRowType());
            }

            /// <inheritdoc />
            public override RexToLixTranslator rowTranslator()
            {
                return RexToLixTranslator.forAggregation(typeFactory, currentBlock(), new RexToLixTranslator.InputGetterImpl(in_, inputPhysType), conformance);
            }

        }

    }

}
