using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.enumerable.impl;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.util;
using org.apache.calcite.util.mapping;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Aggregate"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention for an input sorted on the group key.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableSortedAggregate</c>. Groups are produced in key order, and only the current
    /// group's accumulator is held. Grouping sets are not supported. Members shared with
    /// <see cref="ClrCursorAggregate"/> are on <see cref="ClrCursorAggregateBase"/>.
    /// </remarks>
    public class ClrCursorSortedAggregate : ClrCursorAggregateBase, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits, in <see cref="ClrCursorConvention"/> and carrying a
        /// collation on the group keys.</param>
        /// <param name="input">The input, sorted on the group keys.</param>
        /// <param name="groupSet">The group keys.</param>
        /// <param name="groupSets">The grouping sets; only a single set equal to <paramref name="groupSet"/>
        /// can be implemented.</param>
        /// <param name="aggCalls">The aggregate calls, a list of <see cref="AggregateCall"/>.</param>
        public ClrCursorSortedAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, groupSet, groupSets, aggCalls)
        {
            if (getConvention() is not ClrCursorConvention)
                throw new java.lang.AssertionError();
        }

        /// <inheritdoc />
        public override Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls)
        {
            return new ClrCursorSortedAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Mirrors <c>EnumerableSortedAggregate.passThroughTraits</c>, except that a required trait set of
        /// another convention is refused, as in <see cref="ClrCursorMergeJoin.passThroughTraits"/>.
        /// </remarks>
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            if (isSimple(this) == false)
                return null;

            // Calcite returns required as this node's trait set, which would give a node of this convention
            // another convention's trait
            if (required.getConvention() != getConvention())
                return null;

            var inputTraits = getInput().getTraitSet();
            var collation = required.getCollation() ?? throw new java.lang.NullPointerException($"collation trait is null, required traits are {required}");
            var requiredKeys = ImmutableBitSet.of(RelCollations.ordinals(collation));
            var groupKeys = ImmutableBitSet.range(groupSet.cardinality());

            var mapping = Mappings.source(groupSet.toList(), getInput().getRowType().getFieldCount());

            if (requiredKeys.equals(groupKeys))
            {
                var inputCollation = RexUtil.apply(mapping, collation);

                return org.apache.calcite.util.Pair.of(required, com.google.common.collect.ImmutableList.of(inputTraits.replace(inputCollation)));
            }

            if (groupKeys.contains(requiredKeys))
            {
                // GROUP BY a, b, c ORDER BY c, b: the group keys not in the collation are appended to it
                var list = new java.util.ArrayList(collation.getFieldCollations());
                for (var i = groupKeys.except(requiredKeys).iterator(); i.hasNext();)
                    list.add(new RelFieldCollation(((java.lang.Integer)i.next()).intValue()));

                var aggCollation = RelCollations.of(list);
                var inputCollation = RexUtil.apply(mapping, aggCollation);

                return org.apache.calcite.util.Pair.of(getTraitSet().replace(aggCollation), com.google.common.collect.ImmutableList.of(inputTraits.replace(inputCollation)));
            }

            // the group keys do not cover the required keys, as in GROUP BY a, b ORDER BY a, b, c
            return null;
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (isSimple(this) == false)
                throw new java.lang.UnsupportedOperationException("ClrCursorSortedAggregate: grouping sets");

            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = inputPhysType.RowType;
            var rowType = physType.RowType;

            // Calcite's aggregate implementors write the accumulator, key and output row into linq4j blocks,
            // so those use Calcite physical types
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var keyPhysType = inputCalcite.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);

            // the key selector and comparator are built directly as CLR expressions, so they need a ClrPhysType
            var keyClr = ClrPhysTypeImpl.Of(typeFactory, keyPhysType.getRowType(), keyPhysType.getFormat(), false);
            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new ClrAggImpState(i, (AggregateCall)getAggCallList().get(i), false, RexImplementorTables.of(getCluster())));

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

            // hasOrderedCall is false, as EnumerableSortedAggregate passes it, so a call's WITHIN GROUP
            // ordering is not applied
            var lambdaFactory = ImplementLambdaFactory(implementor, inputPhysType, aggs, adders, accumulatorInitializer, false, sourceType);

            var resultBlock = new J.BlockBuilder();
            var results = new java.util.ArrayList();

            var key_ = J.Expressions.parameter(keyPhysType.getJavaRowType(), "key");
            var keyParameter = Expression.Parameter(ClrTypes.Resolve(keyPhysType.getJavaRowType()), "key");
            implementor.Translator.Bind(key_, keyParameter);

            for (int j = 0; j < groupCount; j++)
                results.add(keyPhysType.fieldReference(key_, j));

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                results.add(agg.Implementor.implementResult(agg.context, new AggResultContextImpl(resultBlock, agg.call, agg.state, key_, keyPhysType)));
            }

            resultBlock.add(J.Expressions.return_(null, outputCalcite.record(results)));

            var keySelector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), keyClr.Format);

            var groupResultSelector = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(keyParameter.Type, accType, rowType),
                implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                keyParameter,
                accParameter);

            // the comparator decides where one group ends; it is built from this node's collation so that
            // null keys compare in the same order the input is sorted in
            var comparator = keyClr.GenerateComparator(getTraitSet().getCollation() ?? throw new java.lang.NullPointerException($"getTraitSet().getCollation() is null; traits are {getTraitSet()}"));

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.SortedGroupBy.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    comparator));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (isSimple(this) == false)
                throw new java.lang.UnsupportedOperationException("ClrCursorSortedAggregate: grouping sets");

            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = inputPhysType.RowType;
            var rowType = physType.RowType;

            // Calcite's aggregate implementors write the accumulator, key and output row into linq4j blocks,
            // so those use Calcite physical types
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var keyPhysType = inputCalcite.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);

            // the key selector and comparator are built directly as CLR expressions, so they need a ClrPhysType
            var keyClr = ClrPhysTypeImpl.Of(typeFactory, keyPhysType.getRowType(), keyPhysType.getFormat(), false);
            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new ClrAggImpState(i, (AggregateCall)getAggCallList().get(i), false, RexImplementorTables.of(getCluster())));

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

            // hasOrderedCall is false, as EnumerableSortedAggregate passes it, so a call's WITHIN GROUP
            // ordering is not applied
            var lambdaFactory = ImplementLambdaFactory(implementor, inputPhysType, aggs, adders, accumulatorInitializer, false, sourceType);

            var resultBlock = new J.BlockBuilder();
            var results = new java.util.ArrayList();

            var key_ = J.Expressions.parameter(keyPhysType.getJavaRowType(), "key");
            var keyParameter = Expression.Parameter(ClrTypes.Resolve(keyPhysType.getJavaRowType()), "key");
            implementor.Translator.Bind(key_, keyParameter);

            for (int j = 0; j < groupCount; j++)
                results.add(keyPhysType.fieldReference(key_, j));

            for (int i = 0; i < aggs.size(); i++)
            {
                var agg = (ClrAggImpState)aggs.get(i);
                results.add(agg.Implementor.implementResult(agg.context, new AggResultContextImpl(resultBlock, agg.call, agg.state, key_, keyPhysType)));
            }

            resultBlock.add(J.Expressions.return_(null, outputCalcite.record(results)));

            var keySelector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), keyClr.Format);

            var groupResultSelector = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(keyParameter.Type, accType, rowType),
                implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                keyParameter,
                accParameter);

            // the comparator decides where one group ends; it is built from this node's collation so that
            // null keys compare in the same order the input is sorted in
            var comparator = keyClr.GenerateComparator(getTraitSet().getCollation() ?? throw new java.lang.NullPointerException($"getTraitSet().getCollation() is null; traits are {getTraitSet()}"));

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SortedGroupByAsync.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    comparator));
        }

    }

}
