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

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of <see cref="Aggregate"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention, for an input that already arrives sorted on the group key.
    /// </summary>
    /// <remarks>
    /// Chosen where the query wants its output ordered by the group key and the input carries that
    /// collation: the groups then come out in order and nothing is held but the accumulator of the group
    /// being read. `EnumerableSortedAggregate` is the same, and like it this holds only what
    /// <see cref="ClrEnumerableAggregate"/> does not — everything shared is on
    /// <see cref="ClrEnumerableAggregateBase"/>.
    /// </remarks>
    public class ClrEnumerableSortedAggregate : ClrEnumerableAggregateBase, ClrEnumerableRel
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
        public ClrEnumerableSortedAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, groupSet, groupSets, aggCalls)
        {
            if (getConvention() is not ClrEnumerableConvention)
                throw new java.lang.AssertionError();
        }

        /// <inheritdoc />
        public override Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, java.util.List groupSets, java.util.List aggCalls)
        {
            return new ClrEnumerableSortedAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            if (isSimple(this) == false)
                return null;

            // a required trait set of another convention is refused rather than copied onto, for the reason
            // ClrEnumerableMergeJoin gives at more length
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
                // GROUP BY a, b, c ORDER BY c, b — the keys not ordered by are appended
                var list = new java.util.ArrayList(collation.getFieldCollations());
                for (var i = groupKeys.except(requiredKeys).iterator(); i.hasNext();)
                    list.add(new RelFieldCollation(((java.lang.Integer)i.next()).intValue()));

                var aggCollation = RelCollations.of(list);
                var inputCollation = RexUtil.apply(mapping, aggCollation);

                return org.apache.calcite.util.Pair.of(getTraitSet().replace(aggCollation), com.google.common.collect.ImmutableList.of(inputTraits.replace(inputCollation)));
            }

            // the group keys do not cover the required keys, as in GROUP BY a, b ORDER BY a, b, c, and
            // nothing can be pushed down
            return null;
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (isSimple(this) == false)
                throw new java.lang.UnsupportedOperationException("ClrEnumerableSortedAggregate: grouping sets");

            var typeFactory = implementor.TypeFactory;
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.PreferCustom());
            var inputPhysType = result.PhysType;
            var sourceType = inputPhysType.RowType;
            var rowType = physType.RowType;

            // the accumulator, the group key and the output row are all written by Calcite's aggregate
            // implementors, into blocks of Calcite's, so each of those takes a physical type of Calcite's
            var inputCalcite = PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var keyPhysType = inputCalcite.project(groupSet.asList(), getGroupType() != Group.SIMPLE, JavaRowFormat.LIST);

            // and the key again as ours, because the selector and the comparator are delegates
            var keyClr = ClrPhysTypeImpl.Of(typeFactory, keyPhysType.getRowType(), keyPhysType.getFormat(), false);
            var groupCount = getGroupCount();

            var aggs = new java.util.ArrayList();
            for (int i = 0; i < getAggCallList().size(); i++)
                aggs.add(new AggImpState(i, (AggregateCall)getAggCallList().get(i), false));

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

            var in_ = J.Expressions.parameter(inputPhysType.JavaRowType, "in");
            var acc_ = J.Expressions.parameter(accPhysType.getJavaRowType(), "acc");
            var inParameter = Expression.Parameter(sourceType, "in");
            var accParameter = Expression.Parameter(accType, "acc");
            implementor.Translator.Bind(in_, inParameter);
            implementor.Translator.Bind(acc_, accParameter);

            var adders = CreateAccumulatorAdders(implementor, in_, inParameter, aggs, accPhysType, acc_, accParameter, inputCalcite, typeFactory, accType, sourceType);

            // false, as Calcite passes: an aggregate call with its own ordering is the plain aggregate's
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
                var agg = (AggImpState)aggs.get(i);
                results.add(agg.implementor.implementResult(agg.context, new AggResultContextImpl(resultBlock, agg.call, agg.state, key_, keyPhysType)));
            }

            resultBlock.add(J.Expressions.return_(null, outputCalcite.record(results)));

            var keySelector = inputPhysType.GenerateSelector(inParameter, groupSet.asList(), keyClr.Format);

            var groupResultSelector = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(keyParameter.Type, accType, rowType),
                implementor.Translator.TranslateBody(resultBlock.toBlock(), rowType),
                keyParameter,
                accParameter);

            // the comparator decides where one group ends, and a null in a key has to order consistently,
            // which is why it comes from the collation this node carries rather than from equality
            var comparator = keyClr.GenerateComparator(getTraitSet().getCollation() ?? throw new java.lang.NullPointerException($"getTraitSet().getCollation() is null; traits are {getTraitSet()}"));

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.SortedGroupBy.MakeGenericMethod(sourceType, keySelector.ReturnType, rowType),
                    result.Expression,
                    keySelector,
                    Expression.Call(lambdaFactory, AccInitializer),
                    Expression.Call(lambdaFactory, AccAdder),
                    Expression.Call(lambdaFactory, ResultSelector, Function2Of(groupResultSelector, keyParameter.Type, accType, rowType)),
                    comparator));
        }

    }

}
