using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.util;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAggregate"/> to a <see cref="ClrAsyncEnumerableSortedAggregate"/>.
    /// </summary>
    /// <remarks>
    /// The rule asks its input for a collation on the group set rather than taking what the input has, and
    /// the planner decides whether satisfying that is worth it. It is chosen where the query wants its
    /// output ordered by the group key, because then the ordering is free.
    ///
    /// <para>Calcite does not put this rule in <c>ENUMERABLE_RULES</c>; a caller turns it on.</para>
    /// </remarks>
    public class ClrAsyncEnumerableSortedAggregateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableSortedAggregateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableSortedAggregateRule Create()
        {
            return (ClrAsyncEnumerableSortedAggregateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAggregate), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableSortedAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableSortedAggregateRule>(c => new ClrAsyncEnumerableSortedAggregateRule(c)))
                .toRule(typeof(ClrAsyncEnumerableSortedAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableSortedAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// One line is not Calcite's: an aggregate over no group set is refused. The node tells one group from
        /// the next with a comparator built from the collation it carries, and for an empty group set that
        /// collation is empty; Calcite's rule builds the node anyway and its <c>implement</c> then fails. A
        /// global aggregate has nothing to sort by and belongs to <see cref="ClrAsyncEnumerableAggregate"/>.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var agg = (Aggregate)rel;
            if (Aggregate.isSimple(agg) == false)
                return null;
            if (agg.getGroupSet().isEmpty())
                return null;

            var inputTraits = rel.getCluster().traitSet()
                .replace(ClrAsyncEnumerableConvention.Instance)
                .replace(RelCollations.of(ImmutableIntList.copyOf(agg.getGroupSet().asList())));

            var selfTraits = inputTraits.replace(RelCollations.of(ImmutableIntList.identity(agg.getGroupSet().cardinality())));

            return new ClrAsyncEnumerableSortedAggregate(
                rel.getCluster(),
                selfTraits,
                convert(agg.getInput(), inputTraits),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
        }

    }

}
