using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAggregate"/> to a <see cref="ClrCursorSortedAggregate"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableSortedAggregateRule</c>. The rule requests its input sorted on the group keys and
    /// leaves it to the planner to decide whether providing that order is worth the cost. It is not in
    /// <see cref="ClrCursorRules.Rules"/>; add it explicitly.
    /// </remarks>
    public class ClrCursorSortedAggregateRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorSortedAggregateRule Create()
        {
            return (ClrCursorSortedAggregateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAggregate), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorSortedAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorSortedAggregateRule>(c => new ClrCursorSortedAggregateRule(c)))
                .toRule(typeof(ClrCursorSortedAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule's configuration.</param>
        public ClrCursorSortedAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// Declines an aggregate with grouping sets or with no group keys, as Calcite's rule does; the latter
        /// has nothing to sort by and is left to <see cref="ClrCursorAggregate"/>.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var agg = (Aggregate)rel;
            if (Aggregate.isSimple(agg) == false)
                return null;
            if (agg.getGroupSet().isEmpty())
                return null;

            var inputTraits = rel.getCluster().traitSet()
                .replace(ClrCursorConvention.Instance)
                .replace(RelCollations.of(ImmutableIntList.copyOf(agg.getGroupSet().asList())));

            var selfTraits = inputTraits.replace(RelCollations.of(ImmutableIntList.identity(agg.getGroupSet().cardinality())));

            return new ClrCursorSortedAggregate(
                rel.getCluster(),
                selfTraits,
                convert(agg.getInput(), inputTraits),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
        }

    }

}
