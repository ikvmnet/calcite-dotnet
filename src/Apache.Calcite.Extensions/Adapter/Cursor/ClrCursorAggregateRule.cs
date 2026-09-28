using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAggregate"/> to a <see cref="ClrCursorAggregate"/>.
    /// </summary>
    public class ClrCursorAggregateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorAggregateRule"/>.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorAggregateRule Create()
        {
            return (ClrCursorAggregateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAggregate), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorAggregateRule>(c => new ClrCursorAggregateRule(c)))
                .toRule(typeof(ClrCursorAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule configuration.</param>
        public ClrCursorAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// Returns null, leaving the aggregate to another rule, where a call's function has no implementor in the
        /// cluster's <c>RexImplementorTable</c> or where <see cref="ClrCursorAggregate"/> rejects a call.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var aggregate = (Aggregate)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrCursorConvention.Instance);

            // refused here rather than failing in Implement, after the plan is chosen. The cluster's table is
            // the one the node is implemented against, and includes implementors a caller registered on it
            var implementors = RexImplementorTables.of(rel.getCluster());
            for (var i = aggregate.getAggCallList().iterator(); i.hasNext();)
                if (implementors.get(((AggregateCall)i.next()).getAggregation(), false) == null)
                    return null;

            try
            {
                return new ClrCursorAggregate(
                    rel.getCluster(),
                    traitSet,
                    RelOptRule.convert(aggregate.getInput(), traitSet),
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList());
            }
            catch (InvalidRelException)
            {
                // left for another rule, as EnumerableAggregateRule does
                return null;
            }
        }

    }

}
