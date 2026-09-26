using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAggregate"/> to a <see cref="ClrDataCursorAggregate"/>.
    /// </summary>
    public class ClrDataCursorAggregateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorAggregateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorAggregateRule Create()
        {
            return (ClrDataCursorAggregateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAggregate), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorAggregateRule>(c => new ClrDataCursorAggregateRule(c)))
                .toRule(typeof(ClrDataCursorAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var aggregate = (Aggregate)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrDataCursorConvention.Instance);

            // an aggregate whose function nothing can implement is refused here rather than left to fail
            // while the chosen plan is being implemented. The table is the cluster's, so a caller that put
            // its own implementors on the cluster is asked about them and not about Calcite's defaults
            var implementors = RexImplementorTables.of(rel.getCluster());
            for (var i = aggregate.getAggCallList().iterator(); i.hasNext();)
                if (implementors.get(((AggregateCall)i.next()).getAggregation(), false) == null)
                    return null;

            try
            {
                return new ClrDataCursorAggregate(
                    rel.getCluster(),
                    traitSet,
                    RelOptRule.convert(aggregate.getInput(), traitSet),
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList());
            }
            catch (InvalidRelException)
            {
                // an aggregate this convention cannot implement is left for another rule, exactly as Calcite
                // leaves one: refusing here rather than in Implement, which runs after a plan has been chosen
                return null;
            }
        }

    }

}
