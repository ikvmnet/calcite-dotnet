using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAggregate"/> to a <see cref="ClrEnumerableAggregate"/>.
    /// </summary>
    public class ClrEnumerableAggregateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableAggregateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableAggregateRule Create()
        {
            return (ClrEnumerableAggregateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAggregate), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableAggregateRule>(c => new ClrEnumerableAggregateRule(c)))
                .toRule(typeof(ClrEnumerableAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var aggregate = (Aggregate)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrEnumerableConvention.Instance);

            try
            {
                return new ClrEnumerableAggregate(
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
                return null!;
            }
        }

    }

}
