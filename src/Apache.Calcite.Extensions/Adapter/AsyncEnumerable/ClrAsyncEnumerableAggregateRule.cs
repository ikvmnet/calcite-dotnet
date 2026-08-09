using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAggregate"/> to a <see cref="ClrAsyncEnumerableAggregate"/>.
    /// </summary>
    public class ClrAsyncEnumerableAggregateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableAggregateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableAggregateRule Create()
        {
            return (ClrAsyncEnumerableAggregateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAggregate), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableAggregateRule>(c => new ClrAsyncEnumerableAggregateRule(c)))
                .toRule(typeof(ClrAsyncEnumerableAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var aggregate = (Aggregate)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrAsyncEnumerableConvention.Instance);

            try
            {
                return new ClrAsyncEnumerableAggregate(
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
