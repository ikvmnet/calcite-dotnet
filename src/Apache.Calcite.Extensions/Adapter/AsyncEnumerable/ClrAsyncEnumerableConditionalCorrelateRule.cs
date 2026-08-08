using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalConditionalCorrelate"/> to a
    /// <see cref="ClrAsyncEnumerableConditionalCorrelate"/>.
    /// </summary>
    public class ClrAsyncEnumerableConditionalCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableConditionalCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableConditionalCorrelateRule Create()
        {
            return (ClrAsyncEnumerableConditionalCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalConditionalCorrelate), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableConditionalCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableConditionalCorrelateRule>(c => new ClrAsyncEnumerableConditionalCorrelateRule(c)))
                .toRule(typeof(ClrAsyncEnumerableConditionalCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableConditionalCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var c = (LogicalConditionalCorrelate)rel;

            return ClrAsyncEnumerableConditionalCorrelate.Create(
                RelOptRule.convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                RelOptRule.convert(c.getRight(), c.getRight().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType(),
                c.getCondition());
        }

    }

}
