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
    /// Rule that converts a <see cref="LogicalCorrelate"/> to a <see cref="ClrAsyncEnumerableCorrelate"/>.
    /// </summary>
    public class ClrAsyncEnumerableCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableCorrelateRule Create()
        {
            return (ClrAsyncEnumerableCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalCorrelate), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableCorrelateRule>(c => new ClrAsyncEnumerableCorrelateRule(c)))
                .toRule(typeof(ClrAsyncEnumerableCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var c = (Correlate)rel;

            return ClrAsyncEnumerableCorrelate.Create(
                convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                convert(c.getRight(), c.getRight().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType());
        }

    }

}
