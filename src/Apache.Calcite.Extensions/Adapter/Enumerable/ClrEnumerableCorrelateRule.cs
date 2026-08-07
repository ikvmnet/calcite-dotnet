using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCorrelate"/> to a <see cref="ClrEnumerableCorrelate"/>.
    /// </summary>
    public class ClrEnumerableCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableCorrelateRule Create()
        {
            return (ClrEnumerableCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalCorrelate), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableCorrelateRule>(c => new ClrEnumerableCorrelateRule(c)))
                .toRule(typeof(ClrEnumerableCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var c = (Correlate)rel;

            return ClrEnumerableCorrelate.Create(
                convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                convert(c.getRight(), c.getRight().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType());
        }

    }

}
