using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCorrelate"/> to a <see cref="ClrDataCursorCorrelate"/>.
    /// </summary>
    public class ClrDataCursorCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorCorrelateRule Create()
        {
            return (ClrDataCursorCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalCorrelate), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorCorrelateRule>(c => new ClrDataCursorCorrelateRule(c)))
                .toRule(typeof(ClrDataCursorCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var c = (Correlate)rel;

            return ClrDataCursorCorrelate.Create(
                convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                convert(c.getRight(), c.getRight().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType());
        }

    }

}
