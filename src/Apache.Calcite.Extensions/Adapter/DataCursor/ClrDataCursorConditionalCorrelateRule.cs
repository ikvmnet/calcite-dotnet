using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalConditionalCorrelate"/> to a
    /// <see cref="ClrDataCursorConditionalCorrelate"/>.
    /// </summary>
    public class ClrDataCursorConditionalCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorConditionalCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorConditionalCorrelateRule Create()
        {
            return (ClrDataCursorConditionalCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalConditionalCorrelate), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorConditionalCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorConditionalCorrelateRule>(c => new ClrDataCursorConditionalCorrelateRule(c)))
                .toRule(typeof(ClrDataCursorConditionalCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorConditionalCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var c = (LogicalConditionalCorrelate)rel;

            return ClrDataCursorConditionalCorrelate.Create(
                RelOptRule.convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                RelOptRule.convert(c.getRight(), c.getRight().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType(),
                c.getCondition());
        }

    }

}
