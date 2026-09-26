using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalConditionalCorrelate"/> to a
    /// <see cref="ClrCursorConditionalCorrelate"/>.
    /// </summary>
    public class ClrCursorConditionalCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorConditionalCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorConditionalCorrelateRule Create()
        {
            return (ClrCursorConditionalCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalConditionalCorrelate), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorConditionalCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorConditionalCorrelateRule>(c => new ClrCursorConditionalCorrelateRule(c)))
                .toRule(typeof(ClrCursorConditionalCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorConditionalCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var c = (LogicalConditionalCorrelate)rel;

            return ClrCursorConditionalCorrelate.Create(
                RelOptRule.convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrCursorConvention.Instance)),
                RelOptRule.convert(c.getRight(), c.getRight().getTraitSet().replace(ClrCursorConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType(),
                c.getCondition());
        }

    }

}
