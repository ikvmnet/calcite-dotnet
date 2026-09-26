using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCorrelate"/> to a <see cref="ClrCursorCorrelate"/>.
    /// </summary>
    public class ClrCursorCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorCorrelateRule Create()
        {
            return (ClrCursorCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalCorrelate), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorCorrelateRule>(c => new ClrCursorCorrelateRule(c)))
                .toRule(typeof(ClrCursorCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var c = (Correlate)rel;

            return ClrCursorCorrelate.Create(
                convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrCursorConvention.Instance)),
                convert(c.getRight(), c.getRight().getTraitSet().replace(ClrCursorConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType());
        }

    }

}
