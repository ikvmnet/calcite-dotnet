using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalConditionalCorrelate"/> to a
    /// <see cref="ClrEnumerableConditionalCorrelate"/>.
    /// </summary>
    public class ClrEnumerableConditionalCorrelateRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableConditionalCorrelateRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableConditionalCorrelateRule Create()
        {
            return (ClrEnumerableConditionalCorrelateRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalConditionalCorrelate), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableConditionalCorrelateRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableConditionalCorrelateRule>(c => new ClrEnumerableConditionalCorrelateRule(c)))
                .toRule(typeof(ClrEnumerableConditionalCorrelateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableConditionalCorrelateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var c = (LogicalConditionalCorrelate)rel;

            return ClrEnumerableConditionalCorrelate.Create(
                RelOptRule.convert(c.getLeft(), c.getLeft().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                RelOptRule.convert(c.getRight(), c.getRight().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                c.getCorrelationId(),
                c.getRequiredColumns(),
                c.getJoinType(),
                c.getCondition());
        }

    }

}
