using java.util.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="ClrDataCursorConvention"/> node to a
    /// <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    public class ClrDataCursorToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorToClrEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorToClrEnumerableConverterRule Create()
        {
            return (ClrDataCursorToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrDataCursorConvention.Instance,
                    ClrEnumerableConvention.Instance,
                    "ClrDataCursorToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorToClrEnumerableConverterRule>(c => new ClrDataCursorToClrEnumerableConverterRule(c)))
                .toRule(typeof(ClrDataCursorToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorToClrEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override bool isGuaranteed() => true;

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            // simplified, because that is the trait set of the subset the input is registered in: RelSet.add
            // simplifies a rel's traits before choosing its subset, so a merge join carrying two collations
            // sits in the subset carrying none, and a converter claiming both over that subset is a claim its
            // input does not keep. RelOptRule.convert simplifies for the same reason.
            return new ClrDataCursorToClrEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify(),
                rel);
        }

    }

}
