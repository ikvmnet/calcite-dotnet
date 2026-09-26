using java.util.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="ClrCursorConvention"/> node to a
    /// <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    public class ClrCursorToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorToClrEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorToClrEnumerableConverterRule Create()
        {
            return (ClrCursorToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrCursorConvention.Instance,
                    ClrEnumerableConvention.Instance,
                    "ClrCursorToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorToClrEnumerableConverterRule>(c => new ClrCursorToClrEnumerableConverterRule(c)))
                .toRule(typeof(ClrCursorToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorToClrEnumerableConverterRule(Config config) :
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
            return new ClrCursorToClrEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance).simplify(),
                rel);
        }

    }

}
