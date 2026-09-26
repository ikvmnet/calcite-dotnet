using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="ClrDataCursorConvention"/> node to an <c>EnumerableConvention</c> one.
    /// </summary>
    public class ClrDataCursorToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorToEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorToEnumerableConverterRule Create()
        {
            return (ClrDataCursorToEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrDataCursorConvention.Instance,
                    EnumerableConvention.INSTANCE,
                    "ClrDataCursorToEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorToEnumerableConverterRule>(c => new ClrDataCursorToEnumerableConverterRule(c)))
                .toRule(typeof(ClrDataCursorToEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorToEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            // simplified, because that is the trait set of the subset the input is registered in: RelSet.add
            // simplifies a rel's traits before choosing its subset, so a merge join carrying two collations
            // sits in the subset carrying none, and a converter claiming both over that subset is a claim its
            // input does not keep. RelOptRule.convert simplifies for the same reason.
            return new ClrDataCursorToEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify(),
                rel);
        }

    }

}
