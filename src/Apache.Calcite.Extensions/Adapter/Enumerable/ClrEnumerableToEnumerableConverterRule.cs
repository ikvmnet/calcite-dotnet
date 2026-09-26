using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="ClrEnumerableConvention"/> node to an <c>EnumerableConvention</c> one.
    /// </summary>
    public class ClrEnumerableToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableToEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableToEnumerableConverterRule Create()
        {
            return (ClrEnumerableToEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrEnumerableConvention.Instance,
                    EnumerableConvention.INSTANCE,
                    "ClrEnumerableToEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableToEnumerableConverterRule>(c => new ClrEnumerableToEnumerableConverterRule(c)))
                .toRule(typeof(ClrEnumerableToEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableToEnumerableConverterRule(Config config) :
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
            return new ClrEnumerableToEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify(),
                rel);
        }

    }

}
