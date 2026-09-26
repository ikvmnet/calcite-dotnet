using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="ClrCursorConvention"/> node to an <c>EnumerableConvention</c> one.
    /// </summary>
    public class ClrCursorToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorToEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorToEnumerableConverterRule Create()
        {
            return (ClrCursorToEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrCursorConvention.Instance,
                    EnumerableConvention.INSTANCE,
                    "ClrCursorToEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorToEnumerableConverterRule>(c => new ClrCursorToEnumerableConverterRule(c)))
                .toRule(typeof(ClrCursorToEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorToEnumerableConverterRule(Config config) :
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
            return new ClrCursorToEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify(),
                rel);
        }

    }

}
