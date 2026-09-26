using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts an <c>EnumerableConvention</c> node to a <see cref="ClrDataCursorConvention"/> one.
    /// </summary>
    public class EnumerableToClrDataCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates an <see cref="EnumerableToClrDataCursorConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static EnumerableToClrDataCursorConverterRule Create()
        {
            return (EnumerableToClrDataCursorConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    EnumerableConvention.INSTANCE,
                    ClrDataCursorConvention.Instance,
                    "EnumerableToClrDataCursorConverterRule")
                .withRuleFactory(new DelegateFunction<Config, EnumerableToClrDataCursorConverterRule>(c => new EnumerableToClrDataCursorConverterRule(c)))
                .toRule(typeof(EnumerableToClrDataCursorConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public EnumerableToClrDataCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// <see langword="true"/>, because <see cref="convert"/> accepts any node of the input convention.
        /// Guaranteed is what puts the rule into <c>ConventionTraitDef</c>'s conversion graph, which is the
        /// only route a conversion has when its input is itself a <c>Converter</c>.
        /// </remarks>
        public override bool isGuaranteed() => true;

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            // simplified, because that is the trait set of the subset the input is registered in: RelSet.add
            // simplifies a rel's traits before choosing its subset, so a merge join carrying two collations
            // sits in the subset carrying none, and a converter claiming both over that subset is a claim its
            // input does not keep. RelOptRule.convert simplifies for the same reason.
            return new EnumerableToClrDataCursorConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrDataCursorConvention.Instance).simplify(),
                rel);
        }

    }

}
