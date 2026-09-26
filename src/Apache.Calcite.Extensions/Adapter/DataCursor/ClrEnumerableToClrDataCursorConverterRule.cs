using java.util.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="ClrEnumerableConvention"/> node to a
    /// <see cref="ClrDataCursorConvention"/> one.
    /// </summary>
    public class ClrEnumerableToClrDataCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableToClrDataCursorConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableToClrDataCursorConverterRule Create()
        {
            return (ClrEnumerableToClrDataCursorConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrEnumerableConvention.Instance,
                    ClrDataCursorConvention.Instance,
                    "ClrEnumerableToClrDataCursorConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableToClrDataCursorConverterRule>(c => new ClrEnumerableToClrDataCursorConverterRule(c)))
                .toRule(typeof(ClrEnumerableToClrDataCursorConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableToClrDataCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// <see langword="true"/>, for the reason <c>EnumerableToClrDataCursorConverterRule</c> gives: it is
        /// what puts the rule into <c>ConventionTraitDef</c>'s conversion graph, which is the only route a
        /// conversion has when its input is itself a converter.
        /// </remarks>
        public override bool isGuaranteed() => true;

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            // simplified, because that is the trait set of the subset the input is registered in: RelSet.add
            // simplifies a rel's traits before choosing its subset, so a merge join carrying two collations
            // sits in the subset carrying none, and a converter claiming both over that subset is a claim its
            // input does not keep. RelOptRule.convert simplifies for the same reason.
            return new ClrEnumerableToClrDataCursorConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrDataCursorConvention.Instance).simplify(),
                rel);
        }

    }

}
