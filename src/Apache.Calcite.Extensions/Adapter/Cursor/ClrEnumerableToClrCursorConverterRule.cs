using java.util.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="ClrEnumerableConvention"/> node to a
    /// <see cref="ClrCursorConvention"/> one.
    /// </summary>
    public class ClrEnumerableToClrCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableToClrCursorConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableToClrCursorConverterRule Create()
        {
            return (ClrEnumerableToClrCursorConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrEnumerableConvention.Instance,
                    ClrCursorConvention.Instance,
                    "ClrEnumerableToClrCursorConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableToClrCursorConverterRule>(c => new ClrEnumerableToClrCursorConverterRule(c)))
                .toRule(typeof(ClrEnumerableToClrCursorConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableToClrCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// <see langword="true"/>, for the reason <c>EnumerableToClrCursorConverterRule</c> gives: it is
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
            return new ClrEnumerableToClrCursorConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrCursorConvention.Instance).simplify(),
                rel);
        }

    }

}
