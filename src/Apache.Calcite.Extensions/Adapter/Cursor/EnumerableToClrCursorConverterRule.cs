using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a node of <c>EnumerableConvention</c> to <see cref="ClrCursorConvention"/> by placing an
    /// <see cref="EnumerableToClrCursorConverter"/> over it.
    /// </summary>
    public class EnumerableToClrCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
        public static EnumerableToClrCursorConverterRule Create()
        {
            return (EnumerableToClrCursorConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    EnumerableConvention.INSTANCE,
                    ClrCursorConvention.Instance,
                    "EnumerableToClrCursorConverterRule")
                .withRuleFactory(new DelegateFunction<Config, EnumerableToClrCursorConverterRule>(c => new EnumerableToClrCursorConverterRule(c)))
                .toRule(typeof(EnumerableToClrCursorConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule's configuration.</param>
        public EnumerableToClrCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// Returns <see langword="true"/>, because <see cref="convert"/> accepts any node of
        /// <c>EnumerableConvention</c>. A guaranteed rule is registered in <c>ConventionTraitDef</c>'s
        /// conversion graph.
        /// </remarks>
        public override bool isGuaranteed() => true;

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            // the traits are simplified because RelSet.add registers the input in the subset of its
            // simplified traits: a node with two collations sits in a subset with none, and a converter
            // claiming both would claim an order its input does not guarantee. RelOptRule.convert simplifies
            // for the same reason.
            return new EnumerableToClrCursorConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrCursorConvention.Instance).simplify(),
                rel);
        }

    }

}
