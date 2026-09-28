using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a node of <see cref="ClrCursorConvention"/> to <c>EnumerableConvention</c> by
    /// placing a <see cref="ClrCursorToEnumerableConverter"/> over it.
    /// </summary>
    public class ClrCursorToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The rule's configuration.</param>
        public ClrCursorToEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            // the traits are simplified because RelSet.add registers the input in the subset of its
            // simplified traits: a node with two collations sits in a subset with none, and a converter
            // claiming both would claim an order its input does not guarantee. RelOptRule.convert simplifies
            // for the same reason.
            return new ClrCursorToEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify(),
                rel);
        }

    }

}
