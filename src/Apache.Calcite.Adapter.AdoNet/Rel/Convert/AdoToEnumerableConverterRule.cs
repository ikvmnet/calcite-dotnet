using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that puts an <see cref="AdoToEnumerableConverter"/> over a node of an <see cref="AdoConvention"/>,
    /// so that Calcite's <see cref="EnumerableConvention"/> can read its rows. Mirrors
    /// <c>JdbcToEnumerableConverterRule</c>.
    /// </summary>
    public class AdoToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted from.</param>
        /// <returns>The rule.</returns>
        public static AdoToEnumerableConverterRule Create(AdoConvention convention)
        {
            return (AdoToEnumerableConverterRule)Config.INSTANCE
                .withConversion(typeof(RelNode), convention, EnumerableConvention.INSTANCE, "AdoToEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoToEnumerableConverterRule>(c => new AdoToEnumerableConverterRule(c)))
                .toRule(typeof(AdoToEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
        public AdoToEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <summary>
        /// Returns an <see cref="AdoToEnumerableConverter"/> over <paramref name="rel"/>.
        /// </summary>
        /// <param name="rel">The node of the <see cref="AdoConvention"/>.</param>
        /// <returns>The converter.</returns>
        public override RelNode? convert(RelNode rel)
        {
            return new AdoToEnumerableConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
        }

    }

}
