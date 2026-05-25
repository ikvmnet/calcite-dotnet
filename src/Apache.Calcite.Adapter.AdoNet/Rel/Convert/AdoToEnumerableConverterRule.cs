using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule to convert a relational expression from <see cref="AdoConvention"/> to <see cref="EnumerableConvention"/>.
    /// </summary>
    public class AdoToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention whose nodes will be converted to <see cref="EnumerableConvention"/>.</param>
        /// <returns>A configured <see cref="AdoToEnumerableConverterRule"/> instance.</returns>
        public static AdoToEnumerableConverterRule Create(AdoConvention convention)
        {
            return (AdoToEnumerableConverterRule)Config.INSTANCE
                .withConversion(typeof(RelNode), convention, EnumerableConvention.INSTANCE, "AdoToEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoToEnumerableConverterRule>(c => new AdoToEnumerableConverterRule(c)))
                .toRule(typeof(AdoToEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoToEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            return new AdoToEnumerableConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
        }

    }

}
