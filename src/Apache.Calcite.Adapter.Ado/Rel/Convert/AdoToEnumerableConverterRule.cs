using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    /// <summary>
    /// Rule to convert a relational expression from <see cref="AdoConvention"/> to <see cref="EnumerableConvention"/>.
    /// </summary>
    public class AdoToEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
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
        /// <param name="config"></param>
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
