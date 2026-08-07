using Apache.Calcite.Extensions;

using java.util.function;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule to convert a relational expression from <see cref="AdoConvention"/> to
    /// <see cref="ClrEnumerableConvention"/>.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="AdoToEnumerableConverterRule"/>. Both are registered, so a plan whose
    /// root is asked for in either convention has a route out of the adapter; which one a query uses is the
    /// planner's choice, and a plan ending in <see cref="ClrEnumerableConvention"/> reaches it without a
    /// second converter.
    /// </remarks>
    public class AdoToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention whose nodes will be converted.</param>
        /// <returns></returns>
        public static AdoToClrEnumerableConverterRule Create(AdoConvention convention)
        {
            return (AdoToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(typeof(RelNode), convention, ClrEnumerableConvention.Instance, "AdoToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoToClrEnumerableConverterRule>(c => new AdoToClrEnumerableConverterRule(c)))
                .toRule(typeof(AdoToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoToClrEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            return new AdoToClrEnumerableConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
        }

    }

}
