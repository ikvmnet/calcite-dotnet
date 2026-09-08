using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;

using java.util.function;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule to convert a relational expression from <see cref="AdoConvention"/> to
    /// <see cref="ClrAsyncEnumerableConvention"/>.
    /// </summary>
    /// <remarks>
    /// The third of the adapter's converter rules, beside <see cref="AdoToEnumerableConverterRule"/> and
    /// <see cref="AdoToClrEnumerableConverterRule"/>, so that a plan whose root is asked for in any of the
    /// three conventions has a route out of the adapter. A plan ending in
    /// <see cref="ClrAsyncEnumerableConvention"/> reaches it without a second converter, and reads its rows
    /// without blocking a thread on the provider.
    /// </remarks>
    public class AdoToClrAsyncEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention whose nodes will be converted.</param>
        /// <returns></returns>
        public static AdoToClrAsyncEnumerableConverterRule Create(AdoConvention convention)
        {
            return (AdoToClrAsyncEnumerableConverterRule)Config.INSTANCE
                .withConversion(typeof(RelNode), convention, ClrAsyncEnumerableConvention.Instance, "AdoToClrAsyncEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoToClrAsyncEnumerableConverterRule>(c => new AdoToClrAsyncEnumerableConverterRule(c)))
                .toRule(typeof(AdoToClrAsyncEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoToClrAsyncEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new AdoToClrAsyncEnumerableConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
        }

    }

}
