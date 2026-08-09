using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts an <c>EnumerableConvention</c> node to a <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    public class EnumerableToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates an <see cref="EnumerableToClrEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static EnumerableToClrEnumerableConverterRule Create()
        {
            return (EnumerableToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    EnumerableConvention.INSTANCE,
                    ClrEnumerableConvention.Instance,
                    "EnumerableToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, EnumerableToClrEnumerableConverterRule>(c => new EnumerableToClrEnumerableConverterRule(c)))
                .toRule(typeof(EnumerableToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public EnumerableToClrEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new EnumerableToClrEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance),
                rel);
        }

    }

}
