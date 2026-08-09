using java.util.function;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="ClrAsyncEnumerableConvention"/> node to a
    /// <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    public class ClrAsyncEnumerableToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableToClrEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableToClrEnumerableConverterRule Create()
        {
            return (ClrAsyncEnumerableToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrAsyncEnumerableConvention.Instance,
                    ClrEnumerableConvention.Instance,
                    "ClrAsyncEnumerableToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableToClrEnumerableConverterRule>(c => new ClrAsyncEnumerableToClrEnumerableConverterRule(c)))
                .toRule(typeof(ClrAsyncEnumerableToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableToClrEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new ClrAsyncEnumerableToClrEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance),
                rel);
        }

    }

}
