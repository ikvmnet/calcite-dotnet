using java.util.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="ClrEnumerableConvention"/> node to a
    /// <see cref="ClrAsyncEnumerableConvention"/> one.
    /// </summary>
    public class ClrEnumerableToClrAsyncEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableToClrAsyncEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableToClrAsyncEnumerableConverterRule Create()
        {
            return (ClrEnumerableToClrAsyncEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrEnumerableConvention.Instance,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrEnumerableToClrAsyncEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableToClrAsyncEnumerableConverterRule>(c => new ClrEnumerableToClrAsyncEnumerableConverterRule(c)))
                .toRule(typeof(ClrEnumerableToClrAsyncEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableToClrAsyncEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new ClrEnumerableToClrAsyncEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance),
                rel);
        }

    }

}
