using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="Combine"/> to a <see cref="ClrAsyncEnumerableCombine"/>.
    /// </summary>
    public class ClrAsyncEnumerableCombineRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableCombineRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableCombineRule Create()
        {
            return (ClrAsyncEnumerableCombineRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Combine), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableCombineRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableCombineRule>(c => new ClrAsyncEnumerableCombineRule(c)))
                .toRule(typeof(ClrAsyncEnumerableCombineRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableCombineRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var combine = (Combine)rel;
            var traitSet = combine.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance);

            return new ClrAsyncEnumerableCombine(
                combine.getCluster(),
                traitSet,
                convertList(combine.getInputs(), ClrAsyncEnumerableConvention.Instance));
        }

    }

}
