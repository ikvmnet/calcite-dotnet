using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalWindow"/> to a <see cref="ClrAsyncEnumerableWindow"/>.
    /// </summary>
    public class ClrAsyncEnumerableWindowRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableWindowRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableWindowRule Create()
        {
            return (ClrAsyncEnumerableWindowRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalWindow), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableWindowRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableWindowRule>(c => new ClrAsyncEnumerableWindowRule(c)))
                .toRule(typeof(ClrAsyncEnumerableWindowRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableWindowRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var window = (Window)rel;
            var traitSet = window.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance);

            return new ClrAsyncEnumerableWindow(
                window.getCluster(),
                traitSet,
                RelOptRule.convert(window.getInput(), window.getInput().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                window.getConstants(),
                window.getRowType(),
                window.groups);
        }

    }

}
