using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalFilter"/> to a <see cref="ClrAsyncEnumerableFilter"/>.
    /// </summary>
    public class ClrAsyncEnumerableFilterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableFilterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableFilterRule Create()
        {
            return (ClrAsyncEnumerableFilterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalFilter),
                    new DelegatePredicate<LogicalFilter>(f => f.containsOver() == false && RexUtil.SubQueryFinder.containsSubQuery(f) == false),
                    Convention.NONE,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrAsyncEnumerableFilterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableFilterRule>(c => new ClrAsyncEnumerableFilterRule(c)))
                .toRule(typeof(ClrAsyncEnumerableFilterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableFilterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var filter = (Filter)rel;

            return new ClrAsyncEnumerableFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance),
                convert(filter.getInput(), filter.getInput().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                filter.getCondition());
        }

    }

}
