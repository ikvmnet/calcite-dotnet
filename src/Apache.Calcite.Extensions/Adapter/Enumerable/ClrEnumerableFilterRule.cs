using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalFilter"/> to a <see cref="ClrEnumerableFilter"/>.
    /// </summary>
    public class ClrEnumerableFilterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableFilterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableFilterRule Create()
        {
            return (ClrEnumerableFilterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalFilter),
                    new DelegatePredicate<LogicalFilter>(f => f.containsOver() == false && RexUtil.SubQueryFinder.containsSubQuery(f) == false),
                    Convention.NONE,
                    ClrEnumerableConvention.Instance,
                    "ClrEnumerableFilterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableFilterRule>(c => new ClrEnumerableFilterRule(c)))
                .toRule(typeof(ClrEnumerableFilterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableFilterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var filter = (Filter)rel;

            return new ClrEnumerableFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance),
                convert(filter.getInput(), filter.getInput().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                filter.getCondition());
        }

    }

}
