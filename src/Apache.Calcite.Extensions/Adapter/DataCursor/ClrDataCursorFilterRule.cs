using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalFilter"/> to a <see cref="ClrDataCursorFilter"/>.
    /// </summary>
    public class ClrDataCursorFilterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorFilterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorFilterRule Create()
        {
            return (ClrDataCursorFilterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalFilter),
                    new DelegatePredicate<LogicalFilter>(f => f.containsOver() == false && RexUtil.SubQueryFinder.containsSubQuery(f) == false),
                    Convention.NONE,
                    ClrDataCursorConvention.Instance,
                    "ClrDataCursorFilterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorFilterRule>(c => new ClrDataCursorFilterRule(c)))
                .toRule(typeof(ClrDataCursorFilterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorFilterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var filter = (Filter)rel;

            return new ClrDataCursorFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrDataCursorConvention.Instance),
                convert(filter.getInput(), filter.getInput().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                filter.getCondition());
        }

    }

}
