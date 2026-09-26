using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalFilter"/> to a <see cref="ClrCursorFilter"/>.
    /// </summary>
    public class ClrCursorFilterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorFilterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorFilterRule Create()
        {
            return (ClrCursorFilterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalFilter),
                    new DelegatePredicate<LogicalFilter>(f => f.containsOver() == false && RexUtil.SubQueryFinder.containsSubQuery(f) == false),
                    Convention.NONE,
                    ClrCursorConvention.Instance,
                    "ClrCursorFilterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorFilterRule>(c => new ClrCursorFilterRule(c)))
                .toRule(typeof(ClrCursorFilterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorFilterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var filter = (Filter)rel;

            return new ClrCursorFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrCursorConvention.Instance),
                convert(filter.getInput(), filter.getInput().getTraitSet().replace(ClrCursorConvention.Instance)),
                filter.getCondition());
        }

    }

}
