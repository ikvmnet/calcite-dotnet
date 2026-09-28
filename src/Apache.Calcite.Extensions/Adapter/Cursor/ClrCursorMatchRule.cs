using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalMatch"/> to a <see cref="ClrCursorMatch"/>.
    /// </summary>
    public class ClrCursorMatchRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorMatchRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorMatchRule Create()
        {
            return (ClrCursorMatchRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalMatch), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorMatchRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorMatchRule>(c => new ClrCursorMatchRule(c)))
                .toRule(typeof(ClrCursorMatchRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorMatchRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var match = (Match)rel;

            return ClrCursorMatch.Create(
                RelOptRule.convert(match.getInput(), match.getInput().getTraitSet().replace(ClrCursorConvention.Instance)),
                match.getRowType(),
                match.getPattern(),
                match.isStrictStart(),
                match.isStrictEnd(),
                match.getPatternDefinitions(),
                match.getMeasures(),
                match.getAfter(),
                match.getSubsets(),
                match.isAllRows(),
                match.getPartitionKeys(),
                match.getOrderKeys(),
                match.getInterval());
        }

    }

}
