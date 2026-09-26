using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="Collect"/> to a <see cref="ClrCursorCollect"/>.
    /// </summary>
    public class ClrCursorCollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorCollectRule Create()
        {
            return (ClrCursorCollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Collect), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorCollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorCollectRule>(c => new ClrCursorCollectRule(c)))
                .toRule(typeof(ClrCursorCollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorCollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var collect = (Collect)rel;
            var input = collect.getInput();

            return ClrCursorCollect.Create(
                convert(input, input.getTraitSet().replace(ClrCursorConvention.Instance)),
                collect.getRowType());
        }

    }

    /// <summary>
    /// Rule that converts an <see cref="Uncollect"/> to a <see cref="ClrCursorUncollect"/>.
    /// </summary>
    public class ClrCursorUncollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorUncollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorUncollectRule Create()
        {
            return (ClrCursorUncollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Uncollect), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorUncollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorUncollectRule>(c => new ClrCursorUncollectRule(c)))
                .toRule(typeof(ClrCursorUncollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorUncollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var uncollect = (Uncollect)rel;
            var traitSet = uncollect.getTraitSet().replace(ClrCursorConvention.Instance);
            var input = uncollect.getInput();
            var newInput = convert(input, input.getTraitSet().replace(ClrCursorConvention.Instance));

            return ClrCursorUncollect.Create(traitSet, newInput, uncollect.withOrdinality, uncollect.expandStructFields, uncollect.isOuter);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalSort"/> carrying a fetch to a
    /// <see cref="ClrCursorLimitSort"/>.
    /// </summary>
    /// <remarks>
    /// A sort and a limit together read only as many rows as are wanted, where a sort followed by a limit
    /// orders everything first.
    /// </remarks>
    public class ClrCursorLimitSortRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorLimitSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorLimitSortRule Create()
        {
            var config = EnumerableLimitSortRule.Config.DEFAULT.withDescription("ClrCursorLimitSortRule");

            return new ClrCursorLimitSortRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorLimitSortRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            var input = sort.getInput();

            call.transformTo(
                ClrCursorLimitSort.Create(
                    convert(call.getPlanner(), input, input.getTraitSet().replace(ClrCursorConvention.Instance)),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch));
        }

    }

}
