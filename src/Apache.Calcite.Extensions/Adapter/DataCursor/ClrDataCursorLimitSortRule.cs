using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalSort"/> carrying a fetch to a
    /// <see cref="ClrDataCursorLimitSort"/>.
    /// </summary>
    /// <remarks>
    /// A sort and a limit together read only as many rows as are wanted, where a sort followed by a limit
    /// orders everything first.
    /// </remarks>
    public class ClrDataCursorLimitSortRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorLimitSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorLimitSortRule Create()
        {
            var config = EnumerableLimitSortRule.Config.DEFAULT.withDescription("ClrDataCursorLimitSortRule");

            return new ClrDataCursorLimitSortRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorLimitSortRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            var input = sort.getInput();

            call.transformTo(
                ClrDataCursorLimitSort.Create(
                    convert(call.getPlanner(), input, input.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch));
        }

    }

}
