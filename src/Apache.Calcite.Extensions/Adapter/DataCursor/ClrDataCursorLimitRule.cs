using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> carrying an offset or a fetch to a
    /// <see cref="ClrDataCursorLimit"/>, and a <see cref="ClrDataCursorSort"/> for its ordering.
    /// </summary>
    public class ClrDataCursorLimitRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorLimitRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorLimitRule Create()
        {
            // the operand is the one Calcite matches on, taken from its rule rather than restated; only the
            // description differs, because two rules cannot share one
            var config = EnumerableLimitRule.Config.DEFAULT.withDescription("ClrDataCursorLimitRule");

            return new ClrDataCursorLimitRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorLimitRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            if (sort.offset == null && sort.fetch == null)
                return;

            var input = sort.getInput();

            // the ordering stays a sort of its own, with the offset and the fetch lifted off it
            if (sort.getCollation().getFieldCollations().isEmpty() == false)
                input = sort.copy(sort.getTraitSet(), input, sort.getCollation(), null, null);

            call.transformTo(
                ClrDataCursorLimit.Create(
                    convert(call.getPlanner(), input, input.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                    sort.offset,
                    sort.fetch));
        }

    }

}
