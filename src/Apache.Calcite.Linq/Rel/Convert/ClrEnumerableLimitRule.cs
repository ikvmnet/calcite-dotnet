using Apache.Calcite.Linq.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="Sort"/> carrying an offset or a fetch to a
    /// <see cref="ClrEnumerableLimit"/>, and a <see cref="ClrEnumerableSort"/> for its ordering.
    /// </summary>
    public class ClrEnumerableLimitRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableLimitRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableLimitRule Create()
        {
            // the operand is the one Calcite matches on, taken from its rule rather than restated; only the
            // description differs, because two rules cannot share one
            var config = EnumerableLimitRule.Config.DEFAULT.withDescription("ClrEnumerableLimitRule");

            return new ClrEnumerableLimitRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableLimitRule(RelRule.Config config) :
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
                ClrEnumerableLimit.Create(
                    convert(call.getPlanner(), input, input.getTraitSet().replace(ClrEnumerableConvention.Instance)),
                    sort.offset,
                    sort.fetch));
        }

    }

}
