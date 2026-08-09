using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="Collect"/> to a <see cref="ClrEnumerableCollect"/>.
    /// </summary>
    public class ClrEnumerableCollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableCollectRule Create()
        {
            return (ClrEnumerableCollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Collect), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableCollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableCollectRule>(c => new ClrEnumerableCollectRule(c)))
                .toRule(typeof(ClrEnumerableCollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableCollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var collect = (Collect)rel;
            var input = collect.getInput();

            return ClrEnumerableCollect.Create(
                convert(input, input.getTraitSet().replace(ClrEnumerableConvention.Instance)),
                collect.getRowType());
        }

    }

    /// <summary>
    /// Rule that converts an <see cref="Uncollect"/> to a <see cref="ClrEnumerableUncollect"/>.
    /// </summary>
    public class ClrEnumerableUncollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableUncollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableUncollectRule Create()
        {
            return (ClrEnumerableUncollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Uncollect), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableUncollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableUncollectRule>(c => new ClrEnumerableUncollectRule(c)))
                .toRule(typeof(ClrEnumerableUncollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableUncollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var uncollect = (Uncollect)rel;
            var traitSet = uncollect.getTraitSet().replace(ClrEnumerableConvention.Instance);
            var input = uncollect.getInput();
            var newInput = convert(input, input.getTraitSet().replace(ClrEnumerableConvention.Instance));

            return ClrEnumerableUncollect.Create(traitSet, newInput, uncollect.withOrdinality);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalSort"/> carrying a fetch to a
    /// <see cref="ClrEnumerableLimitSort"/>.
    /// </summary>
    /// <remarks>
    /// A sort and a limit together read only as many rows as are wanted, where a sort followed by a limit
    /// orders everything first.
    /// </remarks>
    public class ClrEnumerableLimitSortRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableLimitSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableLimitSortRule Create()
        {
            var config = EnumerableLimitSortRule.Config.DEFAULT.withDescription("ClrEnumerableLimitSortRule");

            return new ClrEnumerableLimitSortRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableLimitSortRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            var input = sort.getInput();

            call.transformTo(
                ClrEnumerableLimitSort.Create(
                    convert(call.getPlanner(), input, input.getTraitSet().replace(ClrEnumerableConvention.Instance)),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch));
        }

    }

}
