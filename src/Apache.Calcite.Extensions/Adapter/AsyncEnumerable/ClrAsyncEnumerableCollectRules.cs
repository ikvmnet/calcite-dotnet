using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="Collect"/> to a <see cref="ClrAsyncEnumerableCollect"/>.
    /// </summary>
    public class ClrAsyncEnumerableCollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableCollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableCollectRule Create()
        {
            return (ClrAsyncEnumerableCollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Collect), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableCollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableCollectRule>(c => new ClrAsyncEnumerableCollectRule(c)))
                .toRule(typeof(ClrAsyncEnumerableCollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableCollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var collect = (Collect)rel;
            var input = collect.getInput();

            return ClrAsyncEnumerableCollect.Create(
                convert(input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                collect.getRowType());
        }

    }

    /// <summary>
    /// Rule that converts an <see cref="Uncollect"/> to a <see cref="ClrAsyncEnumerableUncollect"/>.
    /// </summary>
    public class ClrAsyncEnumerableUncollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableUncollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableUncollectRule Create()
        {
            return (ClrAsyncEnumerableUncollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Uncollect), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableUncollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableUncollectRule>(c => new ClrAsyncEnumerableUncollectRule(c)))
                .toRule(typeof(ClrAsyncEnumerableUncollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableUncollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var uncollect = (Uncollect)rel;
            var traitSet = uncollect.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance);
            var input = uncollect.getInput();
            var newInput = convert(input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance));

            return ClrAsyncEnumerableUncollect.Create(traitSet, newInput, uncollect.withOrdinality);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalSort"/> carrying a fetch to a
    /// <see cref="ClrAsyncEnumerableLimitSort"/>.
    /// </summary>
    /// <remarks>
    /// A sort and a limit together read only as many rows as are wanted, where a sort followed by a limit
    /// orders everything first.
    /// </remarks>
    public class ClrAsyncEnumerableLimitSortRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableLimitSortRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableLimitSortRule Create()
        {
            var config = EnumerableLimitSortRule.Config.DEFAULT.withDescription("ClrAsyncEnumerableLimitSortRule");

            return new ClrAsyncEnumerableLimitSortRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableLimitSortRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            var input = sort.getInput();

            call.transformTo(
                ClrAsyncEnumerableLimitSort.Create(
                    convert(call.getPlanner(), input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch));
        }

    }

}
