using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="Collect"/> to a <see cref="ClrDataCursorCollect"/>.
    /// </summary>
    public class ClrDataCursorCollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorCollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorCollectRule Create()
        {
            return (ClrDataCursorCollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Collect), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorCollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorCollectRule>(c => new ClrDataCursorCollectRule(c)))
                .toRule(typeof(ClrDataCursorCollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorCollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var collect = (Collect)rel;
            var input = collect.getInput();

            return ClrDataCursorCollect.Create(
                convert(input, input.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                collect.getRowType());
        }

    }

    /// <summary>
    /// Rule that converts an <see cref="Uncollect"/> to a <see cref="ClrDataCursorUncollect"/>.
    /// </summary>
    public class ClrDataCursorUncollectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorUncollectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorUncollectRule Create()
        {
            return (ClrDataCursorUncollectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Uncollect), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorUncollectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorUncollectRule>(c => new ClrDataCursorUncollectRule(c)))
                .toRule(typeof(ClrDataCursorUncollectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorUncollectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var uncollect = (Uncollect)rel;
            var traitSet = uncollect.getTraitSet().replace(ClrDataCursorConvention.Instance);
            var input = uncollect.getInput();
            var newInput = convert(input, input.getTraitSet().replace(ClrDataCursorConvention.Instance));

            return ClrDataCursorUncollect.Create(traitSet, newInput, uncollect.withOrdinality, uncollect.expandStructFields, uncollect.isOuter);
        }

    }

}
