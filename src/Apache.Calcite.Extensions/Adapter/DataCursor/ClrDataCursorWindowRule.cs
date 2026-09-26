using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalWindow"/> to a <see cref="ClrDataCursorWindow"/>.
    /// </summary>
    public class ClrDataCursorWindowRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorWindowRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorWindowRule Create()
        {
            return (ClrDataCursorWindowRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalWindow), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorWindowRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorWindowRule>(c => new ClrDataCursorWindowRule(c)))
                .toRule(typeof(ClrDataCursorWindowRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorWindowRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var window = (Window)rel;
            var traitSet = window.getTraitSet().replace(ClrDataCursorConvention.Instance);

            return new ClrDataCursorWindow(
                window.getCluster(),
                traitSet,
                RelOptRule.convert(window.getInput(), window.getInput().getTraitSet().replace(ClrDataCursorConvention.Instance)),
                window.getConstants(),
                window.getRowType(),
                window.groups);
        }

    }

}
