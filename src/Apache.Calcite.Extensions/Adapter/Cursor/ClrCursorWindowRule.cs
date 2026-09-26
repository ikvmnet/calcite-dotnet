using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalWindow"/> to a <see cref="ClrCursorWindow"/>.
    /// </summary>
    public class ClrCursorWindowRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorWindowRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorWindowRule Create()
        {
            return (ClrCursorWindowRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalWindow), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorWindowRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorWindowRule>(c => new ClrCursorWindowRule(c)))
                .toRule(typeof(ClrCursorWindowRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorWindowRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var window = (Window)rel;
            var traitSet = window.getTraitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorWindow(
                window.getCluster(),
                traitSet,
                RelOptRule.convert(window.getInput(), window.getInput().getTraitSet().replace(ClrCursorConvention.Instance)),
                window.getConstants(),
                window.getRowType(),
                window.groups);
        }

    }

}
