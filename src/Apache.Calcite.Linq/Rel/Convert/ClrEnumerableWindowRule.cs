using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalWindow"/> to a <see cref="ClrEnumerableWindow"/>.
    /// </summary>
    public class ClrEnumerableWindowRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableWindowRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableWindowRule Create()
        {
            return (ClrEnumerableWindowRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalWindow), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableWindowRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableWindowRule>(c => new ClrEnumerableWindowRule(c)))
                .toRule(typeof(ClrEnumerableWindowRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableWindowRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var window = (Window)rel;
            var traitSet = window.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableWindow(
                window.getCluster(),
                traitSet,
                RelOptRule.convert(window.getInput(), window.getInput().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                window.getConstants(),
                window.getRowType(),
                window.groups);
        }

    }

}
