using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="Combine"/> to a <see cref="ClrDataCursorCombine"/>.
    /// </summary>
    public class ClrDataCursorCombineRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorCombineRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorCombineRule Create()
        {
            return (ClrDataCursorCombineRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Combine), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorCombineRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorCombineRule>(c => new ClrDataCursorCombineRule(c)))
                .toRule(typeof(ClrDataCursorCombineRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorCombineRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var combine = (Combine)rel;
            var traitSet = combine.getTraitSet().replace(ClrDataCursorConvention.Instance);

            return new ClrDataCursorCombine(
                combine.getCluster(),
                traitSet,
                convertList(combine.getInputs(), ClrDataCursorConvention.Instance));
        }

    }

}
