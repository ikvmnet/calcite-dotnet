using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="Combine"/> to a <see cref="ClrCursorCombine"/>.
    /// </summary>
    public class ClrCursorCombineRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCombineRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorCombineRule Create()
        {
            return (ClrCursorCombineRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Combine), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorCombineRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorCombineRule>(c => new ClrCursorCombineRule(c)))
                .toRule(typeof(ClrCursorCombineRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorCombineRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var combine = (Combine)rel;
            var traitSet = combine.getTraitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorCombine(
                combine.getCluster(),
                traitSet,
                convertList(combine.getInputs(), ClrCursorConvention.Instance));
        }

    }

}
