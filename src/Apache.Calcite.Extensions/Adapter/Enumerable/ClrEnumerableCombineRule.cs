using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="Combine"/> to a <see cref="ClrEnumerableCombine"/>.
    /// </summary>
    public class ClrEnumerableCombineRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCombineRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableCombineRule Create()
        {
            return (ClrEnumerableCombineRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(Combine), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableCombineRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableCombineRule>(c => new ClrEnumerableCombineRule(c)))
                .toRule(typeof(ClrEnumerableCombineRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableCombineRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var combine = (Combine)rel;
            var traitSet = combine.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableCombine(
                combine.getCluster(),
                traitSet,
                convertList(combine.getInputs(), ClrEnumerableConvention.Instance));
        }

    }

}
