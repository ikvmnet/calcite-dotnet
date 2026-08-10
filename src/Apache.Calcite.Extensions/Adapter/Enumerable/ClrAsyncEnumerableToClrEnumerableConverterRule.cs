using java.util.function;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="ClrAsyncEnumerableConvention"/> node to a
    /// <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    public class ClrAsyncEnumerableToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableToClrEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableToClrEnumerableConverterRule Create()
        {
            return (ClrAsyncEnumerableToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrAsyncEnumerableConvention.Instance,
                    ClrEnumerableConvention.Instance,
                    "ClrAsyncEnumerableToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableToClrEnumerableConverterRule>(c => new ClrAsyncEnumerableToClrEnumerableConverterRule(c)))
                .toRule(typeof(ClrAsyncEnumerableToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableToClrEnumerableConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// <see langword="true"/>, because <see cref="convert"/> accepts any node of the input convention.
        /// Guaranteed is what puts the rule into <c>ConventionTraitDef</c>'s conversion graph, which is the
        /// only route a conversion has when its input is itself a <c>Converter</c>: Calcite's
        /// <c>ConverterRelOptRuleOperand</c> refuses to stack a converter on a converter of the same trait
        /// def, and expects the abstract-converter expansion to walk this graph instead. Without this, a
        /// plan whose only node in this rule's input convention is a converter cannot be completed at all.
        /// </remarks>
        public override bool isGuaranteed() => true;

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new ClrAsyncEnumerableToClrEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance),
                rel);
        }

    }

}
