using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts an <c>EnumerableConvention</c> node to a
    /// <see cref="ClrAsyncEnumerableConvention"/> one.
    /// </summary>
    public class EnumerableToClrAsyncEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates an <see cref="EnumerableToClrAsyncEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static EnumerableToClrAsyncEnumerableConverterRule Create()
        {
            return (EnumerableToClrAsyncEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    EnumerableConvention.INSTANCE,
                    ClrAsyncEnumerableConvention.Instance,
                    "EnumerableToClrAsyncEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, EnumerableToClrAsyncEnumerableConverterRule>(c => new EnumerableToClrAsyncEnumerableConverterRule(c)))
                .toRule(typeof(EnumerableToClrAsyncEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public EnumerableToClrAsyncEnumerableConverterRule(Config config) :
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
            return new EnumerableToClrAsyncEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance),
                rel);
        }

    }

}
