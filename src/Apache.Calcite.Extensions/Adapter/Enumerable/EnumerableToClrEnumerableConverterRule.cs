using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts an <c>EnumerableConvention</c> node to a <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    public class EnumerableToClrEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates an <see cref="EnumerableToClrEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static EnumerableToClrEnumerableConverterRule Create()
        {
            return (EnumerableToClrEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    EnumerableConvention.INSTANCE,
                    ClrEnumerableConvention.Instance,
                    "EnumerableToClrEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, EnumerableToClrEnumerableConverterRule>(c => new EnumerableToClrEnumerableConverterRule(c)))
                .toRule(typeof(EnumerableToClrEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public EnumerableToClrEnumerableConverterRule(Config config) :
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
            return new EnumerableToClrEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrEnumerableConvention.Instance),
                rel);
        }

    }

}
