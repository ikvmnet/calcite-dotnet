using java.util.function;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="ClrEnumerableConvention"/> node to a
    /// <see cref="ClrAsyncEnumerableConvention"/> one.
    /// </summary>
    public class ClrEnumerableToClrAsyncEnumerableConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableToClrAsyncEnumerableConverterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableToClrAsyncEnumerableConverterRule Create()
        {
            return (ClrEnumerableToClrAsyncEnumerableConverterRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(RelNode),
                    ClrEnumerableConvention.Instance,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrEnumerableToClrAsyncEnumerableConverterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableToClrAsyncEnumerableConverterRule>(c => new ClrEnumerableToClrAsyncEnumerableConverterRule(c)))
                .toRule(typeof(ClrEnumerableToClrAsyncEnumerableConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableToClrAsyncEnumerableConverterRule(Config config) :
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
            return new ClrEnumerableToClrAsyncEnumerableConverter(
                rel.getCluster(),
                rel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance),
                rel);
        }

    }

}
