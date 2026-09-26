using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;

using java.util.function;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule that converts a node of an <see cref="AdoConvention"/> to an <see cref="AdoToClrCursorConverter"/>.
    /// </summary>
    public class AdoToClrCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates an <see cref="AdoToClrCursorConverterRule"/>.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoToClrCursorConverterRule Create(AdoConvention convention)
        {
            return (AdoToClrCursorConverterRule)Config.INSTANCE
                .withConversion(typeof(RelNode), convention, ClrCursorConvention.Instance, "AdoToClrCursorConverterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoToClrCursorConverterRule>(c => new AdoToClrCursorConverterRule(c)))
                .toRule(typeof(AdoToClrCursorConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public AdoToClrCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new AdoToClrCursorConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()).simplify(), rel);
        }

    }

}
