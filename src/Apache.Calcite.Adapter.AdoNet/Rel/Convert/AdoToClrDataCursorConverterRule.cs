using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.DataCursor;

using java.util.function;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule that converts a node of an <see cref="AdoConvention"/> to an <see cref="AdoToClrDataCursorConverter"/>.
    /// </summary>
    public class AdoToClrDataCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates an <see cref="AdoToClrDataCursorConverterRule"/>.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoToClrDataCursorConverterRule Create(AdoConvention convention)
        {
            return (AdoToClrDataCursorConverterRule)Config.INSTANCE
                .withConversion(typeof(RelNode), convention, ClrDataCursorConvention.Instance, "AdoToClrDataCursorConverterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoToClrDataCursorConverterRule>(c => new AdoToClrDataCursorConverterRule(c)))
                .toRule(typeof(AdoToClrDataCursorConverterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public AdoToClrDataCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return new AdoToClrDataCursorConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()).simplify(), rel);
        }

    }

}
