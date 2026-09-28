using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;

using java.util.function;

using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that puts an <see cref="AdoToClrCursorConverter"/> over a node of an <see cref="AdoConvention"/>,
    /// so that <see cref="ClrCursorConvention"/> can read its rows.
    /// </summary>
    public class AdoToClrCursorConverterRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted from.</param>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
        public AdoToClrCursorConverterRule(Config config) :
            base(config)
        {

        }

        /// <summary>
        /// Returns an <see cref="AdoToClrCursorConverter"/> over <paramref name="rel"/>.
        /// </summary>
        /// <param name="rel">The node of the <see cref="AdoConvention"/>.</param>
        /// <returns>The converter.</returns>
        /// <remarks>
        /// The trait set is simplified, as <c>RelOptRule.convert</c> does: the planner files the input under its
        /// simplified traits, and a converter that kept, say, two collations would claim an order its input does
        /// not have.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            return new AdoToClrCursorConverter(rel.getCluster(), rel.getTraitSet().replace(getOutConvention()).simplify(), rel);
        }

    }

}
