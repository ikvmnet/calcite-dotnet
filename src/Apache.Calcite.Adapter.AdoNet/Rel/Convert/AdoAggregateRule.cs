using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts a <see cref="Aggregate"/> expressed in the default calling
    /// convention to an <see cref="AdoAggregate"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoAggregateRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoAggregateRule"/> instance.</returns>
        public static AdoAggregateRule Create(AdoConvention convention)
        {
            return (AdoAggregateRule)Config.INSTANCE
                .withConversion(typeof(Aggregate), Convention.NONE, convention, "AdoAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, AdoAggregateRule>(c => new AdoAggregateRule(c)))
                .toRule(typeof(AdoAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoAggregateRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var agg = (Aggregate)rel;
            if (agg.getGroupSets().size() != 1)
                return null;

            var traitSet = agg.getTraitSet().replace(@out);

            try
            {
                return new AdoAggregate(rel.getCluster(), traitSet, convert(agg.getInput(), @out), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
            }
            catch (InvalidRelException)
            {
                return null;
            }
        }

    }

}
