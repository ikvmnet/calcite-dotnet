using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts a <see cref="Values"/> expressed in the default calling
    /// convention to an <see cref="AdoValues"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoValuesRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoValuesRule"/> instance.</returns>
        public static AdoValuesRule Create(AdoConvention convention)
        {
            return (AdoValuesRule)Config.INSTANCE
                .withConversion(typeof(Values), Convention.NONE, convention, "AdoValuesRule")
                .withRuleFactory(new DelegateFunction<Config, AdoValuesRule>(c => new AdoValuesRule(c)))
                .toRule(typeof(AdoValuesRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoValuesRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var values = (Values)rel;
            return new AdoValues(values.getCluster(), values.getRowType(), values.getTuples(), values.getTraitSet().replace(@out));
        }

    }

}
