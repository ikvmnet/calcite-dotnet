using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that converts a logical <see cref="Values"/> into an <see cref="AdoValues"/>. Mirrors
    /// <c>JdbcRules.JdbcValuesRule</c>.
    /// </summary>
    public class AdoValuesRule : AdoConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to.</param>
        /// <returns>The rule.</returns>
        public static AdoValuesRule Create(AdoConvention convention)
        {
            return (AdoValuesRule)Config.INSTANCE
                .withConversion(typeof(Values), Convention.NONE, convention, "AdoValuesRule")
                .withRuleFactory(new DelegateFunction<Config, AdoValuesRule>(c => new AdoValuesRule(c)))
                .toRule(typeof(AdoValuesRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
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
