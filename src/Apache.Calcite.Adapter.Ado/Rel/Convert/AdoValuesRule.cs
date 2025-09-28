using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    class AdoValuesRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoValuesRule Create(AdoConvention convention)
        {
            return (AdoValuesRule)Config.INSTANCE
                .withConversion(typeof(Sort), Convention.NONE, convention, "AdoValuesRule")
                .withRuleFactory(new DelegateFunction<Config, AdoValuesRule>(c => new AdoValuesRule(c)))
                .toRule(typeof(AdoValuesRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
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
