using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    public class AdoAggregateRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoAggregateRule Create(AdoConvention convention)
        {
            return (AdoAggregateRule)Config.INSTANCE
                .withConversion(typeof(Aggregate), Convention.NONE, convention, "AdoAggregateRule")
                .withRuleFactory(new DelegateFunction<Config, AdoAggregateRule>(c => new AdoAggregateRule(c)))
                .toRule(typeof(AdoAggregateRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
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
