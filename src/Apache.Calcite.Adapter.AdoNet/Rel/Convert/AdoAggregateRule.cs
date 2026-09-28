using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that converts a logical <see cref="Aggregate"/> into an <see cref="AdoAggregate"/>. Mirrors
    /// <c>JdbcRules.JdbcAggregateRule</c>.
    /// </summary>
    /// <remarks>
    /// An aggregate with more than one grouping set (<c>GROUPING SETS</c>, <c>ROLLUP</c>, <c>CUBE</c>) is not
    /// converted, nor is one whose functions or <c>FILTER</c> clauses the dialect does not support.
    /// </remarks>
    public class AdoAggregateRule : AdoConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to.</param>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
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
