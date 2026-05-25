using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts a <see cref="Sort"/> expressed in the default calling
    /// convention to an <see cref="AdoSort"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoSortRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoSortRule"/> instance.</returns>
        public static AdoSortRule Create(AdoConvention convention)
        {
            return (AdoSortRule)Config.INSTANCE
                .withConversion(typeof(Sort), Convention.NONE, convention, "AdoSortRule")
                .withRuleFactory(new DelegateFunction<Config, AdoSortRule>(c => new AdoSortRule(c)))
                .toRule(typeof(AdoSortRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoSortRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return Convert((Sort)rel, true);
        }

        RelNode Convert(Sort sort, bool convertInputTraits)
        {
            var traitSet = sort.getTraitSet().replace(@out);

            RelNode input;
            if (convertInputTraits)
            {
                var inputTraitSet = sort.getInput().getTraitSet().replace(@out);
                input = convert(sort.getInput(), inputTraitSet);
            }
            else
            {
                input = sort.getInput();
            }

            return new AdoSort(sort.getCluster(), traitSet, input, sort.getCollation(), sort.offset, sort.fetch);
        }

    }

}
