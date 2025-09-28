using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    class AdoSortRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoSortRule Create(AdoConvention convention)
        {
            return (AdoSortRule)Config.INSTANCE
                .withConversion(typeof(Sort), Convention.NONE, convention, "AdoSortRule")
                .withRuleFactory(new DelegateFunction<Config, AdoSortRule>(c => new AdoSortRule(c)))
                .toRule(typeof(AdoSortRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
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
