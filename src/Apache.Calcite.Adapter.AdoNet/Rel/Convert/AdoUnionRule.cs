using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    class AdoUnionRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoUnionRule Create(AdoConvention convention)
        {
            return (AdoUnionRule)Config.INSTANCE
                .withConversion(typeof(Union), Convention.NONE, convention, "AdoUnionRule")
                .withRuleFactory(new DelegateFunction<Config, AdoUnionRule>(c => new AdoUnionRule(c)))
                .toRule(typeof(AdoUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public AdoUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (Union)rel;
            var traitSet = union.getTraitSet().replace(@out);
            return new AdoUnion(rel.getCluster(), traitSet, convertList(union.getInputs(), @out), union.all);
        }

    }

}
