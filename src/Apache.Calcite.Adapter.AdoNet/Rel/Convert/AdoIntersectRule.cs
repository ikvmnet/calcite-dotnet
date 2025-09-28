using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    class AdoIntersectRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoIntersectRule Create(AdoConvention convention)
        {
            return (AdoIntersectRule)Config.INSTANCE
                .withConversion(typeof(Intersect), Convention.NONE, convention, "AdoIntersectRule")
                .withRuleFactory(new DelegateFunction<Config, AdoIntersectRule>(c => new AdoIntersectRule(c)))
                .toRule(typeof(AdoIntersectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public AdoIntersectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var intersect = (Intersect)rel;
            if (intersect.all)
                return null;

            var traitSet = intersect.getTraitSet().replace(@out);
            return new AdoIntersect(rel.getCluster(), traitSet, convertList(intersect.getInputs(), @out), false);
        }

    }

}
