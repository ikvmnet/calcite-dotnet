using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts an <see cref="Intersect"/> expressed in the default calling
    /// convention to an <see cref="AdoIntersect"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoIntersectRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoIntersectRule"/> instance.</returns>
        public static AdoIntersectRule Create(AdoConvention convention)
        {
            return (AdoIntersectRule)Config.INSTANCE
                .withConversion(typeof(Intersect), Convention.NONE, convention, "AdoIntersectRule")
                .withRuleFactory(new DelegateFunction<Config, AdoIntersectRule>(c => new AdoIntersectRule(c)))
                .toRule(typeof(AdoIntersectRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
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
