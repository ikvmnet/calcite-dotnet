using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts a <see cref="Union"/> expressed in the default calling
    /// convention to an <see cref="AdoUnion"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoUnionRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoUnionRule"/> instance.</returns>
        public static AdoUnionRule Create(AdoConvention convention)
        {
            return (AdoUnionRule)Config.INSTANCE
                .withConversion(typeof(Union), Convention.NONE, convention, "AdoUnionRule")
                .withRuleFactory(new DelegateFunction<Config, AdoUnionRule>(c => new AdoUnionRule(c)))
                .toRule(typeof(AdoUnionRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
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
