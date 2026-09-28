using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that converts a logical <see cref="Union"/>, with or without <c>ALL</c>, into an
    /// <see cref="AdoUnion"/> over inputs converted to the same convention. Mirrors
    /// <c>JdbcRules.JdbcUnionRule</c>.
    /// </summary>
    public class AdoUnionRule : AdoConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to.</param>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
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
