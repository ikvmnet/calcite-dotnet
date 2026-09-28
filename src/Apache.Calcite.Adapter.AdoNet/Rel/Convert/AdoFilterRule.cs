using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that converts a logical <see cref="Filter"/> into an <see cref="AdoFilter"/>, unless its condition
    /// calls a user-defined function. Mirrors <c>JdbcRules.JdbcFilterRule</c>.
    /// </summary>
    public class AdoFilterRule : AdoConverterRule
    {

        /// <summary>
        /// Returns whether the filter's condition calls a user-defined function.
        /// </summary>
        /// <param name="filter">The filter.</param>
        /// <returns>Whether it does.</returns>
        static bool UserDefinedFunctionInFilter(Filter filter)
        {
            var visitor = new CheckingUserDefinedFunctionVisitor();
            filter.getCondition().accept(visitor);
            return visitor.ContainerUserDefinedFunction;
        }

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to.</param>
        /// <returns>The rule.</returns>
        public static AdoFilterRule Create(AdoConvention convention)
        {
            return (AdoFilterRule)Config.INSTANCE
                .withConversion(typeof(Filter), new DelegatePredicate<Filter>(f => !UserDefinedFunctionInFilter(f)), Convention.NONE, convention, "AdoFilterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoFilterRule>(c => new AdoFilterRule(c)))
                .toRule(typeof(AdoFilterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
        public AdoFilterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var filter = (Filter)rel;
            return new AdoFilter(
                rel.getCluster(), 
                rel.getTraitSet().replace(@out),
                convert(
                    filter.getInput(),
                    filter.getInput().getTraitSet().replace(@out)),
                filter.getCondition());
        }

    }

}
