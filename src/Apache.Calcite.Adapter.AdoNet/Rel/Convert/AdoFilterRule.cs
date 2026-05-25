using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule to convert a <see cref="Filter"/> to a <see cref="AdoFilter"/>.
    /// </summary>
    public class AdoFilterRule : AdoConverterRule
    {

        /// <summary>
        /// Returns <c>true</c> if the filter contains a user defined function.
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        static bool UserDefinedFunctionInFilter(Filter filter)
        {
            var visitor = new CheckingUserDefinedFunctionVisitor();
            filter.getCondition().accept(visitor);
            return visitor.ContainerUserDefinedFunction;
        }

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// Filters containing user-defined functions are excluded and remain in the default convention.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoFilterRule"/> instance.</returns>
        public static AdoFilterRule Create(AdoConvention convention)
        {
            return (AdoFilterRule)Config.INSTANCE
                .withConversion(typeof(Filter), new DelegatePredicate<Filter>(f => !UserDefinedFunctionInFilter(f)), Convention.NONE, convention, "AdoFilterRule")
                .withRuleFactory(new DelegateFunction<Config, AdoFilterRule>(c => new AdoFilterRule(c)))
                .toRule(typeof(AdoFilterRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
        public AdoFilterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
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
