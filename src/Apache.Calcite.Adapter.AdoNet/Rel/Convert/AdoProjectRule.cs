using java.lang;
using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that converts a logical <see cref="Project"/> into an <see cref="AdoProject"/>. Mirrors
    /// <c>JdbcRules.JdbcProjectRule</c>.
    /// </summary>
    /// <remarks>
    /// A projection is not converted if it calls a user-defined function, if it has a window function and the
    /// dialect does not support them, or if it has correlation variables.
    /// </remarks>
    public class AdoProjectRule : AdoConverterRule
    {

        /// <summary>
        /// Returns whether any of the projection's expressions calls a user-defined function.
        /// </summary>
        /// <param name="project">The projection.</param>
        /// <returns>Whether one does.</returns>
        static bool UserDefinedFunctionInProject(Project project)
        {
            var visitor = new CheckingUserDefinedFunctionVisitor();

            foreach (var node in project.getProjects().AsEnumerable<RexNode>())
            {
                node.accept(visitor);

                if (visitor.ContainerUserDefinedFunction)
                    return true;
            }

            return false;
        }

        static bool ConversionPredicate(Project project, AdoConvention convention)
        {
            return (convention.Dialect.supportsWindowFunctions() || !project.containsOver()) && !UserDefinedFunctionInProject(project);
        }

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to, whose dialect decides whether a window function
        /// can be pushed down.</param>
        /// <returns>The rule.</returns>
        public static AdoProjectRule Create(AdoConvention convention)
        {
            return (AdoProjectRule)Config.INSTANCE
                .withConversion(typeof(Project), new DelegatePredicate<Project>(p => ConversionPredicate(p, convention)), Convention.NONE, convention, "AdoProjectRule")
                .withRuleFactory(new DelegateFunction<Config, AdoProjectRule>(c => new AdoProjectRule(c)))
                .toRule(typeof(AdoProjectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
        public AdoProjectRule(Config config) :
            base(config)
        {

        }

        /// <summary>
        /// Matches only a projection with no correlation variables.
        /// </summary>
        /// <param name="call">The rule call.</param>
        /// <returns>Whether the rule applies.</returns>
        public override bool matches(RelOptRuleCall call)
        {
            var project = (Project)call.rel(0);
            return project.getVariablesSet().isEmpty();
        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var project = (Project)rel;
            return new AdoProject(
                rel.getCluster(),
                rel.getTraitSet().replace(@out),
                convert(
                    project.getInput(),
                    project.getInput().getTraitSet().replace(@out)),
                project.getProjects(),
                project.getRowType());
        }

    }

}
