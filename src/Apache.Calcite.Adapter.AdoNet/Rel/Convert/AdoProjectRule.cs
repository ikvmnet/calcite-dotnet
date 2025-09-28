using java.lang;
using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Rule to convert a <see cref="Project"/> to an <see cref="AdoProject"/>.
    /// </summary>
    class AdoProjectRule : AdoConverterRule
    {

        /// <summary>
        /// Returns <c>true</c> if the <see cref="Project"/> contains a user defined function.
        /// </summary>
        /// <param name="project"></param>
        /// <returns></returns>
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
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
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
        /// <param name="config"></param>
        public AdoProjectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
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
