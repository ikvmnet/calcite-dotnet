using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalProject"/> to a <see cref="ClrEnumerableProject"/>.
    /// </summary>
    public class ClrEnumerableProjectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableProjectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableProjectRule Create()
        {
            return (ClrEnumerableProjectRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalProject),
                    new DelegatePredicate<LogicalProject>(p =>
                        p.containsOver() == false
                        && RexUtil.M2V_FINDER.inProject(p) == false
                        && RexUtil.SubQueryFinder.containsSubQuery(p) == false),
                    Convention.NONE,
                    ClrEnumerableConvention.Instance,
                    "ClrEnumerableProjectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableProjectRule>(c => new ClrEnumerableProjectRule(c)))
                .toRule(typeof(ClrEnumerableProjectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableProjectRule(Config config) :
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
        public override RelNode convert(RelNode rel)
        {
            var project = (Project)rel;

            return ClrEnumerableProject.Create(
                convert(project.getInput(), project.getInput().getTraitSet().replace(ClrEnumerableConvention.Instance)),
                project.getProjects(),
                project.getRowType());
        }

    }

}
