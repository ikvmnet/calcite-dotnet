using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalProject"/> to a <see cref="ClrCursorProject"/>.
    /// </summary>
    public class ClrCursorProjectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorProjectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorProjectRule Create()
        {
            return (ClrCursorProjectRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalProject),
                    new DelegatePredicate<LogicalProject>(p =>
                        p.containsOver() == false
                        && RexUtil.M2V_FINDER.inProject(p) == false
                        && RexUtil.SubQueryFinder.containsSubQuery(p) == false),
                    Convention.NONE,
                    ClrCursorConvention.Instance,
                    "ClrCursorProjectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorProjectRule>(c => new ClrCursorProjectRule(c)))
                .toRule(typeof(ClrCursorProjectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorProjectRule(Config config) :
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

            return ClrCursorProject.Create(
                convert(project.getInput(), project.getInput().getTraitSet().replace(ClrCursorConvention.Instance)),
                project.getProjects(),
                project.getRowType());
        }

    }

}
