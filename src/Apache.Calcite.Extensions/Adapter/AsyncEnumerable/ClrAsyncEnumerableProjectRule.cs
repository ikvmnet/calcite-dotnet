using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalProject"/> to a <see cref="ClrAsyncEnumerableProject"/>.
    /// </summary>
    public class ClrAsyncEnumerableProjectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableProjectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableProjectRule Create()
        {
            return (ClrAsyncEnumerableProjectRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalProject),
                    new DelegatePredicate<LogicalProject>(p =>
                        p.containsOver() == false
                        && RexUtil.M2V_FINDER.inProject(p) == false
                        && RexUtil.SubQueryFinder.containsSubQuery(p) == false),
                    Convention.NONE,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrAsyncEnumerableProjectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableProjectRule>(c => new ClrAsyncEnumerableProjectRule(c)))
                .toRule(typeof(ClrAsyncEnumerableProjectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableProjectRule(Config config) :
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

            return ClrAsyncEnumerableProject.Create(
                convert(project.getInput(), project.getInput().getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                project.getProjects(),
                project.getRowType());
        }

    }

}
