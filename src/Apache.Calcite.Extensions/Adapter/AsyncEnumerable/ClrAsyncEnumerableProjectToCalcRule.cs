using Apache.Calcite.Extensions.Linq4j.Function;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Variant of <see cref="ProjectToCalcRule"/> for the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrAsyncEnumerableProject"/> cannot implement itself, exactly as <c>EnumerableProject</c> cannot:
    /// a calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrAsyncEnumerableProjectToCalcRule : ProjectToCalcRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableProjectToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableProjectToCalcRule Create()
        {
            var config = (ProjectToCalcRule.Config)ProjectToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrAsyncEnumerableProject)).anyInputs()))
                .withDescription("ClrAsyncEnumerableProjectToCalcRule")
                .@as(typeof(ProjectToCalcRule.Config));

            return new ClrAsyncEnumerableProjectToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableProjectToCalcRule(ProjectToCalcRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var project = (ClrAsyncEnumerableProject)call.rel(0);
            var input = project.getInput();

            var program = RexProgram.create(
                input.getRowType(),
                project.getProjects(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder());

            call.transformTo(ClrAsyncEnumerableCalc.Create(input, program));
        }

    }

}
