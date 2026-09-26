using Apache.Calcite.Extensions.Linq4j.Function;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Variant of <see cref="ProjectToCalcRule"/> for the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrCursorProject"/> cannot implement itself, exactly as <c>EnumerableProject</c> cannot:
    /// a calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrCursorProjectToCalcRule : ProjectToCalcRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorProjectToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorProjectToCalcRule Create()
        {
            var config = (ProjectToCalcRule.Config)ProjectToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrCursorProject)).anyInputs()))
                .withDescription("ClrCursorProjectToCalcRule")
                .@as(typeof(ProjectToCalcRule.Config));

            return new ClrCursorProjectToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorProjectToCalcRule(ProjectToCalcRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var project = (ClrCursorProject)call.rel(0);
            var input = project.getInput();

            var program = RexProgram.create(
                input.getRowType(),
                project.getProjects(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder());

            call.transformTo(ClrCursorCalc.Create(input, program));
        }

    }

}
