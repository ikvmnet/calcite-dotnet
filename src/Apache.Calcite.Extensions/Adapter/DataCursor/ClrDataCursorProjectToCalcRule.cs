using Apache.Calcite.Extensions.Linq4j.Function;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Variant of <see cref="ProjectToCalcRule"/> for the <see cref="ClrDataCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrDataCursorProject"/> cannot implement itself, exactly as <c>EnumerableProject</c> cannot:
    /// a calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrDataCursorProjectToCalcRule : ProjectToCalcRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorProjectToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorProjectToCalcRule Create()
        {
            var config = (ProjectToCalcRule.Config)ProjectToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrDataCursorProject)).anyInputs()))
                .withDescription("ClrDataCursorProjectToCalcRule")
                .@as(typeof(ProjectToCalcRule.Config));

            return new ClrDataCursorProjectToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorProjectToCalcRule(ProjectToCalcRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var project = (ClrDataCursorProject)call.rel(0);
            var input = project.getInput();

            var program = RexProgram.create(
                input.getRowType(),
                project.getProjects(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder());

            call.transformTo(ClrDataCursorCalc.Create(input, program));
        }

    }

}
