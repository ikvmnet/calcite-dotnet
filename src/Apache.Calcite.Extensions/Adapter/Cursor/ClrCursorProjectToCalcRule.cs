using Apache.Calcite.Extensions.Linq4j.Function;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Variant of <see cref="ProjectToCalcRule"/> that converts a <see cref="ClrCursorProject"/> to a
    /// <see cref="ClrCursorCalc"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableProjectToCalcRule</c>. <see cref="ClrCursorProject"/> cannot be implemented, so this
    /// rule must run before the plan is.
    /// </remarks>
    public class ClrCursorProjectToCalcRule : ProjectToCalcRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The rule's configuration; its operand should match a <see cref="ClrCursorProject"/>.</param>
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
