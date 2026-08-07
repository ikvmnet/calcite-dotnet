using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;
using Apache.Calcite.Extensions.Linq4j.Function;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Variant of <see cref="ProjectToCalcRule"/> for the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableProject"/> cannot implement itself, exactly as <c>EnumerableProject</c> cannot:
    /// a calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrEnumerableProjectToCalcRule : ProjectToCalcRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableProjectToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableProjectToCalcRule Create()
        {
            var config = (ProjectToCalcRule.Config)ProjectToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrEnumerableProject)).anyInputs()))
                .withDescription("ClrEnumerableProjectToCalcRule")
                .@as(typeof(ProjectToCalcRule.Config));

            return new ClrEnumerableProjectToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableProjectToCalcRule(ProjectToCalcRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var project = (ClrEnumerableProject)call.rel(0);
            var input = project.getInput();

            var program = RexProgram.create(
                input.getRowType(),
                project.getProjects(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder());

            call.transformTo(ClrEnumerableCalc.Create(input, program));
        }

    }

}
