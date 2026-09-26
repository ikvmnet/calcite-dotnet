using Apache.Calcite.Extensions.Linq4j.Function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Variant of <c>FilterToCalcRule</c> for the <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrDataCursorFilter"/> cannot implement itself, exactly as <c>EnumerableFilter</c> cannot: a
    /// calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrDataCursorFilterToCalcRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorFilterToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorFilterToCalcRule Create()
        {
            var config = EnumerableFilterToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrDataCursorFilter)).anyInputs()))
                .withDescription("ClrDataCursorFilterToCalcRule");

            return new ClrDataCursorFilterToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorFilterToCalcRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var filter = (ClrDataCursorFilter)call.rel(0);
            var input = filter.getInput();

            // a program that is the identity projection with the condition on it
            var rexBuilder = filter.getCluster().getRexBuilder();
            var programBuilder = new RexProgramBuilder(input.getRowType(), rexBuilder);
            programBuilder.addIdentity();
            programBuilder.addCondition(filter.getCondition());

            call.transformTo(ClrDataCursorCalc.Create(input, programBuilder.getProgram()));
        }

    }

}
