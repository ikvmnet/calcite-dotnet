using Apache.Calcite.Extensions.Linq4j.Function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Variant of <c>FilterToCalcRule</c> for the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableFilterToCalcRule</c>: replaces a <see cref="ClrCursorFilter"/>, which cannot be
    /// implemented, with a <see cref="ClrCursorCalc"/> that applies the condition to an identity projection.
    /// Part of <see cref="ClrCursorRules.CalcRules"/>.
    /// </remarks>
    public class ClrCursorFilterToCalcRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorFilterToCalcRule"/>.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorFilterToCalcRule Create()
        {
            var config = EnumerableFilterToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrCursorFilter)).anyInputs()))
                .withDescription("ClrCursorFilterToCalcRule");

            return new ClrCursorFilterToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule configuration.</param>
        public ClrCursorFilterToCalcRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var filter = (ClrCursorFilter)call.rel(0);
            var input = filter.getInput();

            var rexBuilder = filter.getCluster().getRexBuilder();
            var programBuilder = new RexProgramBuilder(input.getRowType(), rexBuilder);
            programBuilder.addIdentity();
            programBuilder.addCondition(filter.getCondition());

            call.transformTo(ClrCursorCalc.Create(input, programBuilder.getProgram()));
        }

    }

}
