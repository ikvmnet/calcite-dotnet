using Apache.Calcite.Linq.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rex;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Variant of <c>FilterToCalcRule</c> for the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableFilter"/> cannot implement itself, exactly as <c>EnumerableFilter</c> cannot: a
    /// calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrEnumerableFilterToCalcRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableFilterToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableFilterToCalcRule Create()
        {
            var config = EnumerableFilterToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrEnumerableFilter)).anyInputs()))
                .withDescription("ClrEnumerableFilterToCalcRule");

            return new ClrEnumerableFilterToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableFilterToCalcRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var filter = (ClrEnumerableFilter)call.rel(0);
            var input = filter.getInput();

            // a program that is the identity projection with the condition on it
            var rexBuilder = filter.getCluster().getRexBuilder();
            var programBuilder = new RexProgramBuilder(input.getRowType(), rexBuilder);
            programBuilder.addIdentity();
            programBuilder.addCondition(filter.getCondition());

            call.transformTo(ClrEnumerableCalc.Create(input, programBuilder.getProgram()));
        }

    }

}
