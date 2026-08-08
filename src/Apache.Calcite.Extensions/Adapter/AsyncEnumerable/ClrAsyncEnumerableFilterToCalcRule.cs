using Apache.Calcite.Extensions.Linq4j.Function;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rex;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Variant of <c>FilterToCalcRule</c> for the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrAsyncEnumerableFilter"/> cannot implement itself, exactly as <c>EnumerableFilter</c> cannot: a
    /// calc is always better, because it carries the filter and the projection together.
    /// </remarks>
    public class ClrAsyncEnumerableFilterToCalcRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableFilterToCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableFilterToCalcRule Create()
        {
            var config = EnumerableFilterToCalcRule.Config.DEFAULT
                .withOperandSupplier(new DelegateOperandTransform(b => b.operand((java.lang.Class)typeof(ClrAsyncEnumerableFilter)).anyInputs()))
                .withDescription("ClrAsyncEnumerableFilterToCalcRule");

            return new ClrAsyncEnumerableFilterToCalcRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableFilterToCalcRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var filter = (ClrAsyncEnumerableFilter)call.rel(0);
            var input = filter.getInput();

            // a program that is the identity projection with the condition on it
            var rexBuilder = filter.getCluster().getRexBuilder();
            var programBuilder = new RexProgramBuilder(input.getRowType(), rexBuilder);
            programBuilder.addIdentity();
            programBuilder.addCondition(filter.getCondition());

            call.transformTo(ClrAsyncEnumerableCalc.Create(input, programBuilder.getProgram()));
        }

    }

}
