
using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> to a <see cref="ClrCursorHashJoin"/> or, where
    /// there is no equality to build a lookup on, to a <see cref="ClrCursorNestedLoopJoin"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableJoinRule</c>, which chooses between the two algorithms from one analysis of the
    /// condition.
    /// </remarks>
    public class ClrCursorJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorJoinRule"/>.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorJoinRule Create()
        {
            return (ClrCursorJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorJoinRule>(c => new ClrCursorJoinRule(c)))
                .toRule(typeof(ClrCursorJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule configuration.</param>
        public ClrCursorJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var join = (Join)rel;
            var newInputs = new java.util.ArrayList();

            for (int i = 0; i < join.getInputs().size(); i++)
            {
                var input = (RelNode)join.getInputs().get(i);
                if (input.getConvention() is not ClrCursorConvention)
                    input = convert(input, input.getTraitSet().replace(ClrCursorConvention.Instance));

                newInputs.add(input);
            }

            var rexBuilder = join.getCluster().getRexBuilder();
            var left = (RelNode)newInputs.get(0);
            var right = (RelNode)newInputs.get(1);
            var info = join.analyzeCondition();

            // a join with equi keys, even alongside non-equi conditions, becomes a hash join, which supports
            // every join type; a join with no equi keys becomes a nested-loop join
            var hasEquiKeys = info.leftKeys.isEmpty() == false && info.rightKeys.isEmpty() == false;
            if (hasEquiKeys)
            {
                // as in Calcite, the condition is rearranged with the equi-join parts first, which keeps the
                // plan readable and stable
                var equi = info.getEquiCondition(left, right, rexBuilder);
                var condition = info.isEqui()
                    ? equi
                    : RexUtil.composeConjunction(rexBuilder, java.util.Arrays.asList([equi, RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions)]));

                return ClrCursorHashJoin.Create(left, right, condition, join.getVariablesSet(), join.getJoinType());
            }

            return ClrCursorNestedLoopJoin.Create(left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
        }

    }

}
