using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> to a <see cref="ClrAsyncEnumerableHashJoin"/> or, where
    /// there is no equality to build a lookup on, to a <see cref="ClrAsyncEnumerableNestedLoopJoin"/>.
    /// </summary>
    /// <remarks>
    /// One rule for both algorithms, as <c>EnumerableJoinRule</c> is. Two rules were tried, and the second
    /// one was ours rather than Calcite's: the choice is made from one <c>JoinInfo</c>, so making it twice
    /// means analysing the condition twice and agreeing about it by luck.
    /// </remarks>
    public class ClrAsyncEnumerableJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableJoinRule Create()
        {
            return (ClrAsyncEnumerableJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableJoinRule>(c => new ClrAsyncEnumerableJoinRule(c)))
                .toRule(typeof(ClrAsyncEnumerableJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var join = (Join)rel;
            var newInputs = new java.util.ArrayList();

            for (int i = 0; i < join.getInputs().size(); i++)
            {
                var input = (RelNode)join.getInputs().get(i);
                if (input.getConvention() is not ClrAsyncEnumerableConvention)
                    input = convert(input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance));

                newInputs.add(input);
            }

            var rexBuilder = join.getCluster().getRexBuilder();
            var left = (RelNode)newInputs.get(0);
            var right = (RelNode)newInputs.get(1);
            var info = join.analyzeCondition();

            // if the join has equi keys (a complete or a partial equi-join), a hash join takes it — it
            // supports every join type, even where the condition also carries non-equi parts; a complete
            // non-equi join goes to the nested loop, where hashing would buy nothing.
            var hasEquiKeys = info.leftKeys.isEmpty() == false && info.rightKeys.isEmpty() == false;
            if (hasEquiKeys)
            {
                // re-arrange the condition: the equi-join elements first, the non-equi ones after. Not
                // strictly necessary, and it keeps a plan readable and stable.
                var equi = info.getEquiCondition(left, right, rexBuilder);
                var condition = info.isEqui()
                    ? equi
                    : RexUtil.composeConjunction(rexBuilder, java.util.Arrays.asList([equi, RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions)]));

                return ClrAsyncEnumerableHashJoin.Create(left, right, condition, join.getVariablesSet(), join.getJoinType());
            }

            return ClrAsyncEnumerableNestedLoopJoin.Create(left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
        }

    }

}
