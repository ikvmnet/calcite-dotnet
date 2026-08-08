using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAsofJoin"/> to a <see cref="ClrAsyncEnumerableAsofJoin"/>.
    /// </summary>
    public class ClrAsyncEnumerableAsofJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableAsofJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableAsofJoinRule Create()
        {
            return (ClrAsyncEnumerableAsofJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAsofJoin), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableAsofJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableAsofJoinRule>(c => new ClrAsyncEnumerableAsofJoinRule(c)))
                .toRule(typeof(ClrAsyncEnumerableAsofJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableAsofJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var join = (LogicalAsofJoin)rel;
            var newInputs = new java.util.ArrayList();

            for (int i = 0; i < join.getInputs().size(); i++)
            {
                var input = (RelNode)join.getInputs().get(i);
                if (input.getConvention() is not ClrAsyncEnumerableConvention)
                    input = convert(input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance));

                newInputs.add(input);
            }

            return ClrAsyncEnumerableAsofJoin.Create(
                (RelNode)newInputs.get(0),
                (RelNode)newInputs.get(1),
                join.getCondition(),
                join.getMatchCondition(),
                join.getVariablesSet(),
                join.getJoinType());
        }

    }

}
