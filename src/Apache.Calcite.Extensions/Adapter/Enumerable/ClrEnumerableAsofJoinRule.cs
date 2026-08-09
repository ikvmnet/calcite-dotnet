using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAsofJoin"/> to a <see cref="ClrEnumerableAsofJoin"/>.
    /// </summary>
    public class ClrEnumerableAsofJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableAsofJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableAsofJoinRule Create()
        {
            return (ClrEnumerableAsofJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAsofJoin), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableAsofJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableAsofJoinRule>(c => new ClrEnumerableAsofJoinRule(c)))
                .toRule(typeof(ClrEnumerableAsofJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableAsofJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var join = (LogicalAsofJoin)rel;
            var newInputs = new java.util.ArrayList();

            for (int i = 0; i < join.getInputs().size(); i++)
            {
                var input = (RelNode)join.getInputs().get(i);
                if (input.getConvention() is not ClrEnumerableConvention)
                    input = convert(input, input.getTraitSet().replace(ClrEnumerableConvention.Instance));

                newInputs.add(input);
            }

            return ClrEnumerableAsofJoin.Create(
                (RelNode)newInputs.get(0),
                (RelNode)newInputs.get(1),
                join.getCondition(),
                join.getMatchCondition(),
                join.getVariablesSet(),
                join.getJoinType());
        }

    }

}
