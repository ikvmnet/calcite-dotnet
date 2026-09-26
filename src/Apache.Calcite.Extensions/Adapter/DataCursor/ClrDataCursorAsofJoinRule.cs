using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAsofJoin"/> to a <see cref="ClrDataCursorAsofJoin"/>.
    /// </summary>
    public class ClrDataCursorAsofJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorAsofJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorAsofJoinRule Create()
        {
            return (ClrDataCursorAsofJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAsofJoin), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorAsofJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorAsofJoinRule>(c => new ClrDataCursorAsofJoinRule(c)))
                .toRule(typeof(ClrDataCursorAsofJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorAsofJoinRule(Config config) :
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
                if (input.getConvention() is not ClrDataCursorConvention)
                    input = convert(input, input.getTraitSet().replace(ClrDataCursorConvention.Instance));

                newInputs.add(input);
            }

            return ClrDataCursorAsofJoin.Create(
                (RelNode)newInputs.get(0),
                (RelNode)newInputs.get(1),
                join.getCondition(),
                join.getMatchCondition(),
                join.getVariablesSet(),
                join.getJoinType());
        }

    }

}
