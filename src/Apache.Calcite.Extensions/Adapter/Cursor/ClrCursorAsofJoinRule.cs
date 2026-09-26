using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalAsofJoin"/> to a <see cref="ClrCursorAsofJoin"/>.
    /// </summary>
    public class ClrCursorAsofJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorAsofJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorAsofJoinRule Create()
        {
            return (ClrCursorAsofJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalAsofJoin), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorAsofJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorAsofJoinRule>(c => new ClrCursorAsofJoinRule(c)))
                .toRule(typeof(ClrCursorAsofJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorAsofJoinRule(Config config) :
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
                if (input.getConvention() is not ClrCursorConvention)
                    input = convert(input, input.getTraitSet().replace(ClrCursorConvention.Instance));

                newInputs.add(input);
            }

            return ClrCursorAsofJoin.Create(
                (RelNode)newInputs.get(0),
                (RelNode)newInputs.get(1),
                join.getCondition(),
                join.getMatchCondition(),
                join.getVariablesSet(),
                join.getJoinType());
        }

    }

}
