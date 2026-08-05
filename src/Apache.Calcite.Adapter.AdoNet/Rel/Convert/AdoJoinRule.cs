using System.Linq;

using java.lang;
using java.util;
using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts a <see cref="Join"/> expressed in the default calling
    /// convention to an <see cref="AdoJoin"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoJoinRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoJoinRule"/> instance.</returns>
        public static AdoJoinRule Create(AdoConvention convention)
        {
            return (AdoJoinRule)Config.INSTANCE
                .withConversion(typeof(Join), Convention.NONE, convention, "AdoJoinRule")
                .withRuleFactory(new DelegateFunction<Config, AdoJoinRule>(c => new AdoJoinRule(c)))
                .toRule(typeof(AdoJoinRule));
        }

        static bool CanJoinOnCondition(RexNode node)
        {
            switch (node.getKind().name())
            {
                case nameof(SqlKind.DYNAMIC_PARAM):
                case nameof(SqlKind.INPUT_REF):
                case nameof(SqlKind.LITERAL):
                    // literal on a join condition would be TRUE or FALSE
                    return true;
                case nameof(SqlKind.AND):
                case nameof(SqlKind.OR):
                case nameof(SqlKind.IS_NULL):
                case nameof(SqlKind.IS_NOT_NULL):
                case nameof(SqlKind.IS_TRUE):
                case nameof(SqlKind.IS_NOT_TRUE):
                case nameof(SqlKind.IS_FALSE):
                case nameof(SqlKind.IS_NOT_FALSE):
                case nameof(SqlKind.EQUALS):
                case nameof(SqlKind.NOT_EQUALS):
                case nameof(SqlKind.GREATER_THAN):
                case nameof(SqlKind.GREATER_THAN_OR_EQUAL):
                case nameof(SqlKind.LESS_THAN):
                case nameof(SqlKind.LESS_THAN_OR_EQUAL):
                case nameof(SqlKind.IS_NOT_DISTINCT_FROM):
                case nameof(SqlKind.CAST):
                    return ((RexCall)node).getOperands().AsEnumerable<RexNode>().All(CanJoinOnCondition);
                default:
                    return false;
            }
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public AdoJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rn)
        {
            var join = (Join)rn;
            switch (join.getJoinType().name())
            {
                case nameof(JoinRelType.SEMI):
                case nameof(JoinRelType.ANTI):
                    return null;
                default:
                    return Convert(join, true);
            }
        }

        /// <inheritdoc />
        AdoJoin? Convert(Join join, bool convertInputTraits)
        {
            var n = new System.Collections.Generic.List<RelNode>(2);

            foreach (var input in join.getInputs().AsEnumerable<RelNode>())
            {
                var i = input;
                if (convertInputTraits && i.getConvention() != getOutTrait())
                    i = convert(i, i.getTraitSet().replace(@out));

                n.Add(i);
            }

            if (convertInputTraits && !CanJoinOnCondition(join.getCondition()))
                return null;

            try
            {
                return new AdoJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(@out),
                    join.getHints(),
                    n[0],
                    n[1],
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType());
            }
            catch (InvalidRelException)
            {
                return null;
            }
        }

    }

}
