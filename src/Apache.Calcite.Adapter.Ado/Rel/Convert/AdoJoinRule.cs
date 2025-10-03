using System.Linq;

using java.lang;
using java.util;
using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    public class AdoJoinRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a new instance of the rule.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static AdoJoinRule Create(AdoConvention convention)
        {
            return (AdoJoinRule)Config.INSTANCE
                .withConversion(typeof(Join), Convention.NONE, convention, "AdoJoinRule")
                .withRuleFactory(new DelegateFunction<Config, AdoJoinRule>(c => new AdoJoinRule(c)))
                .toRule(typeof(AdoJoinRule));
        }

        static bool CanJoinOnCondition(RexNode node)
        {
            switch ((SqlKind.__Enum)node.getKind().ordinal())
            {
                case SqlKind.__Enum.DYNAMIC_PARAM:
                case SqlKind.__Enum.INPUT_REF:
                case SqlKind.__Enum.LITERAL:
                    // literal on a join condition would be TRUE or FALSE
                    return true;
                case SqlKind.__Enum.AND:
                case SqlKind.__Enum.OR:
                case SqlKind.__Enum.IS_NULL:
                case SqlKind.__Enum.IS_NOT_NULL:
                case SqlKind.__Enum.IS_TRUE:
                case SqlKind.__Enum.IS_NOT_TRUE:
                case SqlKind.__Enum.IS_FALSE:
                case SqlKind.__Enum.IS_NOT_FALSE:
                case SqlKind.__Enum.EQUALS:
                case SqlKind.__Enum.NOT_EQUALS:
                case SqlKind.__Enum.GREATER_THAN:
                case SqlKind.__Enum.GREATER_THAN_OR_EQUAL:
                case SqlKind.__Enum.LESS_THAN:
                case SqlKind.__Enum.LESS_THAN_OR_EQUAL:
                case SqlKind.__Enum.IS_NOT_DISTINCT_FROM:
                case SqlKind.__Enum.CAST:
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
            switch ((JoinRelType.__Enum)join.getJoinType().ordinal())
            {
                case JoinRelType.__Enum.SEMI:
                case JoinRelType.__Enum.ANTI:
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
