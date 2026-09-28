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
    /// The rule that converts a logical <see cref="Join"/> into an <see cref="AdoJoin"/>. Mirrors
    /// <c>JdbcRules.JdbcJoinRule</c>.
    /// </summary>
    /// <remarks>
    /// Semi- and anti-joins are not converted; other join types are. The condition may use only
    /// input references, literals, dynamic parameters, <c>AND</c>, <c>OR</c>, the comparisons,
    /// <c>IS [NOT] NULL</c>, <c>IS [NOT] TRUE</c>, <c>IS [NOT] FALSE</c>, <c>IS NOT DISTINCT FROM</c> and
    /// <c>CAST</c>.
    /// </remarks>
    public class AdoJoinRule : AdoConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to.</param>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
        public AdoJoinRule(Config config) :
            base(config)
        {

        }

        /// <summary>
        /// Returns an <see cref="AdoJoin"/> over the join's inputs converted to this convention, or
        /// <see langword="null"/> where the join cannot be pushed down.
        /// </summary>
        /// <param name="rn">The join.</param>
        /// <returns>The converted join, or <see langword="null"/>.</returns>
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

        /// <summary>
        /// Builds the <see cref="AdoJoin"/>. Mirrors <c>JdbcJoinRule.convert(Join, boolean)</c>.
        /// </summary>
        /// <param name="join">The join.</param>
        /// <param name="convertInputTraits">Whether to convert the inputs and check the condition.</param>
        /// <returns>The converted join, or <see langword="null"/>.</returns>
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
