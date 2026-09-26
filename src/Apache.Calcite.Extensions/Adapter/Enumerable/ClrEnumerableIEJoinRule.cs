using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts an inner <see cref="LogicalJoin"/> whose condition is two or more cross-input
    /// field inequalities to a <see cref="ClrEnumerableIEJoin"/>.
    /// </summary>
    /// <remarks>
    /// <c>EnumerableIEJoinRule</c>. The first two inequalities drive the join and the rest are evaluated by
    /// a <see cref="ClrEnumerableCalc"/> above it.
    ///
    /// <para>Based on Khayyat et al., "Lightning Fast and Space Efficient Inequality Joins", PVLDB 8(13),
    /// 2015.</para>
    /// </remarks>
    public class ClrEnumerableIEJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableIEJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableIEJoinRule Create()
        {
            return (ClrEnumerableIEJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableIEJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableIEJoinRule>(c => new ClrEnumerableIEJoinRule(c)))
                .toRule(typeof(ClrEnumerableIEJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableIEJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var join = (Join)rel;

            if (join.getJoinType().name() != nameof(JoinRelType.INNER)
                || join.getVariablesSet().isEmpty() == false
                || join.getSystemFieldList().isEmpty() == false)
                return null;

            var leftFieldCount = join.getLeft().getRowType().getFieldCount();
            var conjunctions = RelOptUtil.conjunctions(join.getCondition());
            if (conjunctions.size() < 2)
                return null;

            for (int i = 0; i < conjunctions.size(); i++)
            {
                var condition = ClrEnumerableIEJoin.AnalyzeConjunction((RexNode)conjunctions.get(i), leftFieldCount);
                if (condition == null)
                    return null;

                // only the two that drive the join have to be orderable; the rest are a calc's predicate
                if (i < 2 && ClrEnumerableIEJoin.SupportsKeyTypes(join.getLeft(), join.getRight(), condition) == false)
                    return null;
            }

            var left = convert(join.getLeft(), join.getLeft().getTraitSet().replace(ClrEnumerableConvention.Instance));
            var right = convert(join.getRight(), join.getRight().getTraitSet().replace(ClrEnumerableConvention.Instance));

            var rexBuilder = join.getCluster().getRexBuilder();
            var ieCondition = RexUtil.composeConjunction(rexBuilder, conjunctions.subList(0, 2))
                ?? throw new java.lang.NullPointerException("ieCondition");

            var ieJoin = ClrEnumerableIEJoin.Create(left, right, ieCondition);
            if (conjunctions.size() == 2)
                return ieJoin;

            var residual = RexUtil.composeConjunction(rexBuilder, conjunctions.subList(2, conjunctions.size()))
                ?? throw new java.lang.NullPointerException("residual");

            var program = RexProgram.create(
                ieJoin.getRowType(),
                rexBuilder.identityProjects(ieJoin.getRowType()),
                residual,
                ieJoin.getRowType(),
                rexBuilder);

            return ClrEnumerableCalc.Create(ieJoin, program);
        }

    }

}
