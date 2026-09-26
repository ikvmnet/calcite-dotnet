using Apache.Calcite.Extensions.Adapter.Enumerable;

using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;
using org.apache.calcite.sql.fun;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> to a <see cref="ClrDataCursorMergeJoin"/>.
    /// </summary>
    /// <remarks>
    /// Unlike the hash and nested loop rule, this one asks its inputs for a collation on the join keys rather
    /// than taking what they have: a merge join is only a merge join if they arrive sorted, and the planner
    /// decides whether satisfying that is worth it.
    /// </remarks>
    public class ClrDataCursorMergeJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorMergeJoinRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorMergeJoinRule Create()
        {
            return (ClrDataCursorMergeJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorMergeJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorMergeJoinRule>(c => new ClrDataCursorMergeJoinRule(c)))
                .toRule(typeof(ClrDataCursorMergeJoinRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorMergeJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var join = (Join)rel;

            // a merge join stops at a null, and IS NOT DISTINCT FROM says two nulls are equal, so a
            // condition carrying one cannot be a merge join key
            if (RexUtil.findOperatorCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, join.getCondition()) != null)
                return null;

            var info = JoinInfo.createWithStrictEquality(join.getLeft(), join.getRight(), join.getCondition());

            // a merge join answers only some join types
            if (ClrDataCursorMergeJoin.IsMergeJoinSupported(join.getJoinType()) == false)
                return null;

            // a cartesian join could be merged too, and Calcite leaves it off for now
            if (info.pairs().isEmpty())
                return null;

            var newInputs = new java.util.ArrayList();
            var collations = new java.util.ArrayList();
            var offset = 0;

            for (int ord = 0; ord < join.getInputs().size(); ord++)
            {
                var input = (RelNode)join.getInputs().get(ord);
                var traits = input.getTraitSet().replace(ClrDataCursorConvention.Instance);

                var fieldCollations = new java.util.ArrayList();
                var keys = (org.apache.calcite.util.ImmutableIntList)info.keys().get(ord);
                for (int i = 0; i < keys.size(); i++)
                    fieldCollations.add(new RelFieldCollation(((java.lang.Integer)keys.get(i)).intValue(), RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST));

                var collation = RelCollations.of(fieldCollations);
                collations.add(RelCollations.shift(collation, offset));
                traits = traits.replace(collation);

                newInputs.add(convert(input, traits));
                offset += input.getRowType().getFieldCount();
            }

            var left = (RelNode)newInputs.get(0);
            var right = (RelNode)newInputs.get(1);

            var traitSet = join.getTraitSet().replace(ClrDataCursorConvention.Instance);
            if (collations.isEmpty() == false)
                traitSet = traitSet.replace(collations);

            // re-arrange the condition: the equi-join elements first, the non-equi ones after
            var rexBuilder = join.getCluster().getRexBuilder();
            var equi = info.getEquiCondition(left, right, rexBuilder);
            var condition = info.isEqui()
                ? equi
                : RexUtil.composeConjunction(rexBuilder, java.util.Arrays.asList([equi, RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions)]));

            return new ClrDataCursorMergeJoin(join.getCluster(), traitSet, left, right, condition, join.getVariablesSet(), join.getJoinType());
        }

    }

}
