
using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;
using org.apache.calcite.sql.fun;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> to a <see cref="ClrCursorMergeJoin"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableMergeJoinRule</c>. The rule requests each input sorted ascending, nulls last, on its
    /// join keys, and leaves it to the planner to decide whether providing that order is worth the cost. It
    /// declines a join whose condition uses <c>IS NOT DISTINCT FROM</c>, a join type the merge join does not
    /// support, and a join with no equi-join keys.
    /// </remarks>
    public class ClrCursorMergeJoinRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>A rule converting <see cref="LogicalJoin"/> in <c>Convention.NONE</c> to <see cref="ClrCursorConvention"/>.</returns>
        public static ClrCursorMergeJoinRule Create()
        {
            return (ClrCursorMergeJoinRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalJoin), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorMergeJoinRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorMergeJoinRule>(c => new ClrCursorMergeJoinRule(c)))
                .toRule(typeof(ClrCursorMergeJoinRule));
        }

        /// <summary>
        /// Initializes a new instance from a converter rule configuration.
        /// </summary>
        /// <param name="config">The rule's configuration.</param>
        public ClrCursorMergeJoinRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var join = (Join)rel;

            // a merge join stops at a null, and IS NOT DISTINCT FROM treats two nulls as equal, so a
            // condition containing one cannot supply merge join keys
            if (RexUtil.findOperatorCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, join.getCondition()) != null)
                return null;

            var info = JoinInfo.createWithStrictEquality(join.getLeft(), join.getRight(), join.getCondition());

            if (ClrCursorMergeJoin.IsMergeJoinSupported(join.getJoinType()) == false)
                return null;

            // a cartesian join could be merged, but EnumerableMergeJoinRule declines it and so does this
            if (info.pairs().isEmpty())
                return null;

            var newInputs = new java.util.ArrayList();
            var collations = new java.util.ArrayList();
            var offset = 0;

            for (int ord = 0; ord < join.getInputs().size(); ord++)
            {
                var input = (RelNode)join.getInputs().get(ord);
                var traits = input.getTraitSet().replace(ClrCursorConvention.Instance);

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

            var traitSet = join.getTraitSet().replace(ClrCursorConvention.Instance);
            if (collations.isEmpty() == false)
                traitSet = traitSet.replace(collations);

            // equi-join conjuncts first and the rest after, as Calcite orders them, so plans print alike
            var rexBuilder = join.getCluster().getRexBuilder();
            var equi = info.getEquiCondition(left, right, rexBuilder);
            var condition = info.isEqui()
                ? equi
                : RexUtil.composeConjunction(rexBuilder, java.util.Arrays.asList([equi, RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions)]));

            return new ClrCursorMergeJoin(join.getCluster(), traitSet, left, right, condition, join.getVariablesSet(), join.getJoinType());
        }

    }

}
