using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalJoin"/> to a
    /// <see cref="ClrCursorBatchNestedLoopJoin"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableBatchNestedLoopJoinRule</c>. The right input becomes a filter over a disjunction of
    /// the join condition repeated once per batch position, each against its own correlation variable, so one
    /// pass of the right input serves a whole batch of left rows. Matches inner, left, semi and anti joins.
    ///
    /// <para>Like Calcite's, this rule is not among the default rules; a caller adds it.</para>
    /// </remarks>
    public class ClrCursorBatchNestedLoopJoinRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorBatchNestedLoopJoinRule"/>, with Calcite's default batch size.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorBatchNestedLoopJoinRule Create()
        {
            return Create(org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoinRule.Config.DEFAULT.batchSize());
        }

        /// <summary>
        /// Creates a <see cref="ClrCursorBatchNestedLoopJoinRule"/>.
        /// </summary>
        /// <param name="batchSize">How many left rows one pass of the right input serves.</param>
        /// <returns>The rule.</returns>
        public static ClrCursorBatchNestedLoopJoinRule Create(int batchSize)
        {
            var config = org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoinRule.Config.DEFAULT
                .withBatchSize(batchSize)
                .withDescription($"ClrCursorBatchNestedLoopJoinRule({batchSize})");

            return new ClrCursorBatchNestedLoopJoinRule((RelRule.Config)config, batchSize);
        }

        readonly int batchSize;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule configuration.</param>
        /// <param name="batchSize">How many left rows one pass of the right input serves.</param>
        public ClrCursorBatchNestedLoopJoinRule(RelRule.Config config, int batchSize) :
            base(config)
        {
            this.batchSize = batchSize;
        }

        /// <inheritdoc />
        public override bool matches(RelOptRuleCall call)
        {
            var joinType = ((Join)call.rel(0)).getJoinType().name();

            return joinType is nameof(JoinRelType.INNER)
                or nameof(JoinRelType.LEFT)
                or nameof(JoinRelType.ANTI)
                or nameof(JoinRelType.SEMI);
        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var join = (Join)call.rel(0);
            var leftFieldCount = join.getLeft().getRowType().getFieldCount();
            var cluster = join.getCluster();
            var rexBuilder = cluster.getRexBuilder();
            var relBuilder = call.builder();

            var correlationIds = new java.util.HashSet();
            var corrVarList = new java.util.ArrayList();

            for (int i = 0; i < batchSize; i++)
            {
                var correlationId = cluster.createCorrel();
                correlationIds.add(correlationId);
                corrVarList.add(rexBuilder.makeCorrel(join.getLeft().getRowType(), correlationId));
            }

            var corrVar0 = (RexNode)corrVarList.get(0);
            var requiredColumns = ImmutableBitSet.builder();

            // the condition against the first correlation variable: a reference to the right input is shifted
            // to the right input's own field numbering, and one to the left becomes a field of that variable
            var condition = (RexNode)join.getCondition().accept(new FirstCondition(rexBuilder, leftFieldCount, corrVar0, requiredColumns));

            var conditionList = new java.util.ArrayList();
            conditionList.add(condition);

            // the same condition against each other batch position
            for (int i = 1; i < batchSize; i++)
                conditionList.add((RexNode)condition.accept(new OtherCondition(corrVar0, (RexNode)corrVarList.get(i))));

            relBuilder.push(join.getRight()).filter(relBuilder.or(conditionList));
            var right = relBuilder.build();

            call.transformTo(
                ClrCursorBatchNestedLoopJoin.Create(
                    convert(call.getPlanner(), join.getLeft(), join.getLeft().getTraitSet().replace(ClrCursorConvention.Instance)),
                    convert(call.getPlanner(), right, right.getTraitSet().replace(ClrCursorConvention.Instance)),
                    join.getCondition(),
                    com.google.common.collect.ImmutableSet.copyOf(correlationIds),
                    requiredColumns.build(),
                    join.getJoinType()));
        }

        /// <summary>
        /// Rewrites a reference to the left input as a field of the first correlation variable, recording it
        /// as required, and renumbers a reference to the right input against the right input alone.
        /// </summary>
        sealed class FirstCondition(RexBuilder rexBuilder, int leftFieldCount, RexNode corrVar0, ImmutableBitSet.Builder requiredColumns) : RexShuttle
        {

            /// <inheritdoc />
            public override RexNode visitInputRef(RexInputRef input)
            {
                var field = input.getIndex();
                if (field >= leftFieldCount)
                    return rexBuilder.makeInputRef(input.getType(), field - leftFieldCount);

                requiredColumns.set(field);

                return rexBuilder.makeFieldAccess(corrVar0, field);
            }

        }

        /// <summary>
        /// Replaces the first correlation variable with another one of the batch.
        /// </summary>
        sealed class OtherCondition(RexNode corrVar0, RexNode corrVar) : RexShuttle
        {

            /// <inheritdoc />
            public override RexNode visitCorrelVariable(RexCorrelVariable variable)
            {
                return variable.equals(corrVar0) ? corrVar : variable;
            }

        }

    }

}
