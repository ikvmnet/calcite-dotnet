using System.Collections.Generic;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The rules that put a plan into the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRules</c>, and the same shape: a field per rule, a list of the ones
    /// registered by default, and an accessor returning it. A field rather than a
    /// factory call, because a caller has to be able to name one to remove it, and
    /// <c>RelOptPlanner.removeRule</c> takes the rule itself.
    ///
    /// <para><b>The list is <c>EnumerableRules.ENUMERABLE_RULES</c>, in Calcite's order</b>, with the same
    /// three rules kept out of it for a caller to add — the sorted aggregate, the batch nested loop join and
    /// the limit sort — and the interpreter's beside them. The two converters against
    /// <c>EnumerableConvention</c> follow. MATCH_RECOGNIZE is the one node this convention cannot write, and
    /// Calcite plans it under a converter.</para>
    /// </remarks>
    public static class ClrCursorRules
    {

        /// <summary>
        /// Rule that converts a table scan to a <see cref="ClrCursorTableScan"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorTableScanRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorTableScanRule.Create();

        /// <summary>
        /// Rule that converts a VALUES to a <see cref="ClrCursorValues"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorValuesRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorValuesRule.Create();

        /// <summary>
        /// Rule that converts a project to a <see cref="ClrCursorProject"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorProjectRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorProjectRule.Create();

        /// <summary>
        /// Rule that converts a filter to a <see cref="ClrCursorFilter"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorFilterRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorFilterRule.Create();

        /// <summary>
        /// Rule that converts a calc to a <see cref="ClrCursorCalc"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorCalcRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorCalcRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrCursorHashJoin"/> or a
        /// <see cref="ClrCursorNestedLoopJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorJoinRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorJoinRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrCursorMergeJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorMergeJoinRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorMergeJoinRule.Create();

        /// <summary>
        /// Rule that converts an ASOF join to a <see cref="ClrCursorAsofJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorAsofJoinRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorAsofJoinRule.Create();

        /// <summary>
        /// Rule that converts a correlate to a <see cref="ClrCursorCorrelate"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorCorrelateRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorCorrelateRule.Create();

        /// <summary>
        /// Rule that converts a conditional correlate to a
        /// <see cref="ClrCursorConditionalCorrelate"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorConditionalCorrelateRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorConditionalCorrelateRule.Create();

        /// <summary>
        /// Rule that converts a combine to a <see cref="ClrCursorCombine"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorCombineRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorCombineRule.Create();

        /// <summary>
        /// Rule that converts an aggregate to a <see cref="ClrCursorAggregate"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorAggregateRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorAggregateRule.Create();

        /// <summary>
        /// Rule that converts a union to a <see cref="ClrCursorUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorUnionRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorUnionRule.Create();

        /// <summary>
        /// Rule that converts a sort over a union to a <see cref="ClrCursorMergeUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorMergeUnionRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorMergeUnionRule.Create();

        /// <summary>
        /// Rule that converts an intersect to a <see cref="ClrCursorIntersect"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorIntersectRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorIntersectRule.Create();

        /// <summary>
        /// Rule that converts a minus to a <see cref="ClrCursorMinus"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorMinusRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorMinusRule.Create();

        /// <summary>
        /// Rule that converts a sort to a <see cref="ClrCursorSort"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorSortRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorSortRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a <see cref="ClrCursorLimit"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorLimitRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorLimitRule.Create();

        /// <summary>
        /// Rule that converts a table function scan to a <see cref="ClrCursorTableFunctionScan"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorTableFunctionScanRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorTableFunctionScanRule.Create();

        /// <summary>
        /// Rule that converts a collect to a <see cref="ClrCursorCollect"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorCollectRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorCollectRule.Create();

        /// <summary>
        /// Rule that converts an uncollect to a <see cref="ClrCursorUncollect"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorUncollectRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorUncollectRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a
        /// <see cref="ClrCursorLimitSort"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_LIMIT_SORT_RULE</c> is not in
        /// <c>ENUMERABLE_RULES</c>: it is one of the three rule fields Calcite declares and leaves out of
        /// the list, with <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> and
        /// <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c>, and nothing in core turns any of them on. A caller
        /// turns this on to sort only as far as the fetch requires rather than sorting and then discarding.
        /// </remarks>
        public static readonly RelOptRule ClrCursorLimitSortRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorLimitSortRule.Create();

        /// <summary>
        /// Rule that converts a window to a <see cref="ClrCursorWindow"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorWindowRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorWindowRule.Create();

        /// <summary>
        /// Rule that converts a repeat union to a <see cref="ClrCursorRepeatUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorRepeatUnionRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRepeatUnionRule.Create();

        /// <summary>
        /// Rule that converts a table spool to a <see cref="ClrCursorTableSpool"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorTableSpoolRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorTableSpoolRule.Create();

        /// <summary>
        /// Rule that turns a filter of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrCursorFilterToCalcRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorFilterToCalcRule.Create();

        /// <summary>
        /// Rule that turns a project of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrCursorProjectToCalcRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorProjectToCalcRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>EnumerableConvention</c> as one of this convention.
        /// </summary>
        public static readonly RelOptRule EnumerableToClrCursorConverterRule = Apache.Calcite.Extensions.Adapter.Cursor.EnumerableToClrCursorConverterRule.Create();

        /// <summary>
        /// Rule that reads a plan of this convention as one of <c>EnumerableConvention</c>.
        /// </summary>
        public static readonly RelOptRule ClrCursorToEnumerableConverterRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorToEnumerableConverterRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrCursorBatchNestedLoopJoin"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c> is
        /// not in <c>ENUMERABLE_RULES</c>: a caller turns it on, and chooses the batch size with
        /// <see cref="ClrCursorBatchNestedLoopJoinRule.Create(int)"/>.
        /// </remarks>
        public static readonly RelOptRule ClrCursorBatchNestedLoopJoinRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorBatchNestedLoopJoinRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>BindableConvention</c> as one of this convention, by interpreting it.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>TO_INTERPRETER</c> is not in
        /// <c>ENUMERABLE_RULES</c>: Calcite registers it from <c>RelOptUtil.registerDefaultRules</c>, which
        /// registers Calcite's own. A caller adds this one to have an interpreted node land here rather than
        /// in <c>EnumerableConvention</c> under a converter.
        /// </remarks>
        public static readonly RelOptRule ClrCursorInterpreterRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorInterpreterRule.Create();

        /// <summary>
        /// Rule that converts an aggregate over a sorted input to a
        /// <see cref="ClrCursorSortedAggregate"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> is not
        /// in <c>ENUMERABLE_RULES</c>: a caller turns it on. It is chosen where a query wants its output
        /// ordered by the group key over an input carrying that collation.
        /// </remarks>
        public static readonly RelOptRule ClrCursorSortedAggregateRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorSortedAggregateRule.Create();

        /// <summary>
        /// The rules registered by default, in Calcite's order.
        /// </summary>
        static readonly IReadOnlyList<RelOptRule> RuleList =
        [
            ClrCursorTableScanRule,
            ClrCursorValuesRule,
            ClrCursorProjectRule,
            ClrCursorFilterRule,
            ClrCursorCalcRule,
            ClrCursorJoinRule,
            ClrCursorMergeJoinRule,
            ClrCursorAsofJoinRule,
            ClrCursorCorrelateRule,
            ClrCursorConditionalCorrelateRule,
            ClrCursorCombineRule,
            ClrCursorAggregateRule,
            ClrCursorUnionRule,
            ClrCursorMergeUnionRule,
            ClrCursorIntersectRule,
            ClrCursorMinusRule,
            ClrCursorSortRule,
            ClrCursorLimitRule,
            ClrCursorRepeatUnionRule,
            ClrCursorTableSpoolRule,
            ClrCursorTableFunctionScanRule,
            ClrCursorCollectRule,
            ClrCursorUncollectRule,
            ClrCursorWindowRule,
            EnumerableToClrCursorConverterRule,
            ClrCursorToEnumerableConverterRule,
        ];

        /// <summary>
        /// The rules that turn a project or a filter into a calc, to be run after <see cref="Rules"/>.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>RelOptRules.CALC_RULES</c>, and a pass of its own for the reason
        /// <c>Programs.standard</c> runs Calcite's as one: a project and a calc cover the same rows and <c>VolcanoCost</c> compares nothing else, so
        /// the rewrite has to be a pass of its own after the planner, and it cannot be a planner rule anyway
        /// because <c>VolcanoPlanner.addRule</c> does not register a <c>TransformationRule</c>'s operand
        /// against a <c>PhysicalNode</c>.
        /// </remarks>
        static readonly IReadOnlyList<RelOptRule> CalcRuleList =
        [
            ClrCursorCalcRule,
            ClrCursorFilterToCalcRule,
            ClrCursorProjectToCalcRule,
            CoreRules.FILTER_TO_CALC,
            CoreRules.PROJECT_TO_CALC,
            CoreRules.CALC_MERGE,
            CoreRules.FILTER_CALC_MERGE,
            CoreRules.PROJECT_CALC_MERGE,
        ];

        /// <summary>
        /// Returns the rules that put a plan into this convention.
        /// </summary>
        /// <returns></returns>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return RuleList;
        }

        /// <summary>
        /// Returns the rules that turn a project or a filter into a calc.
        /// </summary>
        /// <returns></returns>
        public static IReadOnlyList<RelOptRule> CalcRules()
        {
            return CalcRuleList;
        }

    }

}
