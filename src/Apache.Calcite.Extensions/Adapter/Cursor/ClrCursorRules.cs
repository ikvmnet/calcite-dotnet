using System.Collections.Generic;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The rules that convert a plan to the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableRules</c>: a field per rule, so that a caller can pass one to
    /// <c>RelOptPlanner.removeRule</c>, and <see cref="Rules"/> returning those registered by default.
    ///
    /// <para><see cref="Rules"/> corresponds to <c>EnumerableRules.ENUMERABLE_RULES</c> plus the two converters
    /// between this convention and <c>EnumerableConvention</c>. It has no table modification or
    /// <c>MATCH_RECOGNIZE</c> rule, because this convention has no such nodes; those are left to
    /// <c>EnumerableConvention</c> and reach this convention through a converter. As in Calcite, the sorted
    /// aggregate, batch nested loop join and limit sort rules are not in the list, and neither is the
    /// interpreter rule; a caller adds them explicitly.</para>
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
        /// Rule that converts a join on two inequalities between the inputs to a
        /// <see cref="ClrCursorIEJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorIEJoinRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorIEJoinRule.Create();

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
        /// Not in <see cref="Rules"/>, as <c>ENUMERABLE_LIMIT_SORT_RULE</c> is not in <c>ENUMERABLE_RULES</c>.
        /// Add it to have a sort with a fetch keep only as many rows as the fetch needs rather than sorting its
        /// whole input.
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
        /// Rule that converts a <see cref="ClrCursorFilter"/> to a <see cref="ClrCursorCalc"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorFilterToCalcRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorFilterToCalcRule.Create();

        /// <summary>
        /// Rule that converts a <see cref="ClrCursorProject"/> to a <see cref="ClrCursorCalc"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorProjectToCalcRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorProjectToCalcRule.Create();

        /// <summary>
        /// Rule that converts a plan of <c>EnumerableConvention</c> to this convention, through an
        /// <see cref="EnumerableToClrCursorConverter"/>.
        /// </summary>
        public static readonly RelOptRule EnumerableToClrCursorConverterRule = Apache.Calcite.Extensions.Adapter.Cursor.EnumerableToClrCursorConverterRule.Create();

        /// <summary>
        /// Rule that converts a plan of this convention to <c>EnumerableConvention</c>, through a
        /// <see cref="ClrCursorToEnumerableConverter"/>.
        /// </summary>
        public static readonly RelOptRule ClrCursorToEnumerableConverterRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorToEnumerableConverterRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrCursorBatchNestedLoopJoin"/>.
        /// </summary>
        /// <remarks>
        /// Not in <see cref="Rules"/>, as <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c> is not in
        /// <c>ENUMERABLE_RULES</c>. To choose the batch size, create the rule with
        /// <see cref="Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorBatchNestedLoopJoinRule.Create(int)"/> instead.
        /// </remarks>
        public static readonly RelOptRule ClrCursorBatchNestedLoopJoinRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorBatchNestedLoopJoinRule.Create();

        /// <summary>
        /// Rule that converts a plan of <c>BindableConvention</c> to this convention by interpreting it.
        /// </summary>
        /// <remarks>
        /// Not in <see cref="Rules"/>, as <c>EnumerableRules.TO_INTERPRETER</c> is not in
        /// <c>ENUMERABLE_RULES</c>; <c>RelOptUtil.registerDefaultRules</c> registers Calcite's. Add this rule to
        /// have an interpreted plan converted directly to this convention rather than to
        /// <c>EnumerableConvention</c>.
        /// </remarks>
        public static readonly RelOptRule ClrCursorInterpreterRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorInterpreterRule.Create();

        /// <summary>
        /// Rule that converts an aggregate over a sorted input to a
        /// <see cref="ClrCursorSortedAggregate"/>.
        /// </summary>
        /// <remarks>
        /// Not in <see cref="Rules"/>, as <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> is not in
        /// <c>ENUMERABLE_RULES</c>.
        /// </remarks>
        public static readonly RelOptRule ClrCursorSortedAggregateRule = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorSortedAggregateRule.Create();

        /// <summary>
        /// The rules <see cref="Rules"/> returns.
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
            ClrCursorIEJoinRule,
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
        /// The rules <see cref="CalcRules"/> returns: <c>RelOptRules.CALC_RULES</c> without
        /// <c>Bindables.FROM_NONE_RULE</c>, with this convention's three calc rules in place of Calcite's.
        /// </summary>
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
        /// Returns the rules to register on a planner to convert a plan to this convention.
        /// </summary>
        /// <returns>The default rules, the counterpart of <c>EnumerableRules.ENUMERABLE_RULES</c>.</returns>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return RuleList;
        }

        /// <summary>
        /// Returns the rules that convert projects and filters to calcs and merge calcs.
        /// </summary>
        /// <returns>The calc rules, the counterpart of <c>RelOptRules.CALC_RULES</c>.</returns>
        /// <remarks>
        /// Run these as a hep pass after the planner, as <c>Programs.standard</c> runs Calcite's; a plan that
        /// still holds a <see cref="ClrCursorProject"/> or <see cref="ClrCursorFilter"/> cannot be implemented.
        /// They do not work as planner rules: <c>VolcanoPlanner</c> does not match a
        /// <c>TransformationRule</c>'s operand against a physical node, and the planner's cost model, which
        /// compares row counts only, never prefers a calc over the nodes it replaces.
        /// </remarks>
        public static IReadOnlyList<RelOptRule> CalcRules()
        {
            return CalcRuleList;
        }

    }

}
