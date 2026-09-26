using System.Collections.Generic;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The rules that put a plan into the <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRules</c>, and the same shape as <c>ClrEnumerableRules</c>: a field per
    /// rule, a list of the ones registered by default, and an accessor returning it. A field rather than a
    /// factory call, because a caller has to be able to name one to remove it, and
    /// <c>RelOptPlanner.removeRule</c> takes the rule itself.
    ///
    /// <para><b>The list is the nodes written so far, and it is short.</b> Scan, values, calc, the hash,
    /// merge and nested loop joins, sort, limit and union, with a project and a filter that become a calc,
    /// and four converters: two against
    /// <c>EnumerableConvention</c> and two against <c>ClrEnumerableConvention</c>. Everything else one of
    /// those two plans, and a converter carries the rows — the sequence convention's node where it has
    /// one, which is nearly everywhere, since its converter costs no Janino compile and its rows are
    /// already CLR sequences. Each node this convention gains goes into the list as it is written, in
    /// Calcite's order.</para>
    /// </remarks>
    public static class ClrDataCursorRules
    {

        /// <summary>
        /// Rule that converts a table scan to a <see cref="ClrDataCursorTableScan"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorTableScanRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorTableScanRule.Create();

        /// <summary>
        /// Rule that converts a VALUES to a <see cref="ClrDataCursorValues"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorValuesRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorValuesRule.Create();

        /// <summary>
        /// Rule that converts a project to a <see cref="ClrDataCursorProject"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorProjectRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorProjectRule.Create();

        /// <summary>
        /// Rule that converts a filter to a <see cref="ClrDataCursorFilter"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorFilterRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorFilterRule.Create();

        /// <summary>
        /// Rule that converts a calc to a <see cref="ClrDataCursorCalc"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorCalcRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorCalcRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrDataCursorHashJoin"/> or a
        /// <see cref="ClrDataCursorNestedLoopJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorJoinRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorJoinRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrDataCursorMergeJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorMergeJoinRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorMergeJoinRule.Create();

        /// <summary>
        /// Rule that converts a union to a <see cref="ClrDataCursorUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorUnionRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorUnionRule.Create();

        /// <summary>
        /// Rule that converts a sort to a <see cref="ClrDataCursorSort"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorSortRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorSortRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a <see cref="ClrDataCursorLimit"/>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorLimitRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorLimitRule.Create();

        /// <summary>
        /// Rule that turns a filter of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorFilterToCalcRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorFilterToCalcRule.Create();

        /// <summary>
        /// Rule that turns a project of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorProjectToCalcRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorProjectToCalcRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>EnumerableConvention</c> as one of this convention.
        /// </summary>
        public static readonly RelOptRule EnumerableToClrDataCursorConverterRule = Apache.Calcite.Extensions.Adapter.DataCursor.EnumerableToClrDataCursorConverterRule.Create();

        /// <summary>
        /// Rule that reads a plan of this convention as one of <c>EnumerableConvention</c>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorToEnumerableConverterRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorToEnumerableConverterRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>ClrEnumerableConvention</c> as one of this convention.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableToClrDataCursorConverterRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrEnumerableToClrDataCursorConverterRule.Create();

        /// <summary>
        /// Rule that reads a plan of this convention as one of <c>ClrEnumerableConvention</c>.
        /// </summary>
        public static readonly RelOptRule ClrDataCursorToClrEnumerableConverterRule = Apache.Calcite.Extensions.Adapter.DataCursor.ClrDataCursorToClrEnumerableConverterRule.Create();

        /// <summary>
        /// The rules registered by default, in Calcite's order.
        /// </summary>
        static readonly IReadOnlyList<RelOptRule> RuleList =
        [
            ClrDataCursorTableScanRule,
            ClrDataCursorValuesRule,
            ClrDataCursorProjectRule,
            ClrDataCursorFilterRule,
            ClrDataCursorCalcRule,
            ClrDataCursorJoinRule,
            ClrDataCursorMergeJoinRule,
            ClrDataCursorUnionRule,
            ClrDataCursorSortRule,
            ClrDataCursorLimitRule,
            EnumerableToClrDataCursorConverterRule,
            ClrDataCursorToEnumerableConverterRule,
            ClrEnumerableToClrDataCursorConverterRule,
            ClrDataCursorToClrEnumerableConverterRule,
        ];

        /// <summary>
        /// The rules that turn a project or a filter into a calc, to be run after <see cref="Rules"/>.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>RelOptRules.CALC_RULES</c>, for the reason <c>ClrEnumerableRules.CalcRules</c>
        /// gives: a project and a calc cover the same rows and <c>VolcanoCost</c> compares nothing else, so
        /// the rewrite has to be a pass of its own after the planner, and it cannot be a planner rule anyway
        /// because <c>VolcanoPlanner.addRule</c> does not register a <c>TransformationRule</c>'s operand
        /// against a <c>PhysicalNode</c>.
        /// </remarks>
        static readonly IReadOnlyList<RelOptRule> CalcRuleList =
        [
            ClrDataCursorCalcRule,
            ClrDataCursorFilterToCalcRule,
            ClrDataCursorProjectToCalcRule,
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
