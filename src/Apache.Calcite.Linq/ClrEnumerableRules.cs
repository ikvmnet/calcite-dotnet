using System.Collections.Generic;

using Apache.Calcite.Linq.Convert;
using Apache.Calcite.Linq.Rel.Convert;

using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// The rules that put a plan into the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRules</c>, and the same shape: a field per rule, a list of the ones
    /// registered by default, and an accessor returning it. A field rather than a factory call, because a
    /// caller has to be able to name one — to add a rule the list leaves out, or to remove one it holds, both
    /// of which the differential harness does. Two calls to a method returning fresh instances give two rules
    /// that are equal to nothing, and <c>RelOptPlanner.removeRule</c> takes the rule itself.
    /// </remarks>
    public static class ClrEnumerableRules
    {

        /// <summary>
        /// Rule that converts a table scan to a <see cref="Rel.ClrEnumerableTableScan"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableTableScanRule = Rel.Convert.ClrEnumerableTableScanRule.Create();

        /// <summary>
        /// Rule that converts a VALUES to a <see cref="Rel.ClrEnumerableValues"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableValuesRule = Rel.Convert.ClrEnumerableValuesRule.Create();

        /// <summary>
        /// Rule that converts a project to a <see cref="Rel.ClrEnumerableProject"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableProjectRule = Rel.Convert.ClrEnumerableProjectRule.Create();

        /// <summary>
        /// Rule that converts a filter to a <see cref="Rel.ClrEnumerableFilter"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableFilterRule = Rel.Convert.ClrEnumerableFilterRule.Create();

        /// <summary>
        /// Rule that converts a calc to a <see cref="Rel.ClrEnumerableCalc"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableCalcRule = Rel.Convert.ClrEnumerableCalcRule.Create();

        /// <summary>
        /// Rule that converts an aggregate to a <see cref="Rel.ClrEnumerableAggregate"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableAggregateRule = Rel.Convert.ClrEnumerableAggregateRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="Rel.ClrEnumerableHashJoin"/> or a
        /// <see cref="Rel.ClrEnumerableNestedLoopJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableJoinRule = Rel.Convert.ClrEnumerableJoinRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="Rel.ClrEnumerableMergeJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableMergeJoinRule = Rel.Convert.ClrEnumerableMergeJoinRule.Create();

        /// <summary>
        /// Rule that converts an ASOF join to a <see cref="Rel.ClrEnumerableAsofJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableAsofJoinRule = Rel.Convert.ClrEnumerableAsofJoinRule.Create();

        /// <summary>
        /// Rule that converts a correlate to a <see cref="Rel.ClrEnumerableCorrelate"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableCorrelateRule = Rel.Convert.ClrEnumerableCorrelateRule.Create();

        /// <summary>
        /// Rule that converts a conditional correlate to a
        /// <see cref="Rel.ClrEnumerableConditionalCorrelate"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableConditionalCorrelateRule = Rel.Convert.ClrEnumerableConditionalCorrelateRule.Create();

        /// <summary>
        /// Rule that converts a combine to a <see cref="Rel.ClrEnumerableCombine"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableCombineRule = Rel.Convert.ClrEnumerableCombineRule.Create();

        /// <summary>
        /// Rule that converts a union to a <see cref="Rel.ClrEnumerableUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableUnionRule = Rel.Convert.ClrEnumerableUnionRule.Create();

        /// <summary>
        /// Rule that converts a sort over a union to a <see cref="Rel.ClrEnumerableMergeUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableMergeUnionRule = Rel.Convert.ClrEnumerableMergeUnionRule.Create();

        /// <summary>
        /// Rule that converts an intersect to a <see cref="Rel.ClrEnumerableIntersect"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableIntersectRule = Rel.Convert.ClrEnumerableIntersectRule.Create();

        /// <summary>
        /// Rule that converts a minus to a <see cref="Rel.ClrEnumerableMinus"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableMinusRule = Rel.Convert.ClrEnumerableMinusRule.Create();

        /// <summary>
        /// Rule that converts a sort to a <see cref="Rel.ClrEnumerableSort"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableSortRule = Rel.Convert.ClrEnumerableSortRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a <see cref="Rel.ClrEnumerableLimit"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableLimitRule = Rel.Convert.ClrEnumerableLimitRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a
        /// <see cref="Rel.ClrEnumerableLimitSort"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_LIMIT_SORT_RULE</c> is not in
        /// <c>ENUMERABLE_RULES</c>: it is one of the three rule fields Calcite declares and leaves out of
        /// the list, with <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> and
        /// <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c>, and nothing in core turns any of them on. A caller
        /// turns this on to sort only as far as the fetch requires rather than sorting and then discarding.
        /// </remarks>
        public static readonly RelOptRule ClrEnumerableLimitSortRule = Rel.Convert.ClrEnumerableLimitSortRule.Create();

        /// <summary>
        /// Rule that converts a repeat union to a <see cref="Rel.ClrEnumerableRepeatUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableRepeatUnionRule = Rel.Convert.ClrEnumerableRepeatUnionRule.Create();

        /// <summary>
        /// Rule that converts a table spool to a <see cref="Rel.ClrEnumerableTableSpool"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableTableSpoolRule = Rel.Convert.ClrEnumerableTableSpoolRule.Create();

        /// <summary>
        /// Rule that converts a window to a <see cref="Rel.ClrEnumerableWindow"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableWindowRule = Rel.Convert.ClrEnumerableWindowRule.Create();

        /// <summary>
        /// Rule that converts a table function scan to a <see cref="Rel.ClrEnumerableTableFunctionScan"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableTableFunctionScanRule = Rel.Convert.ClrEnumerableTableFunctionScanRule.Create();

        /// <summary>
        /// Rule that converts a collect to a <see cref="Rel.ClrEnumerableCollect"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableCollectRule = Rel.Convert.ClrEnumerableCollectRule.Create();

        /// <summary>
        /// Rule that converts an uncollect to a <see cref="Rel.ClrEnumerableUncollect"/>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableUncollectRule = Rel.Convert.ClrEnumerableUncollectRule.Create();

        /// <summary>
        /// Rule that turns a filter of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableFilterToCalcRule = Rel.Convert.ClrEnumerableFilterToCalcRule.Create();

        /// <summary>
        /// Rule that turns a project of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableProjectToCalcRule = Rel.Convert.ClrEnumerableProjectToCalcRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>EnumerableConvention</c> as one of this convention.
        /// </summary>
        public static readonly RelOptRule EnumerableToClrEnumerableConverterRule = Convert.EnumerableToClrEnumerableConverterRule.Create();

        /// <summary>
        /// Rule that reads a plan of this convention as one of <c>EnumerableConvention</c>.
        /// </summary>
        public static readonly RelOptRule ClrEnumerableToEnumerableConverterRule = Convert.ClrEnumerableToEnumerableConverterRule.Create();

        /// <summary>
        /// Rule that converts an aggregate over a sorted input to a
        /// <see cref="Rel.ClrEnumerableSortedAggregate"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> is not
        /// in <c>ENUMERABLE_RULES</c>: a caller turns it on. It is chosen where a query wants its output
        /// ordered by the group key over an input carrying that collation.
        /// </remarks>
        public static readonly RelOptRule ClrEnumerableSortedAggregateRule = Rel.Convert.ClrEnumerableSortedAggregateRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="Rel.ClrEnumerableBatchNestedLoopJoin"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c> is
        /// not in <c>ENUMERABLE_RULES</c>: a caller turns it on, and chooses the batch size with
        /// <see cref="Rel.Convert.ClrEnumerableBatchNestedLoopJoinRule.Create(int)"/>.
        /// </remarks>
        public static readonly RelOptRule ClrEnumerableBatchNestedLoopJoinRule = Rel.Convert.ClrEnumerableBatchNestedLoopJoinRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>BindableConvention</c> as one of this convention, by interpreting it.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>TO_INTERPRETER</c> is not in
        /// <c>ENUMERABLE_RULES</c>: Calcite registers it from <c>RelOptUtil.registerDefaultRules</c>, which
        /// registers Calcite's own. A caller adds this one to have an interpreted node land here rather than
        /// in <c>EnumerableConvention</c> under a converter.
        /// </remarks>
        public static readonly RelOptRule ClrEnumerableInterpreterRule = Rel.Convert.ClrEnumerableInterpreterRule.Create();

        /// <summary>
        /// The rules registered by default.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableRules.ENUMERABLE_RULES</c>, holding what that holds for the nodes
        /// this convention has, plus the two converters that let one plan hold both conventions. Read through
        /// <see cref="Rules"/>, as Calcite's is read through <c>rules()</c>.
        ///
        /// <para>Membership is Calcite's list, member for member: its 26 less the match and table modify
        /// rules, plus the two converters. Match cannot be written at all; modification is out of scope, so a
        /// plan that writes is left to <c>EnumerableConvention</c>. The three rules Calcite declares as fields
        /// and leaves out of the list — limit-sort, sorted aggregate and batch nested loop join — are left out
        /// here too, and a caller who wants one adds it.</para>
        /// </remarks>
        static readonly IReadOnlyList<RelOptRule> EnumerableRuleList =
        [
            ClrEnumerableTableScanRule,
            ClrEnumerableValuesRule,
            ClrEnumerableProjectRule,
            ClrEnumerableFilterRule,
            ClrEnumerableCalcRule,
            ClrEnumerableAggregateRule,
            ClrEnumerableJoinRule,
            ClrEnumerableMergeJoinRule,
            ClrEnumerableAsofJoinRule,
            ClrEnumerableCorrelateRule,
            ClrEnumerableConditionalCorrelateRule,
            ClrEnumerableCombineRule,
            ClrEnumerableUnionRule,
            ClrEnumerableMergeUnionRule,
            ClrEnumerableIntersectRule,
            ClrEnumerableMinusRule,
            ClrEnumerableSortRule,
            ClrEnumerableLimitRule,
            ClrEnumerableRepeatUnionRule,
            ClrEnumerableTableSpoolRule,
            ClrEnumerableWindowRule,
            ClrEnumerableTableFunctionScanRule,
            ClrEnumerableCollectRule,
            ClrEnumerableUncollectRule,
            EnumerableToClrEnumerableConverterRule,
            ClrEnumerableToEnumerableConverterRule,
        ];

        /// <summary>
        /// The rules that turn a project or a filter into a calc, to be run after <see cref="Rules"/>.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>RelOptRules.CALC_RULES</c>, which is a list of Calcite's own and is run as
        /// its own pass by <c>Programs.calc</c>. They belong where those do: not among the rules that put a
        /// plan into a convention, but in a second pass after the planner has chosen one.
        /// <c>Programs.standard</c> runs them as a hep program, and says so — "second planner pass to do
        /// physical tweaks, this the first time that EnumerableCalcRel is introduced".
        ///
        /// <para>The pass has to be a separate one. A project and a calc cover the same rows, and
        /// <c>VolcanoCost</c> compares nothing but the row count, so neither is ever cheaper and the planner
        /// keeps whichever it saw first. Rewriting unconditionally afterwards is what makes a project's
        /// refusal to implement itself safe.</para>
        /// </remarks>
        /// <para>Rule for rule, and in Calcite's order. The one it holds that this does not is
        /// <c>Bindables.FROM_NONE_RULE</c>, which belongs to the bindable convention and has no counterpart
        /// here.</para>
        static readonly IReadOnlyList<RelOptRule> CalcRuleList =
        [
            ClrEnumerableCalcRule,
            ClrEnumerableFilterToCalcRule,
            ClrEnumerableProjectToCalcRule,
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
        /// <remarks>
        /// The counterpart of <c>EnumerableRules.rules()</c>, and the same list every call, so that a caller
        /// can hand one of these to <c>RelOptPlanner.removeRule</c> and have it removed.
        /// </remarks>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return EnumerableRuleList;
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
