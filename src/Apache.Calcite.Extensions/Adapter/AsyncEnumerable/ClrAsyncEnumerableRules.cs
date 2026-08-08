using System.Collections.Generic;


using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// The rules that put a plan into the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRules</c>, and the same shape: a field per rule, a list of the ones
    /// registered by default, and an accessor returning it. A field rather than a factory call, because a
    /// caller has to be able to name one — to add a rule the list leaves out, or to remove one it holds, both
    /// of which the differential harness does. Two calls to a method returning fresh instances give two rules
    /// that are equal to nothing, and <c>RelOptPlanner.removeRule</c> takes the rule itself.
    /// </remarks>
    public static class ClrAsyncEnumerableRules
    {

        /// <summary>
        /// Rule that converts a table scan to a <see cref="ClrAsyncEnumerableTableScan"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableTableScanRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableTableScanRule.Create();

        /// <summary>
        /// Rule that converts a VALUES to a <see cref="ClrAsyncEnumerableValues"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableValuesRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableValuesRule.Create();

        /// <summary>
        /// Rule that converts a project to a <see cref="ClrAsyncEnumerableProject"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableProjectRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableProjectRule.Create();

        /// <summary>
        /// Rule that converts a filter to a <see cref="ClrAsyncEnumerableFilter"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableFilterRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableFilterRule.Create();

        /// <summary>
        /// Rule that converts a calc to a <see cref="ClrAsyncEnumerableCalc"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableCalcRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableCalcRule.Create();

        /// <summary>
        /// Rule that converts an aggregate to a <see cref="ClrAsyncEnumerableAggregate"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableAggregateRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableAggregateRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrAsyncEnumerableHashJoin"/> or a
        /// <see cref="ClrAsyncEnumerableNestedLoopJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableJoinRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableJoinRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrAsyncEnumerableMergeJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableMergeJoinRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableMergeJoinRule.Create();

        /// <summary>
        /// Rule that converts an ASOF join to a <see cref="ClrAsyncEnumerableAsofJoin"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableAsofJoinRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableAsofJoinRule.Create();

        /// <summary>
        /// Rule that converts a correlate to a <see cref="ClrAsyncEnumerableCorrelate"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableCorrelateRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableCorrelateRule.Create();

        /// <summary>
        /// Rule that converts a conditional correlate to a
        /// <see cref="ClrAsyncEnumerableConditionalCorrelate"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableConditionalCorrelateRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableConditionalCorrelateRule.Create();

        /// <summary>
        /// Rule that converts a combine to a <see cref="ClrAsyncEnumerableCombine"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableCombineRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableCombineRule.Create();

        /// <summary>
        /// Rule that converts a union to a <see cref="ClrAsyncEnumerableUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableUnionRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableUnionRule.Create();

        /// <summary>
        /// Rule that converts a sort over a union to a <see cref="ClrAsyncEnumerableMergeUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableMergeUnionRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableMergeUnionRule.Create();

        /// <summary>
        /// Rule that converts an intersect to a <see cref="ClrAsyncEnumerableIntersect"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableIntersectRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableIntersectRule.Create();

        /// <summary>
        /// Rule that converts a minus to a <see cref="ClrAsyncEnumerableMinus"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableMinusRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableMinusRule.Create();

        /// <summary>
        /// Rule that converts a sort to a <see cref="ClrAsyncEnumerableSort"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableSortRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableSortRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a <see cref="ClrAsyncEnumerableLimit"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableLimitRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableLimitRule.Create();

        /// <summary>
        /// Rule that converts a sort carrying an offset or a fetch to a
        /// <see cref="ClrAsyncEnumerableLimitSort"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_LIMIT_SORT_RULE</c> is not in
        /// <c>ENUMERABLE_RULES</c>: it is one of the three rule fields Calcite declares and leaves out of
        /// the list, with <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> and
        /// <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c>, and nothing in core turns any of them on. A caller
        /// turns this on to sort only as far as the fetch requires rather than sorting and then discarding.
        /// </remarks>
        public static readonly RelOptRule ClrAsyncEnumerableLimitSortRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableLimitSortRule.Create();

        /// <summary>
        /// Rule that converts a repeat union to a <see cref="ClrAsyncEnumerableRepeatUnion"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableRepeatUnionRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableRepeatUnionRule.Create();

        /// <summary>
        /// Rule that converts a table spool to a <see cref="ClrAsyncEnumerableTableSpool"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableTableSpoolRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableTableSpoolRule.Create();

        /// <summary>
        /// Rule that converts a window to a <see cref="ClrAsyncEnumerableWindow"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableWindowRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableWindowRule.Create();


        /// <summary>
        /// Rule that converts a collect to a <see cref="ClrAsyncEnumerableCollect"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableCollectRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableCollectRule.Create();

        /// <summary>
        /// Rule that converts an uncollect to a <see cref="ClrAsyncEnumerableUncollect"/>.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableUncollectRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableUncollectRule.Create();

        /// <summary>
        /// Rule that turns a filter of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableFilterToCalcRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableFilterToCalcRule.Create();

        /// <summary>
        /// Rule that turns a project of this convention into a calc.
        /// </summary>
        public static readonly RelOptRule ClrAsyncEnumerableProjectToCalcRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableProjectToCalcRule.Create();

        /// <summary>
        /// Rule that reads a plan of <c>EnumerableConvention</c> as one of this convention.
        /// </summary>
        public static readonly RelOptRule EnumerableToClrEnumerableConverterRule = Apache.Calcite.Extensions.Adapter.Enumerable.EnumerableToClrEnumerableConverterRule.Create();


        /// <summary>
        /// Rule that converts an aggregate over a sorted input to a
        /// <see cref="ClrAsyncEnumerableSortedAggregate"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_SORTED_AGGREGATE_RULE</c> is not
        /// in <c>ENUMERABLE_RULES</c>: a caller turns it on. It is chosen where a query wants its output
        /// ordered by the group key over an input carrying that collation.
        /// </remarks>
        public static readonly RelOptRule ClrAsyncEnumerableSortedAggregateRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableSortedAggregateRule.Create();

        /// <summary>
        /// Rule that converts a join to a <see cref="ClrAsyncEnumerableBatchNestedLoopJoin"/>.
        /// </summary>
        /// <remarks>
        /// Not in what <see cref="Rules"/> returns, because <c>ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE</c> is
        /// not in <c>ENUMERABLE_RULES</c>: a caller turns it on, and chooses the batch size with
        /// <see cref="ClrAsyncEnumerableBatchNestedLoopJoinRule.Create(int)"/>.
        /// </remarks>
        public static readonly RelOptRule ClrAsyncEnumerableBatchNestedLoopJoinRule = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableBatchNestedLoopJoinRule.Create();


        /// <summary>
        /// The rules registered by default.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableRules.ENUMERABLE_RULES</c>, holding what that holds for the nodes
        /// this convention has, plus the two converters that let one plan hold both conventions. Read through
        /// <see cref="Rules"/>, as Calcite's is read through <c>rules()</c>.
        ///
        /// <para>Membership is the synchronous convention's list less what this one cannot cover. Gone are
        /// the two converters -- an IEnumerable behind an awaited interface is async over sync and the
        /// reverse is sync over async, so this convention has none in either direction and a query it cannot
        /// plan whole throws rather than falling back. Gone with them are the interpreter rule, whose
        /// Interpreter is a pull based Enumerable, and the table function scan, whose two paths both hand a
        /// linq4j Enumerable through a generator of Calcite's.</para>
        ///
        /// <para>The repeat union and the table spool are left out although both nodes exist. The spool's
        /// write is fine; the iterative side scans the TransientTable it filled, only Calcite's interpreter
        /// reads one, and with no converter there is nothing to carry those rows. WITH RECURSIVE therefore
        /// does not plan here, and will not until there is an asynchronous transient table.</para>
        ///
        /// <para>What is left is Calcite's list less the match and table modify
        /// rules. Match cannot be written at all; modification is out of scope, so a
        /// plan that writes is left to <c>EnumerableConvention</c>. The three rules Calcite declares as fields
        /// and leaves out of the list — limit-sort, sorted aggregate and batch nested loop join — are left out
        /// here too, and a caller who wants one adds it.</para>
        /// </remarks>
        static readonly IReadOnlyList<RelOptRule> EnumerableRuleList =
        [
            ClrAsyncEnumerableTableScanRule,
            ClrAsyncEnumerableValuesRule,
            ClrAsyncEnumerableProjectRule,
            ClrAsyncEnumerableFilterRule,
            ClrAsyncEnumerableCalcRule,
            ClrAsyncEnumerableAggregateRule,
            ClrAsyncEnumerableJoinRule,
            ClrAsyncEnumerableMergeJoinRule,
            ClrAsyncEnumerableAsofJoinRule,
            ClrAsyncEnumerableCorrelateRule,
            ClrAsyncEnumerableConditionalCorrelateRule,
            ClrAsyncEnumerableCombineRule,
            ClrAsyncEnumerableUnionRule,
            ClrAsyncEnumerableMergeUnionRule,
            ClrAsyncEnumerableIntersectRule,
            ClrAsyncEnumerableMinusRule,
            ClrAsyncEnumerableSortRule,
            ClrAsyncEnumerableLimitRule,
            ClrAsyncEnumerableWindowRule,
            ClrAsyncEnumerableCollectRule,
            ClrAsyncEnumerableUncollectRule,
            EnumerableToClrEnumerableConverterRule,
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
            ClrAsyncEnumerableCalcRule,
            ClrAsyncEnumerableFilterToCalcRule,
            ClrAsyncEnumerableProjectToCalcRule,
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
