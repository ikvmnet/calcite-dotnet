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
    /// The counterpart of <c>EnumerableRules</c>, holding the same rules for the same nodes, plus the
    /// converter that lets a plan take rows from <c>EnumerableConvention</c> for whatever is not here yet.
    /// </remarks>
    public static class ClrEnumerableRules
    {

        /// <summary>
        /// Returns the rules that put a plan into this convention.
        /// </summary>
        /// <returns></returns>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return
            [
                ClrEnumerableTableScanRule.Create(),
                ClrEnumerableValuesRule.Create(),
                ClrEnumerableProjectRule.Create(),
                ClrEnumerableFilterRule.Create(),
                ClrEnumerableCalcRule.Create(),
                ClrEnumerableSortRule.Create(),
                ClrEnumerableLimitRule.Create(),
                EnumerableToClrEnumerableConverterRule.Create(),
            ];
        }

        /// <summary>
        /// Returns the rules that turn a project or a filter into a calc, to be run after <see cref="Rules"/>.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>RelOptRules.CALC_RULES</c>, and they belong where those do: not in the
        /// convention's rule set, but in a second pass after the planner has chosen a plan.
        /// <c>Programs.standard</c> runs them as a hep program, and says so — "second planner pass to do
        /// physical tweaks, this the first time that EnumerableCalcRel is introduced".
        ///
        /// <para>The pass has to be a separate one. A project and a calc cover the same rows, and
        /// <c>VolcanoCost</c> compares nothing but the row count, so neither is ever cheaper and the planner
        /// keeps whichever it saw first. Rewriting unconditionally afterwards is what makes a project's
        /// refusal to implement itself safe.</para>
        /// </remarks>
        public static IReadOnlyList<RelOptRule> CalcRules()
        {
            return
            [
                ClrEnumerableCalcRule.Create(),
                ClrEnumerableProjectToCalcRule.Create(),
                ClrEnumerableFilterToCalcRule.Create(),
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.CALC_MERGE,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
            ];
        }

    }

}
