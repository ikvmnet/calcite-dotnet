using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.plan;

namespace Apache.Calcite.Extensions.Plan
{

    /// <summary>
    /// The planner set-up this project's conventions need, alongside the one Calcite's own need.
    /// </summary>
    public static class ClrRelOptUtil
    {

        /// <summary>
        /// Registers the rules a planner needs by default: Calcite's, and then the cursor convention's.
        /// </summary>
        /// <param name="planner">The planner to register on.</param>
        /// <param name="enableMaterializations">Whether the materialization rules are registered.</param>
        /// <remarks>
        /// <c>RelOptUtil.registerDefaultRules</c> with this project's convention added after it, and the
        /// whole of the job a caller driving its own planner has. Calcite's own rules stay on: a statement the
        /// cursor convention has no node for is still planned, implemented in <c>EnumerableConvention</c>,
        /// and a converter carries its rows.
        ///
        /// <para><c>enableBindable</c> is not a parameter, and is passed <see langword="false"/>. Calcite
        /// reads that flag only to choose <c>BindableConvention</c> as its own result convention, which is
        /// not a choice available here.</para>
        ///
        /// <para><c>EnumerableRules.TO_INTERPRETER</c> is registered by Calcite's call and
        /// <see cref="Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.ClrCursorInterpreterRule"/> is not
        /// registered here, for the reason it gives: an interpreted node lands in <c>EnumerableConvention</c>
        /// under a converter, and a caller wanting it in the cursor convention adds that rule itself.</para>
        /// </remarks>
        public static void RegisterDefaultRules(RelOptPlanner planner, bool enableMaterializations)
        {
            System.ArgumentNullException.ThrowIfNull(planner);

            RelOptUtil.registerDefaultRules(planner, enableMaterializations, false);

            foreach (var rule in Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRules.Rules())
                planner.addRule(rule);
        }

    }

}
