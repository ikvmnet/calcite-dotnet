using Apache.Calcite.Extensions.Adapter.Cursor;

using org.apache.calcite.plan;

namespace Apache.Calcite.Extensions.Plan
{

    /// <summary>
    /// Planner set-up for the cursor convention.
    /// </summary>
    public static class ClrRelOptUtil
    {

        /// <summary>
        /// Registers Calcite's default rules on a planner, followed by the cursor convention's rules.
        /// </summary>
        /// <param name="planner">The planner to register on.</param>
        /// <param name="enableMaterializations">Whether the materialization rules are registered.</param>
        /// <exception cref="System.ArgumentNullException"><paramref name="planner"/> is
        /// <see langword="null"/>.</exception>
        /// <remarks>
        /// Calls <c>RelOptUtil.registerDefaultRules</c> with <c>enableBindable</c> set to
        /// <see langword="false"/>, then adds <see cref="ClrCursorRules.Rules"/>. Calcite's rules stay
        /// registered, so a node the cursor convention cannot implement is planned in
        /// <c>EnumerableConvention</c> and connected by a converter.
        ///
        /// <para>An interpreted node is planned in <c>EnumerableConvention</c>, because
        /// <see cref="ClrCursorRules.ClrCursorInterpreterRule"/> is not registered here; a caller that wants it
        /// in the cursor convention adds that rule itself.</para>
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
