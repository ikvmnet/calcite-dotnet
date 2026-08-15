using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
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
        /// Registers the rules a planner needs by default: Calcite's, and then both of this project's
        /// conventions'.
        /// </summary>
        /// <param name="planner">The planner to register on.</param>
        /// <param name="enableMaterializations">Whether the materialization rules are registered.</param>
        /// <remarks>
        /// <c>RelOptUtil.registerDefaultRules</c> with this project's conventions added after it, and the
        /// whole of the job a caller driving its own planner has. Calcite's own rules stay on: a statement
        /// neither Clr convention has a node for is still planned, implemented in
        /// <c>EnumerableConvention</c>, and a converter carries its rows.
        ///
        /// <para><b>Both conventions, always.</b> Which one a statement ends in is decided by the convention
        /// demanded of the root and by nothing here. It has to be that way round: a schema may bring rules
        /// of its own, and there is no telling from here which convention one of them targets, so a planner
        /// carrying half of this project would refuse an adapter aimed at the other for no reason the caller
        /// could see. Registering both is also what makes the two cross-convention converters reachable —
        /// each rule list holds the converter *into* its own convention, whose input a planner carrying one
        /// list can never produce.</para>
        ///
        /// <para><c>enableBindable</c> is not a parameter, and is passed <see langword="false"/>. Calcite
        /// reads that flag only to choose <c>BindableConvention</c> as its own result convention, which is
        /// not a choice available here.</para>
        ///
        /// <para><c>EnumerableRules.TO_INTERPRETER</c> is registered by Calcite's call and its counterpart
        /// is not registered here, for the reason
        /// <see cref="ClrEnumerableRules.ClrEnumerableInterpreterRule"/> gives: an interpreted node lands in
        /// <c>EnumerableConvention</c> under a converter, and a caller wanting it to land in this
        /// convention instead adds that rule itself.</para>
        /// </remarks>
        public static void RegisterDefaultRules(RelOptPlanner planner, bool enableMaterializations)
        {
            System.ArgumentNullException.ThrowIfNull(planner);

            RelOptUtil.registerDefaultRules(planner, enableMaterializations, false);

            foreach (var rule in ClrEnumerableRules.Rules())
                planner.addRule(rule);

            foreach (var rule in ClrAsyncEnumerableRules.Rules())
                planner.addRule(rule);
        }

    }

}
