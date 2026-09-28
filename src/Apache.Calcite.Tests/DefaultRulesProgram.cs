using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A program that clears the planner, registers the rules <c>CalcitePrepareImpl</c> registers plus any it
    /// is given, and plans.
    /// </summary>
    /// <param name="extra">The rules of this convention, or an empty list to plan as Calcite alone would.</param>
    /// <param name="topDown">Whether to optimise top down, which is what calls a physical node's
    /// <c>passThroughTraits</c>, <c>deriveTraits</c> and <c>getDeriveMode</c>.</param>
    /// <param name="excludeMergeJoin">Whether to remove <c>EnumerableMergeJoinRule</c> after the default rules
    /// are registered. See the remarks.</param>
    /// <param name="excludeHashJoin">Whether to remove <c>EnumerableJoinRule</c> after the default rules are
    /// registered, leaving the merge join as the only way to join.</param>
    /// <param name="add">Rules to register alongside Calcite's, for a test that needs one Calcite ships but
    /// does not register by default, such as <c>JOIN_TO_CORRELATE</c>.</param>
    /// <param name="remove">Rules of either convention to remove once everything is registered.</param>
    /// <remarks>
    /// The default rules come from <c>RelOptUtil.registerDefaultRules</c>, as <c>CalcitePrepareImpl</c>
    /// calls it. The rule lists it draws on are package private in <c>RelOptRules</c>, so this calls the
    /// method with the planner a <see cref="Program"/> is handed rather than copying them. Materializations and
    /// bindable are off, as they are for a plain query; bindable would otherwise offer itself as a root
    /// convention. The rest of the body follows <c>Programs.RuleSetProgram.run</c>.
    ///
    /// <para><c>excludeMergeJoin</c> works around a Calcite defect. <c>EnumerableMergeJoin.passThroughTraits</c>
    /// returns the required trait set, convention included, and <c>PhysicalNode.passThrough</c> copies the node
    /// onto it. With both conventions in one planner and top-down optimisation on, a <c>CLR_CURSOR</c> subset
    /// then receives an <c>EnumerableMergeJoin</c> in <c>CLR_CURSOR</c>, which the planner refuses because it
    /// does not implement <c>ClrCursorRel</c>. <c>TopDownRuleDriver.convert</c> asserts that a pass-through
    /// keeps the convention, so the defect shows only with Java assertions off.</para>
    /// </remarks>
    public sealed class DefaultRulesProgram(java.util.List extra, bool topDown = false, bool excludeMergeJoin = false, bool excludeHashJoin = false, RelOptRule[]? add = null, RelOptRule[]? remove = null) : Program
    {

        /// <inheritdoc />
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
        {
            planner.clear();

            // Calcite leaves top-down optimisation off by default (CalciteSystemProperty.TOPDOWN_OPT)
            if (topDown && planner is org.apache.calcite.plan.volcano.VolcanoPlanner volcano)
                volcano.setTopDownOpt(true);

            RelOptUtil.registerDefaultRules(planner, false, false);

            if (excludeMergeJoin)
                planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);

            // forces a sort over an input the planner would otherwise hash
            if (excludeHashJoin)
                planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

            foreach (var rule in add ?? [])
                planner.addRule(rule);

            for (int i = 0; i < extra.size(); i++)
                planner.addRule((RelOptRule)extra.get(i));

            // last, so a caller can remove a rule of either convention; Calcite's own tests do the same from
            // Hook.PLANNER, which also runs after registration
            foreach (var rule in remove ?? [])
                planner.removeRule(rule);

            for (int i = 0; i < materializations.size(); i++)
                planner.addMaterialization((RelOptMaterialization)materializations.get(i));

            for (int i = 0; i < lattices.size(); i++)
                planner.addLattice((RelOptLattice)lattices.get(i));

            if (rel.getTraitSet().equals(requiredOutputTraits) == false)
                rel = planner.changeTraits(rel, requiredOutputTraits);

            planner.setRoot(rel);

            return planner.findBestExp();
        }

    }

}
