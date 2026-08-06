using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// A planner pass that registers what Calcite itself registers, plus whatever else it is given.
    /// </summary>
    /// <param name="extra">The rules of this convention, or nothing to plan as Calcite alone would.</param>
    /// <param name="topDown">Whether to optimise top down, which is the only thing that calls a physical
    /// node's <c>passThroughTraits</c>, <c>deriveTraits</c> and <c>getDeriveMode</c>.</param>
    /// <param name="excludeMergeJoin">Whether to drop <c>EnumerableMergeJoinRule</c> after the default rules
    /// are registered. See the remarks.</param>
    /// <param name="add">Rules to register alongside Calcite's, for a test that needs one Calcite ships but
    /// does not register — <c>JOIN_TO_CORRELATE</c>, say.</param>
    /// <param name="remove">Rules to take away once everything is registered, of either convention.</param>
    /// <remarks>
    /// Listing rules by hand is what the harness used to do, and it was not Calcite's planner but a fraction
    /// of one. <c>CalcitePrepareImpl</c> calls <c>RelOptUtil.registerDefaultRules</c>, which adds the
    /// abstract, abstract-relational and base rule sets — and, under
    /// <c>CalciteSystemProperty.ENABLE_ENUMERABLE</c>, <c>EnumerableRules.ENUMERABLE_RULES</c> itself. Those
    /// lists are package private in <c>RelOptRules</c> so they cannot be copied; <c>registerDefaultRules</c>
    /// is public and takes the planner, and a <see cref="Program"/> is handed the planner, which is the way
    /// in.
    ///
    /// <para>The body is <c>Programs.RuleSetProgram.run</c> line for line, with the rule registration
    /// swapped. Materializations and bindable are both off, as they are for a plain query: bindable would
    /// otherwise offer itself as a root convention and change what is being compared.</para>
    ///
    /// <para><c>excludeMergeJoin</c> exists for one reason, and it is a defect of Calcite's rather than a
    /// preference. <c>EnumerableMergeJoin.passThroughTraits</c> returns <c>Pair.of(required, …)</c> — the
    /// trait set it was handed, convention and all — and <c>PhysicalNode.passThrough</c> then copies the node
    /// onto it. With both conventions in one planner and top-down optimisation on, a CLR_ENUMERABLE subset
    /// asks it to pass through and gets an <c>EnumerableMergeJoin</c> wearing CLR_ENUMERABLE, which the
    /// planner then refuses: "has calling-convention CLR_ENUMERABLE but does not implement
    /// ClrEnumerableRel". <c>TopDownRuleDriver.convert</c> asserts that a pass-through preserves the
    /// convention, so Calcite means for it to; only assertions being off lets it through. Nothing of ours can
    /// do this — every trait method here builds from <c>getTraitSet()</c> — and a merge join written here
    /// must not follow Calcite's text on that line.</para>
    /// </remarks>
    public sealed class DefaultRulesProgram(java.util.List extra, bool topDown = false, bool excludeMergeJoin = false, bool excludeHashJoin = false, RelOptRule[]? add = null, RelOptRule[]? remove = null) : Program
    {

        /// <inheritdoc />
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
        {
            planner.clear();

            // top-down optimisation is what asks a physical node to pass a trait down to its inputs or to
            // derive one from them; Calcite leaves it off by default (CalciteSystemProperty.TOPDOWN_OPT)
            if (topDown && planner is org.apache.calcite.plan.volcano.VolcanoPlanner volcano)
                volcano.setTopDownOpt(true);

            RelOptUtil.registerDefaultRules(planner, false, false);

            if (excludeMergeJoin)
                planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);

            // leaves the merge join as the only way to join, which is how a sort is forced over an input the
            // planner would otherwise hash
            if (excludeHashJoin)
                planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

            foreach (var rule in add ?? [])
                planner.addRule(rule);

            for (int i = 0; i < extra.size(); i++)
                planner.addRule((RelOptRule)extra.get(i));

            // last, so that a caller can take away one of this convention's rules as well as one of
            // Calcite's. Calcite's own tests do this from Hook.PLANNER, which likewise runs after
            // registration: a node is only reached by taking away whatever the planner would otherwise
            // prefer, and with two conventions in one planner that is sometimes a rule of ours
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
