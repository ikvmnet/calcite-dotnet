using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.tools;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// The planner passes a query has to make to reach the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>Programs.standard</c>, and the same passes with three differences that are not
    /// preferences.
    ///
    /// <para><b>The planner pass registers rules, where Calcite's plans with the ones already there.</b>
    /// <c>Programs.standard</c> can plan with a bare planner because <c>RelOptUtil.registerDefaultRules</c>
    /// has already put Calcite's rules on it; nothing has heard of this convention, so <see cref="Rules"/>
    /// registers. What it registers is Calcite's default rules <em>and then</em> this convention's, so that
    /// the logical rewrites belonging to no convention — the ones AVG, DISTINCT aggregates and OVER windows
    /// each need before any planner sees them — are still there.</para>
    ///
    /// <para><b>The calc rules are their own pass.</b> A project and a calc cover the same rows, and
    /// <c>VolcanoCost.isLt</c> compares nothing but the row count — cpu and io are dead code behind
    /// <c>if (true)</c> — so neither is ever cheaper and the planner keeps whichever it saw first. Rewriting
    /// unconditionally afterwards, as a hep pass, is what makes a project's refusal to implement itself safe.
    /// <c>Programs.standard</c> says the same thing about its own: "second planner pass to do physical
    /// tweaks, this the first time that EnumerableCalcRel is introduced".</para>
    ///
    /// <para><b>The decorrelation is Calcite's and is run.</b> It was left out for a while, on the grounds
    /// that it rewrites a correlated sub-query into a join and so would leave
    /// <see cref="ClrAsyncEnumerableCorrelate"/> unreachable. That is not so, and was measured: a scalar
    /// sub-query and an EXISTS do become joins, but an UNNEST over a correlation variable cannot be
    /// decorrelated and keeps its correlate, which is how Calcite reaches its own
    /// <c>EnumerableCorrelate</c> under <c>Programs.standard</c> too. Leaving the pass out therefore bought
    /// nothing and cost every correlated sub-query the join Calcite would have given it.</para>
    ///
    /// <para>A caller who wants something else builds its own list; this is the one that matches what the
    /// differential tests measure, which is the only configuration this convention is known to be correct
    /// under.</para>
    /// </remarks>
    public static class ClrAsyncEnumerablePrograms
    {

        /// <summary>
        /// Returns the program a query is planned with, for <c>Frameworks.ConfigBuilder.programs</c>.
        /// </summary>
        /// <param name="metadataProvider">The provider to use, or <see langword="null"/> for Calcite's default.</param>
        /// <returns></returns>
        /// <remarks>
        /// One <see cref="Program"/>, as <c>Programs.standard</c> is one, so that a caller drives it the way
        /// a caller drives Calcite's: <c>planner.transform(0, traits, logical)</c>, once. The passes inside
        /// are a <c>Programs.sequence</c>, which is also what <c>standard</c> is, and they are
        /// <c>standard</c>'s own six in <c>standard</c>'s order. The prepare pipeline sequences the same
        /// list.
        ///
        /// <para>The calc pass is <see cref="PlannerCalcRules"/> rather than <see cref="CalcRules"/>,
        /// because <see cref="Rules"/> leaves Calcite's rules on the planner and so a plan can hold nodes of
        /// either convention. A project and a filter refuse to implement themselves in both, so running only
        /// this convention's calc rules would leave an <c>EnumerableProject</c> standing and it throws when
        /// the plan is implemented.</para>
        /// </remarks>
        public static Program Standard(RelMetadataProvider? metadataProvider = null)
        {
            metadataProvider ??= DefaultRelMetadataProvider.INSTANCE;

            // Programs.standard's six, in its order. Every one but the last two is Calcite's own, taken as
            // it is; the prepare pipeline sequences the same list.
            return Programs.sequence(
                SubQuery(metadataProvider),
                Programs.decorrelate(),
                Programs.measure(metadataProvider),
                Programs.trim(),
                Rules(),
                PlannerCalcRules(metadataProvider));
        }

        /// <summary>
        /// Returns the pass that expands a sub-query, which has to run before the planner.
        /// </summary>
        /// <param name="metadataProvider">The provider to use, or <see langword="null"/> for Calcite's default.</param>
        /// <returns></returns>
        /// <remarks>
        /// A filter carrying a sub-query is refused by both conventions, so a sub-query has to become a
        /// correlate or a join before a rule can match it.
        /// </remarks>
        public static Program SubQuery(RelMetadataProvider? metadataProvider = null)
        {
            return Programs.subQuery(metadataProvider ?? DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Returns the pass that puts a plan into this convention.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>Programs.standard</c>'s planner pass installs no rules at all: it sets the root and calls
        /// <c>findBestExp</c> on whatever the planner already carries, which for a planner Calcite built is
        /// everything <c>RelOptUtil.registerDefaultRules</c> put there. This pass cannot do only that,
        /// because a planner Calcite built has never heard of this convention and would have nothing to plan
        /// into — measured: a pass that installs nothing cannot plan <c>SELECT x FROM t WHERE x &gt; 1</c>.
        ///
        /// <para>So it registers, and what it registers is Calcite's own default rules and then this
        /// convention's — not this convention's alone. That distinction is the whole of it.
        /// <c>Programs.ofRules</c> would clear the planner and leave only what it was given, and the rules
        /// that go missing are the logical rewrites that are nobody's convention:
        /// <c>AGGREGATE_REDUCE_FUNCTIONS</c>, without which AVG, STDDEV and the variances cannot plan at all;
        /// <c>AGGREGATE_EXPAND_DISTINCT_AGGREGATES</c>, without which no DISTINCT aggregate can; and
        /// <c>PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW</c>, without which no OVER window can. Keeping Calcite's
        /// rules is also what lets a node this convention has no rule for be implemented in
        /// <c>EnumerableConvention</c> and carried across a converter, rather than having nowhere to go.</para>
        ///
        /// <para><c>RelOptRules</c>' lists are package private, so they cannot be copied out;
        /// <c>registerDefaultRules</c> is public and takes the planner, and a <see cref="Program"/> is handed
        /// the planner, which is the way in. Materializations and bindable are both off, as they are for a
        /// plain query.</para>
        /// </remarks>
        public static Program Rules()
        {
            return new RuleSetProgram();
        }

        /// <summary>
        /// Registers what Calcite registers, adds this convention's rules, and plans.
        /// </summary>
        /// <remarks>
        /// <c>Programs.RuleSetProgram.run</c> line for line, with the rule registration swapped for the two
        /// steps <see cref="Rules"/> describes. It is the pass the differential tests plan under, so what is
        /// shipped and what is measured are the same configuration.
        /// </remarks>
        sealed class RuleSetProgram : Program
        {

            /// <inheritdoc />
            public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
            {
                planner.clear();

                RelOptUtil.registerDefaultRules(planner, false, false);

                foreach (var rule in ClrAsyncEnumerableRules.Rules())
                    planner.addRule(rule);

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

        /// <summary>
        /// Returns the pass that plans with the rules the planner already carries, adding none and removing
        /// none.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// What <see cref="Rules"/> does to a planner that holds nothing else, this does to one that has been
        /// set up already — <c>CalcitePrepare</c>'s, which has Calcite's own rules on it. It is the shape of
        /// the first program in <c>Programs.standard</c>, which likewise installs no rules and plans with
        /// what is there.
        ///
        /// <para>The difference is who installs. <see cref="Rules"/> registers Calcite's default rules and
        /// then this convention's, because the planner it is handed has neither; this one installs nothing,
        /// because the planner it is handed has both already. Either way a node this convention has no rule
        /// for is implemented in <c>EnumerableConvention</c> and carried across a converter.</para>
        /// </remarks>
        public static Program PlannerRules()
        {
            return new PlannerRulesProgram();
        }

        /// <summary>
        /// Returns the calc pass for a plan that may hold nodes of either convention.
        /// </summary>
        /// <param name="metadataProvider">The provider to use, or <see langword="null"/> for Calcite's default.</param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="CalcRules"/> with <c>RelOptRules.CALC_RULES</c> added, which is what
        /// <c>Programs.calc</c> runs on its own.
        ///
        /// <para>Both halves are needed for the same reason. A project or a filter refuses to implement
        /// itself, in this convention and in Calcite's alike, because a calc carries both in one pass; the
        /// calc pass is what makes that refusal safe. Running only this convention's calc rules leaves
        /// Calcite's <c>EnumerableFilter</c> standing, and it throws when the plan is implemented.</para>
        /// </remarks>
        public static Program PlannerCalcRules(RelMetadataProvider? metadataProvider = null)
        {
            var rules = new java.util.ArrayList();

            foreach (var rule in ClrAsyncEnumerableRules.CalcRules())
                rules.add(rule);

            for (var i = RelOptRules.CALC_RULES.iterator(); i.hasNext();)
                rules.add(i.next());

            return Programs.hep(rules, true, metadataProvider ?? DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Plans with the rules already on the planner.
        /// </summary>
        sealed class PlannerRulesProgram : Program
        {

            /// <inheritdoc />
            public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
            {
                for (var i = materializations.iterator(); i.hasNext();)
                    planner.addMaterialization((RelOptMaterialization)i.next());

                for (var i = lattices.iterator(); i.hasNext();)
                    planner.addLattice((RelOptLattice)i.next());

                planner.setRoot(rel);
                var root = rel.getTraitSet().equals(requiredOutputTraits) ? rel : planner.changeTraits(rel, requiredOutputTraits);
                planner.setRoot(root);

                return planner.chooseDelegate().findBestExp();
            }

        }

        /// <summary>
        /// Returns the pass that rewrites every project and filter into a calc.
        /// </summary>
        /// <param name="metadataProvider">The provider to use, or <see langword="null"/> for Calcite's default.</param>
        /// <returns></returns>
        public static Program CalcRules(RelMetadataProvider? metadataProvider = null)
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrAsyncEnumerableRules.CalcRules())
                rules.add(rule);

            return Programs.hep(rules, true, metadataProvider ?? DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Returns the traits a plan of this convention is asked to end in.
        /// </summary>
        /// <param name="traitSet">The traits the root already carries — the logical root's own, not an empty
        /// set. <c>Prepare.getDesiredRootTraitSet</c> likewise builds from <c>root.rel.getTraitSet()</c>, and
        /// for the same reason: an empty set asks for no collation, and <c>SortRemoveRule</c> — which arrives
        /// with Calcite's abstract rules — then takes an ORDER BY away as unwanted.</param>
        /// <returns></returns>
        /// <remarks>
        /// For a caller driving the planner itself, which has a trait set rather than a <c>RelRoot</c>. The
        /// prepare pipeline does not come through here — <c>ClrPrepare.GetDesiredRootTraitSet</c> is
        /// <c>Prepare.getDesiredRootTraitSet</c> and additionally replaces the collation the query asked for,
        /// which is a property of the root and not of a trait set.
        ///
        /// <para>The simplify is load bearing — it collapses the composite collation a VALUES of several rows
        /// carries, which the planner would otherwise cast to a single <c>RelCollation</c> and fail on.</para>
        /// </remarks>
        public static RelTraitSet DesiredRootTraitSet(RelTraitSet traitSet)
        {
            return traitSet.replace(ClrAsyncEnumerableConvention.Instance).simplify();
        }

    }

}
