using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.tools;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The planner passes a query has to make to reach the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>Programs.standard</c>, and the same three passes with two differences that are
    /// not preferences.
    ///
    /// <para><b>The calc rules are their own pass.</b> A project and a calc cover the same rows, and
    /// <c>VolcanoCost.isLt</c> compares nothing but the row count — cpu and io are dead code behind
    /// <c>if (true)</c> — so neither is ever cheaper and the planner keeps whichever it saw first. Rewriting
    /// unconditionally afterwards, as a hep pass, is what makes a project's refusal to implement itself safe.
    /// <c>Programs.standard</c> says the same thing about its own: "second planner pass to do physical
    /// tweaks, this the first time that EnumerableCalcRel is introduced".</para>
    ///
    /// <para><b>There is no decorrelation.</b> <c>Programs.standard</c> runs one, and it rewrites a
    /// correlated sub-query into a join before the planner sees it — which is exactly the node
    /// <see cref="Rel.ClrEnumerableCorrelate"/> exists to implement. Leaving it out is what puts a correlate
    /// on a plan at all.</para>
    ///
    /// <para>A caller who wants something else builds its own list; this is the one that matches what the
    /// differential tests measure, which is the only configuration this convention is known to be correct
    /// under.</para>
    /// </remarks>
    public static class ClrEnumerablePrograms
    {

        /// <summary>
        /// Returns the three passes, in order, for <c>Frameworks.ConfigBuilder.programs</c>.
        /// </summary>
        /// <param name="metadataProvider">The provider to use, or <see langword="null"/> for Calcite's default.</param>
        /// <returns></returns>
        public static java.util.List Standard(RelMetadataProvider? metadataProvider = null)
        {
            metadataProvider ??= DefaultRelMetadataProvider.INSTANCE;

            var programs = new java.util.ArrayList(3);
            programs.add(SubQuery(metadataProvider));
            programs.add(Rules());
            programs.add(CalcRules(metadataProvider));

            return programs;
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
        public static Program Rules()
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in ClrEnumerableRules.Rules())
                rules.add(rule);

            return Programs.ofRules(rules);
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
        /// <para>The difference is what a query can fall back to. <see cref="Rules"/> is
        /// <c>Programs.ofRules</c>, and that clears the planner before adding its own, so a node this
        /// convention has no rule for has nowhere to go. Here Calcite's rules survive, and anything unported
        /// is implemented in <c>EnumerableConvention</c> and carried across a converter.</para>
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

            foreach (var rule in ClrEnumerableRules.CalcRules())
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
            foreach (var rule in ClrEnumerableRules.CalcRules())
                rules.add(rule);

            return Programs.hep(rules, true, metadataProvider ?? DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Returns the traits a plan of this convention is asked to end in.
        /// </summary>
        /// <param name="traitSet">The traits the root already carries.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Prepare.getDesiredRootTraitSet</c>: the root's own traits with the convention replaced, then
        /// simplified. The simplify is load bearing — it collapses the composite collation a VALUES of
        /// several rows carries, which the planner would otherwise cast to a single <c>RelCollation</c> and
        /// fail on. The collation is replaced rather than dropped, or <c>SortRemoveRule</c> takes an ORDER BY
        /// away as unwanted.
        /// </remarks>
        public static RelTraitSet DesiredRootTraitSet(RelTraitSet traitSet)
        {
            return traitSet.replace(ClrEnumerableConvention.Instance).simplify();
        }

    }

}
