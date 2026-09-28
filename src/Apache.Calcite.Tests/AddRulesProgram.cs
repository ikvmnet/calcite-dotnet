using System.Collections.Generic;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A program that registers rules on the planner and returns the plan unchanged.
    /// </summary>
    /// <param name="rules">The rules to register.</param>
    /// <remarks>
    /// For a test driving a <c>Frameworks</c> planner, this does what <c>ClrPrepareImpl.CreatePlanner</c> does
    /// for a prepared statement. <c>PlannerImpl</c> registers only Calcite's default rules, and the planner pass
    /// of <c>Programs.standard</c> installs none, so a test runs this first and then <c>Programs.standard</c>
    /// itself.
    ///
    /// <para><see cref="DefaultRulesProgram"/> instead clears the planner and registers Calcite's rules too, for
    /// a test that has to control exactly which rules exist.</para>
    /// </remarks>
    public sealed class AddRulesProgram(IReadOnlyList<RelOptRule> rules) : Program
    {

        /// <inheritdoc />
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits, java.util.List materializations, java.util.List lattices)
        {
            foreach (var rule in rules)
                planner.addRule(rule);

            return rel;
        }

    }

}
