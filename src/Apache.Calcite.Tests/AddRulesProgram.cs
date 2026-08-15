using System.Collections.Generic;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A pass that puts rules on the planner and changes the plan not at all.
    /// </summary>
    /// <param name="rules">The rules to register.</param>
    /// <remarks>
    /// What <c>ClrPrepareImpl.CreatePlanner</c> does for a prepared statement, for a test driving a
    /// <c>Frameworks</c> planner instead. <c>PlannerImpl</c> registers Calcite's default rules and has never
    /// heard of this convention, and <c>Programs.standard</c>'s planner pass installs nothing and plans with
    /// whatever the planner already carries. So the rules go on in a pass of their own, in front, and the
    /// program under test is then <c>Programs.standard</c> itself — the same one the prepare pipeline runs,
    /// rather than a hand-written list of the passes it makes.
    ///
    /// <para><see cref="DefaultRulesProgram"/> is the other half of this: it clears the planner and registers
    /// Calcite's own rules as well, for a harness that has to control exactly which rules exist.</para>
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
