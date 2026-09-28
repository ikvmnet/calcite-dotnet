
using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Project"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableProject</c>. The node takes part in planning but cannot be implemented:
    /// <see cref="ClrCursorProjectToCalcRule"/>, in <see cref="ClrCursorRules.CalcRules"/>, replaces it with a
    /// <see cref="ClrCursorCalc"/> before the plan is implemented.
    /// </remarks>
    public class ClrCursorProject : Project, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorProject"/>, deriving its collation from its input and expressions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="projects">The projected expressions, a list of <see cref="org.apache.calcite.rex.RexNode"/>.</param>
        /// <param name="rowType">The output row type.</param>
        /// <returns>The new project.</returns>
        public static ClrCursorProject Create(RelNode input, java.util.List projects, RelDataType rowType)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSet()
                .replace(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.project(mq, input, projects)));

            return new ClrCursorProject(cluster, traitSet, input, projects, rowType);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> derives the trait set; this constructor takes it as
        /// given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="projects">The projected expressions, a list of <see cref="org.apache.calcite.rex.RexNode"/>.</param>
        /// <param name="rowType">The output row type.</param>
        public ClrCursorProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, java.util.List projects, RelDataType rowType) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, projects, rowType, com.google.common.collect.ImmutableSet.of())
        {

        }

        /// <inheritdoc />
        public override Project copy(RelTraitSet traitSet, RelNode input, java.util.List projects, RelDataType rowType)
        {
            return new ClrCursorProject(getCluster(), traitSet, input, projects, rowType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForProject(
                required,
                getProjects(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrCursorTraitsUtils.DeriveTraitsForProject(
                childTraits,
                childId,
                getProjects(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet());
        }

        /// <inheritdoc />
        /// <remarks>
        /// Always throws, as <c>EnumerableProject.implement</c> does. Run <see cref="ClrCursorRules.CalcRules"/>
        /// as a hep pass after the planner so that every project becomes a <see cref="ClrCursorCalc"/>.
        /// </remarks>
        /// <exception cref="java.lang.UnsupportedOperationException">Always.</exception>
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            throw new java.lang.UnsupportedOperationException(
                "ClrCursorProject cannot implement itself, exactly as EnumerableProject cannot: a calc " +
                "carries the filter and the projection in one pass and is always better. Reaching here means " +
                "ClrCursorRules.CalcRules() was not run as a hep pass after the planner. It cannot be run " +
                "on the planner instead: VolcanoPlanner.addRule does not register a TransformationRule's " +
                "operand against a PhysicalNode, and every node of this convention is one.");
        }

    }

}
