using Apache.Calcite.Extensions.Adapter.Enumerable;

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
    public class ClrCursorProject : Project, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorProject"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="projects"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
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
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="projects"></param>
        /// <param name="rowType"></param>
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
            return ClrEnumerableTraitsUtils.PassThroughTraitsForProject(
                required,
                getProjects(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrEnumerableTraitsUtils.DeriveTraitsForProject(
                childTraits,
                childId,
                getProjects(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet());
        }

        /// <inheritdoc />
        /// <remarks>
        /// A calc is always better, exactly as for <c>EnumerableProject</c>, because it carries the filter and
        /// the projection in one pass. Reaching here means the calc rules were not run: they are not part of
        /// a convention's rule set in Calcite either, but of <c>RelOptRules.CALC_RULES</c>, which
        /// <c>Programs.standard</c> runs as a hep pass after the planner.
        /// </remarks>
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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
