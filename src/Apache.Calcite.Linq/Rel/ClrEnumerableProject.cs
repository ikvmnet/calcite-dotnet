using Apache.Calcite.Linq.Runtime;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Project"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableProject : Project, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableProject"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="projects"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public static ClrEnumerableProject Create(RelNode input, java.util.List projects, RelDataType rowType)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSet()
                .replace(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.project(mq, input, projects)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.project(mq, input, projects)));

            return new ClrEnumerableProject(cluster, traitSet, input, projects, rowType);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="projects"></param>
        /// <param name="rowType"></param>
        public ClrEnumerableProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, java.util.List projects, RelDataType rowType) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, projects, rowType, com.google.common.collect.ImmutableSet.of())
        {

        }

        /// <inheritdoc />
        public override Project copy(RelTraitSet traitSet, RelNode input, java.util.List projects, RelDataType rowType)
        {
            return new ClrEnumerableProject(getCluster(), traitSet, input, projects, rowType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForProject(
                required,
                getProjects(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet())!;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrEnumerableTraitsUtils.DeriveTraitsForProject(
                childTraits,
                childId,
                getProjects(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet())!;
        }

        /// <inheritdoc />
        /// <remarks>
        /// A calc is always better, exactly as for <c>EnumerableProject</c>, because it carries the filter and
        /// the projection in one pass. Reaching here means the calc rules were not registered: they are not
        /// part of a convention's rule set in Calcite either, but of <c>RelOptRules.CALC_RULES</c>.
        /// </remarks>
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            throw new java.lang.UnsupportedOperationException();
        }

    }

}
