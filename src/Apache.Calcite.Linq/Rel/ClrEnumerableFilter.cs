using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

using Apache.Calcite.Linq.Runtime;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Filter"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableFilter : Filter, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableFilter"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static ClrEnumerableFilter Create(RelNode input, RexNode condition)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.filter(mq, input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.filter(mq, input)));

            return new ClrEnumerableFilter(cluster, traitSet, input, condition);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="condition"></param>
        public ClrEnumerableFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) :
            base(cluster, traitSet, input, condition)
        {

        }

        /// <inheritdoc />
        public override Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition)
        {
            return new ClrEnumerableFilter(getCluster(), traitSet, input, condition);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A calc is always better, exactly as for <c>EnumerableFilter</c>. See
        /// <see cref="ClrEnumerableProject.Implement"/>.
        /// </remarks>
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            throw new java.lang.UnsupportedOperationException();
        }

    }

}
