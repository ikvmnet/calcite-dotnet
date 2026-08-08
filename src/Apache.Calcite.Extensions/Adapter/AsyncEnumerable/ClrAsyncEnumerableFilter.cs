using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Filter"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrAsyncEnumerableFilter : Filter, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableFilter"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableFilter Create(RelNode input, RexNode condition)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.filter(mq, input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.filter(mq, input)));

            return new ClrAsyncEnumerableFilter(cluster, traitSet, input, condition);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="condition"></param>
        public ClrAsyncEnumerableFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) :
            base(cluster, traitSet, input, condition)
        {

        }

        /// <inheritdoc />
        public override Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition)
        {
            return new ClrAsyncEnumerableFilter(getCluster(), traitSet, input, condition);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            var collation = required.getCollation();
            if (collation == null || collation == RelCollations.EMPTY)
                return null!;

            var traits = getTraitSet().replace(collation);

            return org.apache.calcite.util.Pair.of(traits, com.google.common.collect.ImmutableList.of(traits));
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            var collation = childTraits.getCollation();
            if (collation == null || collation == RelCollations.EMPTY)
                return null!;

            var traits = getTraitSet().replace(collation);

            return org.apache.calcite.util.Pair.of(traits, com.google.common.collect.ImmutableList.of(traits));
        }

        /// <inheritdoc />
        /// <remarks>
        /// A calc is always better, exactly as for <c>EnumerableFilter</c>. See
        /// <see cref="ClrAsyncEnumerableProject.Implement"/>.
        /// </remarks>
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            throw new java.lang.UnsupportedOperationException();
        }

    }

}
