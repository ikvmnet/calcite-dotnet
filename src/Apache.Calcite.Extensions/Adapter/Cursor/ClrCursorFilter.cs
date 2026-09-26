using Apache.Calcite.Extensions.Adapter.Enumerable;

using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Filter"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    public class ClrCursorFilter : Filter, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorFilter"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static ClrCursorFilter Create(RelNode input, RexNode condition)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.filter(mq, input)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.filter(mq, input)));

            return new ClrCursorFilter(cluster, traitSet, input, condition);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="condition"></param>
        public ClrCursorFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) :
            base(cluster, traitSet, input, condition)
        {

        }

        /// <inheritdoc />
        public override Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition)
        {
            return new ClrCursorFilter(getCluster(), traitSet, input, condition);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            var collation = required.getCollation();
            if (collation == null || collation == RelCollations.EMPTY)
                return null;

            var traits = getTraitSet().replace(collation);

            return org.apache.calcite.util.Pair.of(traits, com.google.common.collect.ImmutableList.of(traits));
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            var collation = childTraits.getCollation();
            if (collation == null || collation == RelCollations.EMPTY)
                return null;

            var traits = getTraitSet().replace(collation);

            return org.apache.calcite.util.Pair.of(traits, com.google.common.collect.ImmutableList.of(traits));
        }

        /// <inheritdoc />
        /// <remarks>
        /// A calc is always better, exactly as for <c>EnumerableFilter</c>. See
        /// <see cref="ClrCursorProject.Implement"/>.
        /// </remarks>
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            throw new java.lang.UnsupportedOperationException(
                "ClrCursorFilter cannot implement itself, exactly as EnumerableFilter cannot: a calc " +
                "carries the filter and the projection in one pass and is always better. Reaching here means " +
                "ClrCursorRules.CalcRules() was not run as a hep pass after the planner. It cannot be run " +
                "on the planner instead: VolcanoPlanner.addRule does not register a TransformationRule's " +
                "operand against a PhysicalNode, and every node of this convention is one.");
        }

    }

}
