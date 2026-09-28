using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A sort, offset or fetch pushed down to the source as <c>ORDER BY</c> and the dialect's form of
    /// <c>OFFSET</c>/<c>FETCH</c>. Mirrors <c>JdbcRules.JdbcSort</c>.
    /// </summary>
    public class AdoSort : Sort, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention is an <see cref="AdoConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="collation">The sort order.</param>
        /// <param name="offset">The number of rows to skip, or <see langword="null"/>.</param>
        /// <param name="fetch">The number of rows to return, or <see langword="null"/>.</param>
        public AdoSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {

        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode? offset, RexNode? fetch)
        {
            return new AdoSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        /// <summary>
        /// Returns the base cost multiplied by 0.9, as <c>JdbcSort</c> does.
        /// </summary>
        /// <param name="planner">The planner.</param>
        /// <param name="mq">The metadata query.</param>
        /// <returns>The cost, or <see langword="null"/>.</returns>
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return base.computeSelfCost(planner, mq)?.multiplyBy(.9);
        }

    }

}
