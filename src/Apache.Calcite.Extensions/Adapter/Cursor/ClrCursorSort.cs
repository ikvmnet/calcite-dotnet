using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Sort"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableSort</c>. The node sorts only; an offset or fetch is implemented by
    /// <see cref="ClrCursorLimit"/> or <see cref="ClrCursorLimitSort"/>. The input is read in full when the
    /// cursor is opened.
    /// </remarks>
    public class ClrCursorSort : Sort, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorSort"/>.
        /// </summary>
        /// <param name="child">The input.</param>
        /// <param name="collation">The sort order.</param>
        /// <param name="offset">Must be <see langword="null"/>.</param>
        /// <param name="fetch">Must be <see langword="null"/>.</param>
        /// <returns>The new sort.</returns>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="offset"/> or
        /// <paramref name="fetch"/> is not <see langword="null"/>.</exception>
        public static ClrCursorSort Create(RelNode child, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = child.getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance).replace(collation);

            return new ClrCursorSort(cluster, traitSet, child, collation, offset, fetch);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> builds the trait set from the collation; this
        /// constructor takes it as given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="collation">The sort order.</param>
        /// <param name="offset">Must be <see langword="null"/>.</param>
        /// <param name="fetch">Must be <see langword="null"/>.</param>
        /// <exception cref="java.lang.IllegalArgumentException"><paramref name="offset"/> or
        /// <paramref name="fetch"/> is not <see langword="null"/>.</exception>
        public ClrCursorSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {
            if (offset != null || fetch != null)
                throw new java.lang.IllegalArgumentException("offset and fetch must be null");
        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
        {
            return new ClrCursorSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(collation.getFieldCollations());

            var sourceType = inputPhysType.RowType;

            var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));

            var keyType = keySelector.ReturnType;

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.OrderBy.MakeGenericMethod(sourceType, keyType),
                    result.Expression,
                    keySelector,
                    comparator));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(collation.getFieldCollations());

            var sourceType = inputPhysType.RowType;

            var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));

            var keyType = keySelector.ReturnType;

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.OrderByAsync.MakeGenericMethod(sourceType, keyType),
                    result.Expression,
                    keySelector,
                    comparator));
        }

    }

}
