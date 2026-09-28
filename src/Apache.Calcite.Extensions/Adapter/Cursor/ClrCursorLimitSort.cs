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
    /// Implementation of <see cref="Sort"/> carrying a limit, in the <see cref="ClrCursorConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableLimitSort</c>. Where a sort followed by a limit orders every row, this keeps only
    /// the first offset plus fetch rows while it reads its input.
    ///
    /// <para>The input is passed as an opener, because linq4j's bounded <c>orderBy</c> tests the fetch before
    /// it acquires its source and, for a fetch of zero, never acquires it.</para>
    /// </remarks>
    public class ClrCursorLimitSort : Sort, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorLimitSort"/>.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="collation">The ordering.</param>
        /// <param name="offset">The number of rows to skip, or null.</param>
        /// <param name="fetch">The maximum number of rows to return, or null.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorLimitSort Create(RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance).replace(collation);

            return new ClrCursorLimitSort(cluster, traitSet, input, collation, offset, fetch);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/> and the collation.</param>
        /// <param name="input">The input.</param>
        /// <param name="collation">The ordering.</param>
        /// <param name="offset">The number of rows to skip, or null.</param>
        /// <param name="fetch">The maximum number of rows to return, or null.</param>
        public ClrCursorLimitSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {

        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
        {
            return new ClrCursorLimitSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
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
            var roundingPolicy = ClrCursorLimit.RoundingPolicy(implementor);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.OrderByWithFetchAndOffset.MakeGenericMethod(sourceType, keySelector.ReturnType),
                    implementor.Opener(result),
                    keySelector,
                    comparator,
                    offset == null ? Expression.Constant(java.math.BigDecimal.ZERO) : ClrCursorLimit.Count(implementor, offset, "OFFSET", roundingPolicy),
                    fetch == null ? Expression.Constant(java.math.BigDecimal.valueOf(int.MaxValue)) : ClrCursorLimit.Count(implementor, fetch, "FETCH", roundingPolicy)));
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
            var roundingPolicy = ClrCursorLimit.RoundingPolicy(implementor);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.OrderByWithFetchAndOffsetAsync.MakeGenericMethod(sourceType, keySelector.ReturnType),
                    implementor.OpenerAsync(result),
                    keySelector,
                    comparator,
                    offset == null ? Expression.Constant(java.math.BigDecimal.ZERO) : ClrCursorLimit.Count(implementor, offset, "OFFSET", roundingPolicy),
                    fetch == null ? Expression.Constant(java.math.BigDecimal.valueOf(int.MaxValue)) : ClrCursorLimit.Count(implementor, fetch, "FETCH", roundingPolicy)));
        }

    }

}
