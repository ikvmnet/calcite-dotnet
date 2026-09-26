using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
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
    /// A sort followed by a limit reads every row and orders all of them. This orders and limits together, so
    /// only as many rows as are wanted need be kept.
    ///
    /// <para>The input is handed to the operator as an open of the body's own kind rather than as a cursor,
    /// because linq4j's bounded <c>orderBy</c> tests the fetch inside <c>enumerator()</c> before it acquires
    /// its source, and for a fetch of no rows never acquires it at all.</para>
    /// </remarks>
    public class ClrCursorLimitSort : Sort, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorLimitSort"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrCursorLimitSort Create(RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance).replace(collation);

            return new ClrCursorLimitSort(cluster, traitSet, input, collation, offset, fetch);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
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
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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
