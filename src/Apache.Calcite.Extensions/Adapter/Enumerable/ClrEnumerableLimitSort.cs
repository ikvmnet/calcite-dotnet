using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of <see cref="Sort"/> carrying a limit, in the <see cref="ClrEnumerableConvention"/>
    /// calling convention.
    /// </summary>
    /// <remarks>
    /// A sort followed by a limit reads every row and orders all of them. This orders and limits together, so
    /// only as many rows as are wanted need be kept.
    /// </remarks>
    public class ClrEnumerableLimitSort : Sort, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableLimitSort"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrEnumerableLimitSort Create(RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance).replace(collation);

            return new ClrEnumerableLimitSort(cluster, traitSet, input, collation, offset, fetch);
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
        public ClrEnumerableLimitSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {

        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
        {
            return new ClrEnumerableLimitSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(collation.getFieldCollations());
            var sourceType = inputPhysType.RowType;

            var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));
            var roundingPolicy = ClrEnumerableLimit.RoundingPolicy(implementor);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.OrderByWithFetchAndOffset.MakeGenericMethod(sourceType, keySelector.ReturnType),
                    result.Expression,
                    keySelector,
                    comparator,
                    offset == null ? Expression.Constant(java.math.BigDecimal.ZERO) : ClrEnumerableLimit.Count(implementor, offset, "OFFSET", roundingPolicy),
                    fetch == null ? Expression.Constant(java.math.BigDecimal.valueOf(int.MaxValue)) : ClrEnumerableLimit.Count(implementor, fetch, "FETCH", roundingPolicy)));
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult ImplementAsync(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(collation.getFieldCollations());
            var sourceType = inputPhysType.RowType;

            var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));
            var roundingPolicy = ClrEnumerableLimit.RoundingPolicy(implementor);

            return implementor.ResultAsync(physType,
                ClrBuiltInMethod.CallAsync(ClrBuiltInMethod.OrderByWithFetchAndOffsetAsync.MakeGenericMethod(sourceType, keySelector.ReturnType),
                    result.Expression,
                    keySelector,
                    comparator,
                    offset == null ? Expression.Constant(java.math.BigDecimal.ZERO) : ClrEnumerableLimit.Count(implementor, offset, "OFFSET", roundingPolicy),
                    fetch == null ? Expression.Constant(java.math.BigDecimal.valueOf(int.MaxValue)) : ClrEnumerableLimit.Count(implementor, fetch, "FETCH", roundingPolicy)));
        }

    }

}
