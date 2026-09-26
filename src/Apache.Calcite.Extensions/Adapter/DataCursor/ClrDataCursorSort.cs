using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="Sort"/> in the <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    public class ClrDataCursorSort : Sort, ClrDataCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorSort"/>.
        /// </summary>
        /// <param name="child"></param>
        /// <param name="collation"></param>
        /// <param name="offset"></param>
        /// <param name="fetch"></param>
        /// <returns></returns>
        public static ClrDataCursorSort Create(RelNode child, RelCollation collation, RexNode? offset, RexNode? fetch)
        {
            var cluster = child.getCluster();
            var traitSet = cluster.traitSetOf(ClrDataCursorConvention.Instance).replace(collation);

            return new ClrDataCursorSort(cluster, traitSet, child, collation, offset, fetch);
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
        public ClrDataCursorSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode? offset, RexNode? fetch) :
            base(cluster, traitSet, input, collation, offset, fetch)
        {
            if (offset != null || fetch != null)
                throw new java.lang.IllegalArgumentException("offset and fetch must be null");
        }

        /// <inheritdoc />
        public override Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch)
        {
            return new ClrDataCursorSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrDataCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(collation.getFieldCollations());

            var sourceType = inputPhysType.RowType;

            var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));

            var keyType = keySelector.ReturnType;

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrDataCursorBuiltInMethod.OrderBy.MakeGenericMethod(sourceType, keyType),
                    result.Expression,
                    keySelector,
                    comparator));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrDataCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), result.Format);

            var inputPhysType = result.PhysType;
            var (keySelector, collationComparator) = inputPhysType.GenerateCollationKey(collation.getFieldCollations());

            var sourceType = inputPhysType.RowType;

            var comparator = collationComparator ?? Expression.Constant(null, typeof(java.util.Comparator));

            var keyType = keySelector.ReturnType;

            return implementor.ResultAsync(physType,
                ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.OrderByAsync.MakeGenericMethod(sourceType, keyType),
                    result.Expression,
                    keySelector,
                    comparator));
        }

    }

}
