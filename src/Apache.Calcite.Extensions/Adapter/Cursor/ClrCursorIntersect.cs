using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Intersect"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableIntersect</c>, folding the inputs pairwise. linq4j's <c>intersect</c> drains its
    /// second source before it acquires its first, so at each step the first operand (the intersection so far)
    /// is passed as an opener and the second as an open.
    /// </remarks>
    public class ClrCursorIntersect : Intersect, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="inputs">The inputs.</param>
        /// <param name="all">Whether duplicates are kept (<c>INTERSECT ALL</c>).</param>
        public ClrCursorIntersect(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrCursorIntersect(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            Expression? intersectExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrCursorRel)getInputs().get(i), pref);

                if (intersectExp == null)
                {
                    intersectExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                intersectExp = Expression.Call(null,
                    ClrCursorBuiltInMethod.Intersect.MakeGenericMethod(rowType),
                    implementor.Opener(new ClrCursorResult(intersectExp, result.PhysType, result.Format)),
                    result.Expression,
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, intersectExp ?? throw new java.lang.IllegalStateException("intersectExp"));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            Expression? intersectExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrCursorRel)getInputs().get(i), pref);

                if (intersectExp == null)
                {
                    intersectExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                intersectExp = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.IntersectAsync.MakeGenericMethod(rowType),
                    implementor.OpenerAsync(new ClrCursorAsyncResult(intersectExp, result.PhysType, result.Format)),
                    result.Expression,
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.ResultAsync(physType, intersectExp ?? throw new java.lang.IllegalStateException("intersectExp"));
        }

    }

}
