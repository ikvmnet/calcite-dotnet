using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
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
    /// linq4j's <c>intersect</c> drains its <em>second</em> source first and acquires its first only
    /// afterwards, so it is the first input — the fold so far — that is deferred, within the body's own
    /// kind, through <see cref="ClrCursorRelImplementor.Opener"/> or
    /// <see cref="ClrCursorRelImplementor.OpenerAsync"/>; the second arrives opened.
    /// </remarks>
    public class ClrCursorIntersect : Intersect, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
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
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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
