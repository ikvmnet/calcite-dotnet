using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="Intersect"/> in the <see cref="ClrDataCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// linq4j's <c>intersect</c> drains its <em>second</em> source first and acquires its first only
    /// afterwards, so it is the first input — the fold so far — that is deferred, within the body's own
    /// kind, through <see cref="ClrDataCursorRelImplementor.Opener"/> or
    /// <see cref="ClrDataCursorRelImplementor.OpenerAsync"/>; the second arrives opened.
    /// </remarks>
    public class ClrDataCursorIntersect : Intersect, ClrDataCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrDataCursorIntersect(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrDataCursorIntersect(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? intersectExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrDataCursorRel)getInputs().get(i), pref);

                if (intersectExp == null)
                {
                    intersectExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                intersectExp = Expression.Call(null,
                    ClrDataCursorBuiltInMethod.Intersect.MakeGenericMethod(rowType),
                    implementor.Opener(new ClrDataCursorResult(intersectExp, result.PhysType, result.Format)),
                    result.Expression,
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, intersectExp ?? throw new java.lang.IllegalStateException("intersectExp"));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? intersectExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrDataCursorRel)getInputs().get(i), pref);

                if (intersectExp == null)
                {
                    intersectExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                intersectExp = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.IntersectAsync.MakeGenericMethod(rowType),
                    implementor.OpenerAsync(new ClrDataCursorAsyncResult(intersectExp, result.PhysType, result.Format)),
                    result.Expression,
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.ResultAsync(physType, intersectExp ?? throw new java.lang.IllegalStateException("intersectExp"));
        }

    }

}
