using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Intersect"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    public class ClrAsyncEnumerableIntersect : Intersect, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrAsyncEnumerableIntersect(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrAsyncEnumerableIntersect(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? intersectExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrAsyncEnumerableRel)getInputs().get(i), pref);

                if (intersectExp == null)
                {
                    intersectExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                intersectExp = ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Intersect.MakeGenericMethod(rowType),
                    intersectExp,
                    result.Expression,
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, intersectExp ?? throw new java.lang.IllegalStateException("intersectExp"));
        }

    }

}
