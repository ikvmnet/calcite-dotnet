using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="Minus"/> in the <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// linq4j's <c>except</c> drains its first source and then acquires its second, so the second input is
    /// deferred within the body's own kind, through <see cref="ClrDataCursorRelImplementor.Opener"/> or
    /// <see cref="ClrDataCursorRelImplementor.OpenerAsync"/>, as <see cref="ClrDataCursorUnion"/> defers
    /// the second input of a <c>UNION</c>.
    /// </remarks>
    public class ClrDataCursorMinus : Minus, ClrDataCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrDataCursorMinus(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrDataCursorMinus(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? minusExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrDataCursorRel)getInputs().get(i), pref);

                if (minusExp == null)
                {
                    minusExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                minusExp = Expression.Call(null,
                    ClrDataCursorBuiltInMethod.Except.MakeGenericMethod(rowType),
                    minusExp,
                    implementor.Opener(result),
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, minusExp ?? throw new java.lang.IllegalStateException("minusExp"));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? minusExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrDataCursorRel)getInputs().get(i), pref);

                if (minusExp == null)
                {
                    minusExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                minusExp = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.ExceptAsync.MakeGenericMethod(rowType),
                    minusExp,
                    implementor.OpenerAsync(result),
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.ResultAsync(physType, minusExp ?? throw new java.lang.IllegalStateException("minusExp"));
        }

    }

}
