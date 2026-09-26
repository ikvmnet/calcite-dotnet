using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Minus"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// linq4j's <c>except</c> drains its first source and then acquires its second, so the second input is
    /// deferred within the body's own kind, through <see cref="ClrCursorRelImplementor.Opener"/> or
    /// <see cref="ClrCursorRelImplementor.OpenerAsync"/>, as <see cref="ClrCursorUnion"/> defers
    /// the second input of a <c>UNION</c>.
    /// </remarks>
    public class ClrCursorMinus : Minus, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrCursorMinus(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrCursorMinus(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? minusExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrCursorRel)getInputs().get(i), pref);

                if (minusExp == null)
                {
                    minusExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                minusExp = Expression.Call(null,
                    ClrCursorBuiltInMethod.Except.MakeGenericMethod(rowType),
                    minusExp,
                    implementor.Opener(result),
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, minusExp ?? throw new java.lang.IllegalStateException("minusExp"));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? minusExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrCursorRel)getInputs().get(i), pref);

                if (minusExp == null)
                {
                    minusExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                minusExp = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.ExceptAsync.MakeGenericMethod(rowType),
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
