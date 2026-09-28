using System.Linq.Expressions;

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
    /// Mirrors <c>EnumerableMinus</c>. linq4j's <c>except</c> drains its first source before acquiring the
    /// next, so every input after the first is passed as an opener of the body's own kind, as in
    /// <see cref="ClrCursorUnion"/>.
    /// </remarks>
    public class ClrCursorMinus : Minus, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="inputs">The inputs, a list of <see cref="org.apache.calcite.rel.RelNode"/>; rows of the
        /// first that appear in any later one are removed.</param>
        /// <param name="all">Whether duplicates are kept (<c>EXCEPT ALL</c>).</param>
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
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
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
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
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
