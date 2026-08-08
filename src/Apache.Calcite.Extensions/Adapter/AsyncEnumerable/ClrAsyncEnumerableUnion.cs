using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Union"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrAsyncEnumerableUnion : Union, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrAsyncEnumerableUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrAsyncEnumerableUnion(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public virtual ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? unionExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrAsyncEnumerableRel)getInputs().get(i), pref);

                if (unionExp == null)
                {
                    unionExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                unionExp = all
                    ? ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Concat.MakeGenericMethod(rowType), unionExp, result.Expression)
                    : ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Union.MakeGenericMethod(rowType), unionExp, result.Expression, result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, unionExp ?? throw new java.lang.IllegalStateException("unionExp"));
        }

    }

}
