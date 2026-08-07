using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of <see cref="Minus"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableMinus : Minus, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrEnumerableMinus(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrEnumerableMinus(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? minusExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrEnumerableRel)getInputs().get(i), pref);

                if (minusExp == null)
                {
                    minusExp = result.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                minusExp = Expression.Call(null,
                    ClrBuiltInMethod.Except.MakeGenericMethod(rowType),
                    minusExp,
                    result.Expression,
                    result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)),
                    Expression.Constant(all));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, minusExp ?? throw new java.lang.IllegalStateException("minusExp"));
        }

    }

}
