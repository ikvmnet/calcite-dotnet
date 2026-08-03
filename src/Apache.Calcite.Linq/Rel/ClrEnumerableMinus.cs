using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Linq.Rel
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
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
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

                var rowType = TypeResolver.Resolve(result.PhysType.getJavaRowType());

                minusExp = Expression.Call(null,
                    ClrBuiltInMethod.Except.MakeGenericMethod(rowType),
                    minusExp,
                    result.Expression,
                    ClrPhysTypes.Comparer(implementor, result.PhysType),
                    Expression.Constant(all));
            }

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, minusExp ?? throw new java.lang.IllegalStateException("minusExp"));
        }

    }

}
