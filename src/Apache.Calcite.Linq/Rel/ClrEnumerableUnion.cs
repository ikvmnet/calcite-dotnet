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
    /// Implementation of <see cref="Union"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableUnion : Union, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrEnumerableUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrEnumerableUnion(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public virtual ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? unionExp = null;
            ClrEnumerableResult? last = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrEnumerableRel)getInputs().get(i), pref);
                last = result;

                if (unionExp == null)
                {
                    unionExp = result.Expression;
                    continue;
                }

                var rowType = TypeResolver.Resolve(result.PhysType.getJavaRowType());

                unionExp = all
                    ? Expression.Call(null, ClrBuiltInMethod.Concat.MakeGenericMethod(rowType), unionExp, result.Expression)
                    : Expression.Call(null, ClrBuiltInMethod.Union.MakeGenericMethod(rowType), unionExp, result.Expression, ClrPhysTypes.Comparer(implementor, result.PhysType));
            }

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, unionExp ?? throw new java.lang.IllegalStateException("unionExp"));
        }

    }

}
