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
    /// Implementation of <see cref="Intersect"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    public class ClrEnumerableIntersect : Intersect, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrEnumerableIntersect(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrEnumerableIntersect(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? intersectExp = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrEnumerableRel)getInputs().get(i), pref);

                if (intersectExp == null)
                {
                    intersectExp = result.Expression;
                    continue;
                }

                var rowType = ClrTypes.Resolve(result.PhysType.getJavaRowType());

                intersectExp = Expression.Call(null,
                    ClrBuiltInMethod.Intersect.MakeGenericMethod(rowType),
                    intersectExp,
                    result.Expression,
                    ClrPhysTypes.Comparer(implementor, result.PhysType),
                    Expression.Constant(all));
            }

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, intersectExp ?? throw new java.lang.IllegalStateException("intersectExp"));
        }

    }

}
