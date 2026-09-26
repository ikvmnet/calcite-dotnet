using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Implementation of <see cref="Union"/> in the <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The node that shows the shape a deferred source takes in this convention. <c>UNION ALL</c> is
    /// linq4j's <c>concat</c>, which acquires each source at its turn inside <c>moveNext</c>, so the inputs
    /// are handed to the operator as opens — both opens of each, because the advance that reaches a source
    /// may be either — through <see cref="ClrDataCursorRelImplementor.Opener"/> and
    /// <see cref="ClrDataCursorRelImplementor.OpenerAsync"/>. So each body visits every input through both
    /// hierarchies and folds both chains in step, and what differs between the bodies is which chain is
    /// handed up. <c>UNION</c> is linq4j's <c>union</c>, which drains its first source and then acquires its
    /// second inside the open, so the second is deferred within the body's own kind and only that opener is
    /// needed.
    /// </remarks>
    public class ClrDataCursorUnion : Union, ClrDataCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrDataCursorUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrDataCursorUnion(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public virtual ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? unionExp = null;

            // the other hierarchy's fold, kept in step for a concat: a source it acquires inside an advance
            // has to be openable by the advance of either kind
            Expression? unionExpAsync = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrDataCursorRel)getInputs().get(i), pref);
                var resultAsync = all ? implementor.VisitChildAsync(this, i, (ClrDataCursorRel)getInputs().get(i), pref) : null;

                if (unionExp == null)
                {
                    unionExp = result.Expression;
                    unionExpAsync = resultAsync?.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                if (all)
                {
                    Expression[] openers =
                    [
                        implementor.Opener(new ClrDataCursorResult(unionExp, result.PhysType, result.Format)),
                        implementor.OpenerAsync(new ClrDataCursorAsyncResult(unionExpAsync!, result.PhysType, result.Format)),
                        implementor.Opener(result),
                        implementor.OpenerAsync(resultAsync!),
                    ];

                    unionExp = Expression.Call(null, ClrDataCursorBuiltInMethod.Concat.MakeGenericMethod(rowType), openers);
                    unionExpAsync = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.ConcatAsync.MakeGenericMethod(rowType), openers);
                }
                else
                {
                    unionExp = Expression.Call(null, ClrDataCursorBuiltInMethod.Union.MakeGenericMethod(rowType), unionExp, implementor.Opener(result), result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)));
                }
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, unionExp ?? throw new java.lang.IllegalStateException("unionExp"));
        }

        /// <inheritdoc />
        public virtual ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? unionExp = null;

            // the other hierarchy's fold, kept in step for a concat: a source it acquires inside an advance
            // has to be openable by the advance of either kind
            Expression? unionExpSync = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrDataCursorRel)getInputs().get(i), pref);
                var resultSync = all ? implementor.VisitChild(this, i, (ClrDataCursorRel)getInputs().get(i), pref) : null;

                if (unionExp == null)
                {
                    unionExp = result.Expression;
                    unionExpSync = resultSync?.Expression;
                    continue;
                }

                var rowType = result.PhysType.RowType;

                if (all)
                {
                    Expression[] openers =
                    [
                        implementor.Opener(new ClrDataCursorResult(unionExpSync!, result.PhysType, result.Format)),
                        implementor.OpenerAsync(new ClrDataCursorAsyncResult(unionExp, result.PhysType, result.Format)),
                        implementor.Opener(resultSync!),
                        implementor.OpenerAsync(result),
                    ];

                    unionExp = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.ConcatAsync.MakeGenericMethod(rowType), openers);
                    unionExpSync = Expression.Call(null, ClrDataCursorBuiltInMethod.Concat.MakeGenericMethod(rowType), openers);
                }
                else
                {
                    unionExp = ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.UnionAsync.MakeGenericMethod(rowType), unionExp, implementor.OpenerAsync(result), result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)));
                }
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.ResultAsync(physType, unionExp ?? throw new java.lang.IllegalStateException("unionExp"));
        }

    }

}
