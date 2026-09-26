using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Union"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The node that shows the shape a deferred source takes in this convention. <c>UNION ALL</c> is
    /// linq4j's <c>concat</c>, which acquires each source at its turn inside <c>moveNext</c>, so the inputs
    /// are handed to the operator as opens — both opens of each, because the advance that reaches a source
    /// may be either — through <see cref="ClrCursorRelImplementor.Opener"/> and
    /// <see cref="ClrCursorRelImplementor.OpenerAsync"/>. So each body visits every input through both
    /// hierarchies and folds both chains in step, and what differs between the bodies is which chain is
    /// handed up. <c>UNION</c> is linq4j's <c>union</c>, which drains its first source and then acquires its
    /// second inside the open, so the second is deferred within the body's own kind and only that opener is
    /// needed.
    /// </remarks>
    public class ClrCursorUnion : Union, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public ClrCursorUnion(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />
        public override SetOp copy(RelTraitSet traitSet, java.util.List inputs, bool all)
        {
            return new ClrCursorUnion(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public virtual ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? unionExp = null;

            // the other hierarchy's fold, kept in step for a concat: a source it acquires inside an advance
            // has to be openable by the advance of either kind
            Expression? unionExpAsync = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChild(this, i, (ClrCursorRel)getInputs().get(i), pref);
                var resultAsync = all ? implementor.VisitChildAsync(this, i, (ClrCursorRel)getInputs().get(i), pref) : null;

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
                        implementor.Opener(new ClrCursorResult(unionExp, result.PhysType, result.Format)),
                        implementor.OpenerAsync(new ClrCursorAsyncResult(unionExpAsync!, result.PhysType, result.Format)),
                        implementor.Opener(result),
                        implementor.OpenerAsync(resultAsync!),
                    ];

                    unionExp = Expression.Call(null, ClrCursorBuiltInMethod.Concat.MakeGenericMethod(rowType), openers);
                    unionExpAsync = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.ConcatAsync.MakeGenericMethod(rowType), openers);
                }
                else
                {
                    unionExp = Expression.Call(null, ClrCursorBuiltInMethod.Union.MakeGenericMethod(rowType), unionExp, implementor.Opener(result), result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)));
                }
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.Result(physType, unionExp ?? throw new java.lang.IllegalStateException("unionExp"));
        }

        /// <inheritdoc />
        public virtual ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            Expression? unionExp = null;

            // the other hierarchy's fold, kept in step for a concat: a source it acquires inside an advance
            // has to be openable by the advance of either kind
            Expression? unionExpSync = null;

            for (int i = 0; i < getInputs().size(); i++)
            {
                var result = implementor.VisitChildAsync(this, i, (ClrCursorRel)getInputs().get(i), pref);
                var resultSync = all ? implementor.VisitChild(this, i, (ClrCursorRel)getInputs().get(i), pref) : null;

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
                        implementor.Opener(new ClrCursorResult(unionExpSync!, result.PhysType, result.Format)),
                        implementor.OpenerAsync(new ClrCursorAsyncResult(unionExp, result.PhysType, result.Format)),
                        implementor.Opener(resultSync!),
                        implementor.OpenerAsync(result),
                    ];

                    unionExp = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.ConcatAsync.MakeGenericMethod(rowType), openers);
                    unionExpSync = Expression.Call(null, ClrCursorBuiltInMethod.Concat.MakeGenericMethod(rowType), openers);
                }
                else
                {
                    unionExp = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.UnionAsync.MakeGenericMethod(rowType), unionExp, implementor.OpenerAsync(result), result.PhysType.Comparer() ?? Expression.Constant(null, typeof(org.apache.calcite.linq4j.function.EqualityComparer)));
                }
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.CUSTOM));

            return implementor.ResultAsync(physType, unionExp ?? throw new java.lang.IllegalStateException("unionExp"));
        }

    }

}
