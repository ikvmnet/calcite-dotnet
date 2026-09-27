using System.Collections.Generic;
using System.Linq.Expressions;

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
    /// handed up. The fold is Calcite's — a pairwise <c>concat</c>, left to right — with each step's pair of
    /// opens bound to locals, as Calcite binds each child, so that a step names the fold before it once.
    /// <c>UNION</c> is linq4j's <c>union</c>, which drains its first source and then acquires its
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
        public virtual ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            Expression? unionExp = null;

            // the other hierarchy's fold, kept in step for a concat: a source it acquires inside an advance
            // has to be openable by the advance of either kind
            Expression? unionExpAsync = null;

            // the fold so far, bound once per step as Calcite binds each child
            var locals = new List<ParameterExpression>();
            var body = new List<Expression>();

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
                    var (open, openAsync) = Bind(implementor, locals, body, i,
                        new ClrCursorResult(unionExp, result.PhysType, result.Format),
                        new ClrCursorAsyncResult(unionExpAsync!, result.PhysType, result.Format));

                    Expression[] openers =
                    [
                        open,
                        openAsync,
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

            return implementor.Result(physType, Block(locals, body, unionExp ?? throw new java.lang.IllegalStateException("unionExp")));
        }

        /// <inheritdoc />
        public virtual ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            Expression? unionExp = null;

            // the other hierarchy's fold, kept in step for a concat: a source it acquires inside an advance
            // has to be openable by the advance of either kind
            Expression? unionExpSync = null;

            // the fold so far, bound once per step as Calcite binds each child
            var locals = new List<ParameterExpression>();
            var body = new List<Expression>();

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
                    var (open, openAsync) = Bind(implementor, locals, body, i,
                        new ClrCursorResult(unionExpSync!, result.PhysType, result.Format),
                        new ClrCursorAsyncResult(unionExp, result.PhysType, result.Format));

                    Expression[] openers =
                    [
                        open,
                        openAsync,
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

            return implementor.ResultAsync(physType, Block(locals, body, unionExp ?? throw new java.lang.IllegalStateException("unionExp")));
        }

        /// <summary>
        /// Binds both opens of the fold so far to locals, and returns the locals.
        /// </summary>
        /// <remarks>
        /// Calcite's <c>EnumerableUnion</c> appends each child to its block as <c>child{i}</c> and folds
        /// <c>concat</c> over those names, so every step names the fold once. A step here needs the fold
        /// twice — as the open of each kind, since the advance that reaches it may be either — and each open
        /// holds the other hierarchy's fold as well, so inlined the expression doubles with every input: a
        /// twenty-way <c>UNION ALL</c>, which an <c>IN</c> list of twenty dynamic parameters becomes, compiled
        /// about a million copies of its first input. Bound, each step names the previous pair once.
        /// </remarks>
        static (ParameterExpression Open, ParameterExpression OpenAsync) Bind(ClrCursorRelImplementor implementor, List<ParameterExpression> locals, List<Expression> body, int i, ClrCursorResult fold, ClrCursorAsyncResult foldAsync)
        {
            var opener = implementor.Opener(fold);
            var openerAsync = implementor.OpenerAsync(foldAsync);

            var open = Expression.Variable(opener.Type, "union" + i);
            var openAsync = Expression.Variable(openerAsync.Type, "unionAsync" + i);

            locals.Add(open);
            locals.Add(openAsync);
            body.Add(Expression.Assign(open, opener));
            body.Add(Expression.Assign(openAsync, openerAsync));

            return (open, openAsync);
        }

        /// <summary>
        /// Wraps the result in the block that binds the fold's locals, where there are any.
        /// </summary>
        static Expression Block(List<ParameterExpression> locals, List<Expression> body, Expression result)
        {
            return locals.Count == 0 ? result : Expression.Block(result.Type, locals, [.. body, result]);
        }

    }

}
