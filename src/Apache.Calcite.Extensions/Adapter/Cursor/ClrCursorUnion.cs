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
    /// Mirrors <c>EnumerableUnion</c>, folding the inputs pairwise from left to right.
    ///
    /// <para><c>UNION ALL</c> uses linq4j's <c>concat</c>, which acquires each source in turn inside an
    /// advance of either kind. Each implementation therefore visits every input through both hierarchies and
    /// passes a synchronous and an awaiting opener for each operand, building the synchronous and awaiting
    /// folds side by side.</para>
    ///
    /// <para><c>UNION</c> uses linq4j's <c>union</c>, which drains its first source and then acquires the
    /// next within its own open, so each later input is passed as an opener of the implementation's own
    /// kind.</para>
    /// </remarks>
    public class ClrCursorUnion : Union, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="inputs">The inputs, a list of <see cref="org.apache.calcite.rel.RelNode"/>.</param>
        /// <param name="all">Whether duplicates are kept (<c>UNION ALL</c>).</param>
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

            // the other kind's fold, needed by concat, which acquires sources inside advances of either kind
            Expression? unionExpAsync = null;

            // locals holding the openers of the fold so far; see Bind
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

            // the other kind's fold, needed by concat, which acquires sources inside advances of either kind
            Expression? unionExpSync = null;

            // locals holding the openers of the fold so far; see Bind
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
        /// Assigns the synchronous and awaiting openers of the fold so far to new locals, and returns them.
        /// </summary>
        /// <remarks>
        /// Each step of a <c>UNION ALL</c> fold refers to the previous step's fold twice, once per kind, so
        /// inlining it would double the expression with every input. Binding each step to locals keeps the
        /// tree linear in the number of inputs, as <c>EnumerableUnion</c> binds each child to a variable.
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
        /// Wraps the result in a block declaring and assigning the fold's locals, if there are any.
        /// </summary>
        static Expression Block(List<ParameterExpression> locals, List<Expression> body, Expression result)
        {
            return locals.Count == 0 ? result : Expression.Block(result.Type, locals, [.. body, result]);
        }

    }

}
