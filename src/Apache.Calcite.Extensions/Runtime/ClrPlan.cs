using System;
using System.Linq.Expressions;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A plan of one of the CLR conventions, compiled the first time it is run.
    /// </summary>
    /// <typeparam name="TRows">What the plan yields — an <c>IEnumerable</c> or an
    /// <c>IAsyncEnumerable&lt;object&gt;</c>.</typeparam>
    /// <remarks>
    /// What a converter <em>out of</em> one of these conventions stashes. It holds the expression tree and
    /// nothing else until something asks it to run.
    ///
    /// <para><b>Compiling is not planning.</b> A node's <c>Implement</c> builds an expression and returns
    /// it; the compiler is <c>ClrEnumerableInterpretable.ToBindable</c>, called once, at the end. A
    /// converter that called <see cref="LambdaExpression.Compile()"/> inside its own <c>Implement</c> did
    /// JIT work while the plan was still being assembled, and did it once per converter — so a plan holding
    /// three of them compiled four times, three of those during planning. That is what this exists to
    /// stop.</para>
    ///
    /// <para>The sub-plan cannot simply join the root's tree instead, which would be the real fix: at this
    /// boundary the parent is Calcite's generated Java, so there is no CLR tree above to splice into and the
    /// sequence has to be produced by calling out. Deferring the compile is what is available.</para>
    ///
    /// <para>It compiles at most once per prepared statement, because the converter stashes it in the
    /// internal parameters and those belong to the <c>IClrPrepare.Signature</c>. Two threads reaching an uncompiled
    /// one at the same moment may both compile it; that is idempotent and cheaper than locking a path taken
    /// once.</para>
    /// </remarks>
    sealed class ClrPlan<TRows>
    {

        readonly LambdaExpression tree;

        Func<DataContext, TRows>? compiled;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="tree">The plan, as a lambda of one <see cref="DataContext"/>.</param>
        public ClrPlan(LambdaExpression tree)
        {
            this.tree = tree ?? throw new ArgumentNullException(nameof(tree));
        }

        /// <summary>
        /// Runs the plan, compiling it if this is the first time.
        /// </summary>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns>The rows.</returns>
        public TRows Invoke(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return (compiled ??= (Func<DataContext, TRows>)tree.Compile())(root);
        }

    }

}
