using System;
using System.Linq.Expressions;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A sub-plan of the cursor convention held as an expression tree and compiled the first time it is run.
    /// </summary>
    /// <typeparam name="TRows">What the plan returns, an <c>IClrCursor</c>.</typeparam>
    /// <remarks>
    /// <c>ClrCursorToEnumerableConverter</c> stashes one in the generated Java code that calls it, since a
    /// sub-plan under a Calcite node cannot be spliced into a CLR tree. Compiling on first use keeps JIT work
    /// out of planning.
    ///
    /// <para>Not synchronized: two threads that run an uncompiled plan at once may both compile it, which
    /// is harmless.</para>
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
        /// <returns>What the plan returns.</returns>
        public TRows Invoke(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return (compiled ??= (Func<DataContext, TRows>)tree.Compile())(root);
        }

    }

}
