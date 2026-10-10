using System;

using org.apache.calcite.rex;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A <see cref="RexExecutor"/> that remembers what another executor reduced each list of constant
    /// expressions to, and answers a list it has seen before without calling it again.
    /// </summary>
    /// <remarks>
    /// <c>RexExecutorImpl</c> compiles a Janino class for every list it is given, and <c>ReduceExpressionsRule</c>
    /// hands it the same list each time it meets those constants on another node. One instance belongs to one
    /// planner, so nothing it remembers outlives the statement or reaches another data context.
    ///
    /// <para>A list is identified by each expression's digest together with its full type string, because a
    /// call's digest does not include the type of the call itself. A reduction the inner executor gave up on
    /// is remembered too, and a later hit answers it with the caller's own expressions, as
    /// <c>RexExecutorImpl</c> returns its input. An exception from the inner executor is not remembered.</para>
    /// </remarks>
    internal sealed class CachingRexExecutor : RexExecutor
    {

        /// <summary>
        /// What one list of constant expressions was reduced to.
        /// </summary>
        /// <param name="ConstExps">The expressions the inner executor was given.</param>
        /// <param name="ReducedValues">What it returned, one per expression.</param>
        sealed record Reduction(java.util.List ConstExps, java.util.List ReducedValues);

        readonly RexExecutor _executor;

        // java.util.HashMap because the key is a java.util.List of Pair, whose equality is element-wise
        readonly java.util.HashMap _reductions = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="executor">The executor that performs a reduction not seen before.</param>
        public CachingRexExecutor(RexExecutor executor)
        {
            _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        }

        /// <inheritdoc />
        public void reduce(RexBuilder rexBuilder, java.util.List constExps, java.util.List reducedValues)
        {
            var key = new java.util.ArrayList(constExps.size());
            for (var i = constExps.iterator(); i.hasNext();)
            {
                var node = (RexNode)i.next();
                key.add(Pair.of(node.ToString(), node.getType().getFullTypeString()));
            }

            if (_reductions.get(key) is not Reduction reduction)
            {
                // a copy of each list, so that a caller mutating its own afterwards does not alter the entry
                var values = new java.util.ArrayList();
                _executor.reduce(rexBuilder, constExps, values);
                reduction = new Reduction(new java.util.ArrayList(constExps), values);
                _reductions.put(key, reduction);
            }

            for (var i = 0; i < reduction.ReducedValues.size(); i++)
            {
                // an expression left unreduced is answered with the caller's own, which is equal to it but
                // may not be the same instance
                var value = reduction.ReducedValues.get(i);
                reducedValues.add(ReferenceEquals(value, reduction.ConstExps.get(i)) ? constExps.get(i) : value);
            }
        }

    }

}
