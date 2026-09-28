using System;

using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="FilterFactory"/> that creates an <see cref="AdoFilter"/> with its input's traits.
    /// </summary>
    public class AdoFilterFactory : FilterFactory
    {

        /// <inheritdoc />
        /// <remarks>
        /// TODO: this throws when <paramref name="variablesSet"/> is empty, where <c>JdbcFilterFactory</c> throws when it
        /// is not.
        /// </remarks>
        public RelNode createFilter(RelNode input, RexNode condition, Set variablesSet)
        {
            if (variablesSet.isEmpty())
                throw new ArgumentException("AdoFilter does not allow variables");

            return new AdoFilter(input.getCluster(), input.getTraitSet(), input, condition);
        }

        /// <summary>
        /// Not implemented.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="condition">The condition.</param>
        /// <returns>Does not return.</returns>
        /// <exception cref="NotImplementedException">Always.</exception>
        public RelNode createFilter(RelNode input, RexNode condition)
        {
            throw new NotImplementedException();
        }

    }

}
