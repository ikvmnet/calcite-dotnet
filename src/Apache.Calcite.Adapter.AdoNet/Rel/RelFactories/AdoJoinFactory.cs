using System;

using java.lang;
using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="JoinFactory"/> that creates an <see cref="AdoJoin"/> in its left input's convention.
    /// </summary>
    public class AdoJoinFactory : JoinFactory
    {

        /// <inheritdoc />
        public RelNode createJoin(RelNode left, RelNode right, List hints, RexNode condition, Set variablesSet, JoinRelType joinType, bool semiJoinDone)
        {
            ArgumentNullException.ThrowIfNull(left);
            ArgumentNullException.ThrowIfNull(left.getConvention());

            try
            {
                return new AdoJoin(left.getCluster(), left.getCluster().traitSetOf(left.getConvention()), hints, left, right, condition, variablesSet, joinType);
            }
            catch (InvalidRelException e)
            {
                throw new AssertionError(e);
            }
        }

    }

}
