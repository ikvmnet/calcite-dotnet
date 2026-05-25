using System;

using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="FilterFactory"/> implementation that creates <see cref="AdoFilter"/> nodes
    /// during relational-algebra construction in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoFilterFactory : FilterFactory
    {

        /// <inheritdoc />
        public RelNode createFilter(RelNode input, RexNode condition, Set variablesSet)
        {
            if (variablesSet.isEmpty())
                throw new ArgumentException("AdoFilter does not allow variables");

            return new AdoFilter(input.getCluster(), input.getTraitSet(), input, condition);
        }

        /// <inheritdoc />
        public RelNode createFilter(RelNode input, RexNode condition)
        {
            throw new NotImplementedException();
        }

    }

}
