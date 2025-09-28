using System;

using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    class AdoFilterFactory : FilterFactory
    {

        public RelNode createFilter(RelNode input, RexNode condition, Set variablesSet)
        {
            if (variablesSet.isEmpty())
                throw new ArgumentException("AdoFilter does not allow variables");

            return new AdoFilter(input.getCluster(), input.getTraitSet(), input, condition);
        }

        public RelNode createFilter(RelNode input, RexNode condition)
        {
            throw new NotImplementedException();
        }

    }

}
