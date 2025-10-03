using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.util;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.Ado.Rel.RelFactories
{

    public class AdoAggregateFactory : AggregateFactory
    {

        public RelNode createAggregate(RelNode input, List hints, ImmutableBitSet groupSet, ImmutableList groupSets, List aggCalls)
        {
            Objects.requireNonNull(input.getConvention(), "input.getConvention()");

            try
            {
                return new AdoAggregate(input.getCluster(), input.getCluster().traitSetOf(input.getConvention()), input, groupSet, groupSets, aggCalls);
            }
            catch (InvalidRelException e)
            {
                throw new AssertionError(e);
            }
        }

    }

}
