using java.lang;
using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.util;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    class AdoMatchFactory : MatchFactory
    {

        public RelNode createMatch(RelNode input, RexNode pattern, RelDataType rowType, bool strictStart, bool strictEnd, Map patternDefinitions, Map measures, RexNode after, Map subsets, bool allRows, ImmutableBitSet partitionKeys, RelCollation orderKeys, RexNode interval)
        {
            throw new UnsupportedOperationException("AdoMatch");
        }

    }

}
