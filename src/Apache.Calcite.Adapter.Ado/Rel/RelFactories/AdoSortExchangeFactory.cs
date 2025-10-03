using java.lang;

using org.apache.calcite.rel;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.Ado.Rel.RelFactories
{

    public class AdoSortExchangeFactory : SortExchangeFactory
    {

        public RelNode createSortExchange(RelNode input, RelDistribution distribution, RelCollation collation)
        {
            throw new UnsupportedOperationException("AdoSortExchange");
        }

    }

}
