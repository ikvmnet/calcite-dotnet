using java.lang;

using org.apache.calcite.rel;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="SortExchangeFactory"/> that throws: the adapter has no sort-exchange node.
    /// </summary>
    public class AdoSortExchangeFactory : SortExchangeFactory
    {

        /// <inheritdoc />
        public RelNode createSortExchange(RelNode input, RelDistribution distribution, RelCollation collation)
        {
            throw new UnsupportedOperationException("AdoSortExchange");
        }

    }

}
