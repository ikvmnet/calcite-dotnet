using java.lang;

using org.apache.calcite.rel;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="SortExchangeFactory"/> implementation for the <see cref="AdoConvention"/>.
    /// Sort-exchange operators are not supported by the ADO adapter; this factory always throws.
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
