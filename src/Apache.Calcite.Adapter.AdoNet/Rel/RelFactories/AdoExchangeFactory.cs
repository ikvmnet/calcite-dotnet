using java.lang;

using org.apache.calcite.rel;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="ExchangeFactory"/> implementation for the <see cref="AdoConvention"/>.
    /// Exchange operators are not supported by the ADO adapter; this factory always throws.
    /// </summary>
    public class AdoExchangeFactory : ExchangeFactory
    {

        /// <inheritdoc />
        public RelNode createExchange(RelNode input, RelDistribution distribution)
        {
            throw new UnsupportedOperationException("AdoExchange");
        }

    }

}
