using java.lang;

using org.apache.calcite.rel;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// An <see cref="ExchangeFactory"/> that throws: the adapter has no exchange node.
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
