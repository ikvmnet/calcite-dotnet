﻿using java.lang;

using org.apache.calcite.rel;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    public class AdoExchangeFactory : ExchangeFactory
    {

        public RelNode createExchange(RelNode input, RelDistribution distribution)
        {
            throw new UnsupportedOperationException("AdoExchange");
        }

    }

}
