using java.lang;
using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="ValuesFactory"/> implementation for the <see cref="AdoConvention"/>.
    /// Inline values are handled by the converter rule; this factory always throws.
    /// </summary>
    public class AdoValuesFactory : ValuesFactory
    {

        /// <inheritdoc />
        public RelNode createValues(RelOptCluster cluster, RelDataType rowType, List tuples)
        {
            throw new UnsupportedOperationException();
        }

    }

}
