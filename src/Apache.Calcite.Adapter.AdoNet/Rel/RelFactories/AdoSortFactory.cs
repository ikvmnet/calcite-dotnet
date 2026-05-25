using System;

using java.lang;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="SortFactory"/> implementation for the <see cref="AdoConvention"/>.
    /// Sort pushdown is not supported by the ADO adapter; both factory methods throw.
    /// </summary>
    public class AdoSortFactory : SortFactory
    {

        /// <inheritdoc />
        public RelNode createSort(RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
        {
            throw new UnsupportedOperationException("AdoSort");
        }

        /// <inheritdoc />
        public RelNode createSort(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
        {
            throw new NotImplementedException();
        }

    }

}
