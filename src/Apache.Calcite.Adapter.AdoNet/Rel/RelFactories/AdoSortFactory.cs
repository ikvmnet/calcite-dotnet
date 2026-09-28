using System;

using java.lang;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="SortFactory"/> that throws from both members.
    /// </summary>
    /// <remarks>
    /// Sorts are still pushed down: <c>AdoSortRule</c> creates an <see cref="AdoSort"/> directly. Only building one
    /// through <see cref="AdoRules.Builder"/> is unsupported.
    /// </remarks>
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
