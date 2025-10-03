using System;

using java.lang;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rex;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.Ado.Rel.RelFactories
{

    public class AdoSortFactory : SortFactory
    {

        public RelNode createSort(RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
        {
            throw new UnsupportedOperationException("AdoSort");
        }

        public RelNode createSort(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch)
        {
            throw new NotImplementedException();
        }

    }

}
