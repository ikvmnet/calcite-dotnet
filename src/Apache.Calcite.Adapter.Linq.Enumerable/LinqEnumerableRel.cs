using org.apache.calcite.plan;
using org.apache.calcite.rel;

using System.Collections.Generic;

namespace Apache.Calcite.Adapter.Linq.Enumerable
{

    public partial interface LinqEnumerableRel : PhysicalNode
    {

        public (RelTraitSet, List<RelTraitSet>)? PassThroughTraits(RelTraitSet required)
        {
            return null;
        }

        public (RelTraitSet, List<RelTraitSet>)? DeriveTraits(RelTraitSet required)
        {
            return null;
        }

        public DeriveMode GetDeriveMode()
        {
            return DeriveMode.LEFT_FIRST;
        }

        LinqEnumerableRelResult Implement(LinqEnumerableRelImplementor implementor);

    }

}
