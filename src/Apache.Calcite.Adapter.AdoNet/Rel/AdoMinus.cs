using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// Minus operator implemented in ADO convention
    /// </summary>
    class AdoMinus : Minus, AdoRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        /// <param name="all"></param>
        public AdoMinus(RelOptCluster cluster, RelTraitSet traitSet, List inputs, bool all) :
            base(cluster, traitSet, inputs, all)
        {

        }

        /// <inheritdoc />

        public override SetOp copy(RelTraitSet traitSet, List inputs, bool all)
        {
            return new AdoMinus(getCluster(), traitSet, inputs, all);
        }

        /// <inheritdoc />
        public SqlImplementor.Result implement(AdoImplementor implementor)
        {
            return implementor.implement(this);
        }

    }

}
