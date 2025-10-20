using System;

using Apache.Calcite.Adapter.AdoNet.Rel;

using com.google.common.collect;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Relational expression representing a scan of a table in an ADO data source.
    /// </summary>
    public class AdoTableScan : TableScan, AdoRel
    {

        readonly AdoTable _adoTable;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="hints"></param>
        /// <param name="table"></param>
        /// <param name="adoTable"></param>
        internal AdoTableScan(RelOptCluster cluster, List hints, RelOptTable table, AdoTable adoTable) :
            base(cluster, cluster.traitSetOf(adoTable.Convention), hints, table)
        {
            _adoTable = adoTable ?? throw new ArgumentNullException(nameof(adoTable));
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, List inputs)
        {
            return new AdoTableScan(getCluster(), getHints(), table, _adoTable);
        }

        /// <inheritdoc />
        public override RelNode withHints(List value)
        {
            return new AdoTableScan(getCluster(), value, table, _adoTable);
        }

        /// <inheritdoc />
        public AdoImplementor.Result implement(AdoImplementor implementor)
        {
            return implementor.result(_adoTable.FullyQualifiedTableName, ImmutableList.of(AdoImplementor.Clause.FROM), this, null);
        }

    }

}
