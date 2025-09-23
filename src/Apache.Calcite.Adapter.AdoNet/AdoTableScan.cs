using System;

using com.google.common.collect;

using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;

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
        /// <param name="adoConvention"></param>
        internal AdoTableScan(RelOptCluster cluster, List hints, RelOptTable table, AdoTable adoTable, AdoConvention adoConvention) :
            base(cluster, cluster.traitSetOf(adoConvention), hints, table)
        {
            _adoTable = adoTable ?? throw new ArgumentNullException(nameof(adoTable));
        }

        /// <inheritdoc />
        public SqlImplementor.Result implement(AdoImplementor implementor)
        {
            return implementor.result(_adoTable.FullyQualifiedTableName, ImmutableList.of(AdoImplementor.Clause.FROM), null, null);
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, List inputs)
        {
            return new AdoTableScan(getCluster(), getHints(), table, _adoTable, (AdoConvention)getConvention());
        }

        /// <inheritdoc />
        public override RelNode withHints(List value)
        {
            return new AdoTableScan(getCluster(), value, table, _adoTable, (AdoConvention)getConvention());
        }

    }

}
