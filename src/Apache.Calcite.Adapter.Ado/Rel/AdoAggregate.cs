using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.sql;
using org.apache.calcite.util;

namespace Apache.Calcite.Adapter.Ado.Rel
{

    /// <summary>
    /// Union operator implemented in ADO convention.
    /// </summary>
    class AdoAggregate : Aggregate, AdoRel
    {

        /// <summary>
        /// Returns whether this ADO data source can implement a given aggregate function.
        /// </summary>
        /// <param name="aggregateCall"></param>
        /// <param name="dialect"></param>
        /// <returns></returns>
        static bool CanImplement(AggregateCall aggregateCall, SqlDialect dialect)
        {
            return dialect.supportsAggregateFunction(aggregateCall.getAggregation().getKind()) && aggregateCall.distinctKeys == null;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="groupSet"></param>
        /// <param name="groupSets"></param>
        /// <param name="aggCalls"></param>
        public AdoAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List? groupSets, List aggCalls) :
            base(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls)
        {
            var dialect = ((AdoConvention)getConvention()).Dialect;
            foreach (var aggCall in aggCalls.AsEnumerable<AggregateCall>())
            {
                if (!CanImplement(aggCall, dialect))
                    throw new InvalidRelException($"cannot implement aggregate function {aggCall}");

                if (aggCall.hasFilter() && !dialect.supportsAggregateFunctionFilter())
                    throw new InvalidRelException($"dialect does not support aggregate functions FILTER clauses");
            }
        }

        /// <inheritdoc />
        public override Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List? groupSets, List aggCalls)
        {
            try
            {
                return new AdoAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
            }
            catch (InvalidRelException e)
            {
                throw new AssertionError(e);
            }
        }

        /// <inheritdoc />
        public SqlImplementor.Result implement(AdoImplementor implementor)
        {
            return implementor.implement(this);
        }

    }

}
