using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.sql;
using org.apache.calcite.util;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// An aggregate pushed down to the source as <c>GROUP BY</c>. Mirrors <c>JdbcRules.JdbcAggregate</c>.
    /// </summary>
    public class AdoAggregate : Aggregate, AdoRel
    {

        /// <summary>
        /// Returns whether the dialect supports the call's aggregate function, and the call has no
        /// <c>WITHIN DISTINCT</c> keys.
        /// </summary>
        /// <param name="aggregateCall">The call.</param>
        /// <param name="dialect">The source's dialect.</param>
        /// <returns>Whether the call can be pushed down.</returns>
        static bool CanImplement(AggregateCall aggregateCall, SqlDialect dialect)
        {
            return dialect.supportsAggregateFunction(aggregateCall.getAggregation().getKind()) && aggregateCall.distinctKeys == null;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The traits, whose convention must be an <see cref="AdoConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="groupSet">The grouping columns.</param>
        /// <param name="groupSets">The grouping sets, or <see langword="null"/> for <paramref name="groupSet"/> alone.</param>
        /// <param name="aggCalls">The aggregate calls.</param>
        /// <exception cref="InvalidRelException">The dialect does not support an aggregate function, or a
        /// <c>FILTER</c> clause one of the calls has.</exception>
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
        /// <exception cref="AdoCalciteException">The copy cannot be pushed down.</exception>
        public override Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List? groupSets, List aggCalls)
        {
            try
            {
                return new AdoAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
            }
            catch (InvalidRelException e)
            {
                throw new AdoCalciteException("Failed to implement ADO aggregate.", e);
            }
        }

    }

}
