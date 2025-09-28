using System.Collections.Generic;

using Apache.Calcite.Adapter.AdoNet.Rel.Convert;
using Apache.Calcite.Adapter.AdoNet.Rel.RelFactories;

using org.apache.calcite.plan;
using org.apache.calcite.tools;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Rules and relational operators for the <see cref="AdoConvention"/> calling convention.
    /// </summary>
    static class AdoRules
    {

        static readonly ProjectFactory PROJECT_FACTORY = new AdoProjectFactory();
        static readonly FilterFactory FILTER_FACTORY = new AdoFilterFactory();
        static readonly JoinFactory JOIN_FACTORY = new AdoJoinFactory();
        static readonly SortFactory SORT_FACTORY = new AdoSortFactory();
        static readonly ExchangeFactory EXCHANGE_FACTORY = new AdoExchangeFactory();
        static readonly SortExchangeFactory SORT_EXCHANGE_FACTORY = new AdoSortExchangeFactory();
        static readonly AggregateFactory AGGREGATE_FACTORY = new AdoAggregateFactory();
        static readonly MatchFactory MATCH_FACTORY = new AdoMatchFactory();
        static readonly SetOpFactory SET_OP_FACTORY = new AdoSetOpFactory();
        static readonly ValuesFactory VALUES_FACTORY = new AdoValuesFactory();
        static readonly TableScanFactory TABLE_SCAN_FACTORY = new AdoTableScanFactory();
        static readonly SnapshotFactory SNAPSHOT_FACTORY = new AdoSnapshotFactory();

        /// <summary>
        /// A <see cref="RelBuilderFactory"/> that creates a <see cref="RelBuilder"/> that will crate ADO relational expressions for everything.
        /// </summary>
        public static readonly RelBuilderFactory Builder = RelBuilder.proto(Contexts.of(
            PROJECT_FACTORY,
            FILTER_FACTORY,
            JOIN_FACTORY,
            SORT_FACTORY,
            EXCHANGE_FACTORY,
            SORT_EXCHANGE_FACTORY,
            AGGREGATE_FACTORY,
            MATCH_FACTORY,
            SET_OP_FACTORY,
            VALUES_FACTORY,
            TABLE_SCAN_FACTORY,
            SNAPSHOT_FACTORY));

        /// <summary>
        /// Creates a list of rules with the given ADO convention instance.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        public static IEnumerable<RelOptRule> GetRules(AdoConvention convention)
        {
            //yield return AdoToEnumerableConverterRule.Create(convention);
            yield return AdoJoinRule.Create(convention);
            yield return AdoProjectRule.Create(convention);
            yield return AdoFilterRule.Create(convention);
            yield return AdoAggregateRule.Create(convention);
            yield return AdoSortRule.Create(convention);
            yield return AdoUnionRule.Create(convention);
            yield return AdoIntersectRule.Create(convention);
            yield return AdoMinusRule.Create(convention);
            yield return AdoValuesRule.Create(convention);
        }
        /// <summary>
        /// Creates a list of rules with the given ADO convention instance.
        /// </summary>
        /// <param name="convention"></param>
        /// <param name="relBuilderFactory"></param>
        /// <returns></returns>
        public static IEnumerable<RelOptRule> GetRules(AdoConvention convention, RelBuilderFactory relBuilderFactory)
        {
            //yield return AdoToEnumerableConverterRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoJoinRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoProjectRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoFilterRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoAggregateRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoSortRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoUnionRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoIntersectRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoMinusRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoValuesRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
        }

    }

}
