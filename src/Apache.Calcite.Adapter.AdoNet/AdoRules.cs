using System.Collections.Generic;

using Apache.Calcite.Adapter.AdoNet.Rel.Convert;
using Apache.Calcite.Adapter.AdoNet.Rel.RelFactories;

using org.apache.calcite.plan;
using org.apache.calcite.tools;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The planner rules of an <see cref="AdoConvention"/>. The counterpart of Calcite's <c>JdbcRules</c>.
    /// </summary>
    /// <remarks>
    /// An <see cref="AdoConvention"/> adds its rules to a planner itself, so a caller needs these only to drive a
    /// planner some other way.
    /// </remarks>
    public static class AdoRules
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
        /// A <see cref="RelBuilderFactory"/> whose <see cref="RelBuilder"/> creates the adapter's own nodes for every
        /// kind of relational expression it builds. Mirrors <c>JdbcRules.JDBC_BUILDER</c>.
        /// </summary>
        /// <remarks>
        /// A node is created in its input's convention. Several factories do not create a node at all and throw
        /// (exchange, sort exchange, match, snapshot, sort, table scan, values), and the filter and project
        /// factories throw when given no correlation variables, which is the reverse of <c>JdbcRules</c>; see each
        /// factory in <c>Apache.Calcite.Adapter.AdoNet.Rel.RelFactories</c>. Nothing in the adapter uses this.
        /// </remarks>
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
        /// Returns the rules for a convention: the converters out of it into <c>EnumerableConvention</c> and
        /// <c>ClrCursorConvention</c>, and the rules that convert a join, project, filter, aggregate, sort, union,
        /// intersect, minus and values into it.
        /// </summary>
        /// <param name="convention">The convention.</param>
        /// <returns>The rules.</returns>
        public static IEnumerable<RelOptRule> GetRules(AdoConvention convention)
        {
            yield return AdoToEnumerableConverterRule.Create(convention);
            yield return AdoToClrCursorConverterRule.Create(convention);
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
        /// Returns the rules of <see cref="GetRules(AdoConvention)"/>, each configured with the given
        /// <see cref="RelBuilderFactory"/>.
        /// </summary>
        /// <param name="convention">The convention.</param>
        /// <param name="relBuilderFactory">The factory the rules build relational expressions with.</param>
        /// <returns>The rules.</returns>
        public static IEnumerable<RelOptRule> GetRules(AdoConvention convention, RelBuilderFactory relBuilderFactory)
        {
            yield return AdoToEnumerableConverterRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
            yield return AdoToClrCursorConverterRule.Create(convention).config.withRelBuilderFactory(relBuilderFactory).toRule();
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
