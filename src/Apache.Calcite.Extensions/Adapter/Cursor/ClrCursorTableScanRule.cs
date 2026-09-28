using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableScan"/> to a <see cref="ClrCursorTableScan"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableTableScanRule</c>. Only tables that <see cref="ClrCursorTableScan.CanHandle(RelOptTable)"/>
    /// accepts are matched.
    /// </remarks>
    public class ClrCursorTableScanRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorTableScanRule Create()
        {
            return (ClrCursorTableScanRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalTableScan),
                    new DelegatePredicate<LogicalTableScan>(r => ClrCursorTableScan.CanHandle(r.getTable())),
                    Convention.NONE,
                    ClrCursorConvention.Instance,
                    "ClrCursorTableScanRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorTableScanRule>(c => new ClrCursorTableScanRule(c)))
                .toRule(typeof(ClrCursorTableScanRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule's configuration.</param>
        public ClrCursorTableScanRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// Declines a table that has no expression and is not of this project's table SPI, because no scan
        /// could read it; it is refused here rather than when the plan is implemented.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var scan = (TableScan)rel;
            var relOptTable = scan.getTable();
            var table = (Table)relOptTable.unwrap(typeof(Table));

            // a table of this project's SPI is not asked for an expression: RelOptTableImpl throws
            // UnsupportedOperationException for a table it has no class-expression function for
            if (table is Apache.Calcite.Extensions.Schema.IClrScannableTable
                or Apache.Calcite.Extensions.Schema.IClrQueryableTable
                or Apache.Calcite.Extensions.Schema.IClrCursorTable)
                return ClrCursorTableScan.Create(scan.getCluster(), relOptTable);

            if (table is QueryableTable || relOptTable.getExpression(typeof(object)) != null)
                return ClrCursorTableScan.Create(scan.getCluster(), relOptTable);

            return null;
        }

    }

}
