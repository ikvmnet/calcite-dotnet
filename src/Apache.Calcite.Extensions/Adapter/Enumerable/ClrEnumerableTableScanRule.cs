using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableScan"/> to a <see cref="ClrEnumerableTableScan"/>.
    /// </summary>
    public class ClrEnumerableTableScanRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableTableScanRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableTableScanRule Create()
        {
            return (ClrEnumerableTableScanRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalTableScan),
                    new DelegatePredicate<LogicalTableScan>(r => ClrEnumerableTableScan.CanHandle(r.getTable())),
                    Convention.NONE,
                    ClrEnumerableConvention.Instance,
                    "ClrEnumerableTableScanRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableTableScanRule>(c => new ClrEnumerableTableScanRule(c)))
                .toRule(typeof(ClrEnumerableTableScanRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableTableScanRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// A table with no expression cannot be read by any plan of this convention, and refusing it here is
        /// the only place the refusal belongs: <c>Implement</c> runs after a plan has been chosen. A
        /// <c>QueryableTable</c> passes regardless, because Calcite's own test tables leave
        /// <c>getExpression</c> unimplemented and are still readable.
        /// </remarks>
        public override RelNode convert(RelNode rel)
        {
            var scan = (TableScan)rel;
            var relOptTable = scan.getTable();
            var table = (Table)relOptTable.unwrap(typeof(Table));

            // a table of this convention's own SPI is read directly and has no linq4j expression to ask
            // for. Asking is not merely redundant: RelOptTableImpl throws UnsupportedOperationException for
            // a table it has no class-expression function for, and one of ours always is.
            if (table is Apache.Calcite.Extensions.Schema.IClrScannableTable or Apache.Calcite.Extensions.Schema.IClrQueryableTable)
                return ClrEnumerableTableScan.Create(scan.getCluster(), relOptTable);

            if (table is QueryableTable || relOptTable.getExpression(typeof(object)) != null)
                return ClrEnumerableTableScan.Create(scan.getCluster(), relOptTable);

            return null!;
        }

    }

}
