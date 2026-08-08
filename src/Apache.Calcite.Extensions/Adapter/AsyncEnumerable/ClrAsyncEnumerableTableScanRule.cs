using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.schema;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableScan"/> to a <see cref="ClrAsyncEnumerableTableScan"/>.
    /// </summary>
    public class ClrAsyncEnumerableTableScanRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableTableScanRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableTableScanRule Create()
        {
            return (ClrAsyncEnumerableTableScanRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalTableScan),
                    new DelegatePredicate<LogicalTableScan>(r => ClrAsyncEnumerableTableScan.CanHandle(r.getTable())),
                    Convention.NONE,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrAsyncEnumerableTableScanRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableTableScanRule>(c => new ClrAsyncEnumerableTableScanRule(c)))
                .toRule(typeof(ClrAsyncEnumerableTableScanRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableTableScanRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// The whole test is <c>CanHandle</c>, which the configuration above already applies, and it asks one
        /// question: is this an <see cref="Schema.IClrAsyncScannableTable"/>.
        ///
        /// <para>Where the synchronous rule also asks whether the table has an expression, this does not,
        /// because no plan of this convention ever calls <c>getExpression</c> — the scan reaches the table as
        /// a constant and calls <c>ScanAsync</c> on it. Asking is not merely redundant: <c>RelOptTableImpl</c>
        /// throws <c>UnsupportedOperationException</c> for a table it has no class-expression function for,
        /// and an asynchronous table is exactly that, so the question the synchronous rule asks safely is one
        /// this one cannot ask at all.</para>
        /// </remarks>
        public override RelNode convert(RelNode rel)
        {
            var scan = (TableScan)rel;

            return ClrAsyncEnumerableTableScan.Create(scan.getCluster(), scan.getTable());
        }

    }

}
