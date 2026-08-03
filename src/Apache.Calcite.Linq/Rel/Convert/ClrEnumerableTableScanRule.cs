using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
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
                    new DelegatePredicate<LogicalTableScan>(r => EnumerableTableScan.canHandle(r.getTable())),
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
        public override RelNode convert(RelNode rel)
        {
            var scan = (LogicalTableScan)rel;

            return ClrEnumerableTableScan.Create(scan.getCluster(), scan.getTable());
        }

    }

}
