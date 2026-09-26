using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableFunctionScan"/> to a
    /// <see cref="ClrDataCursorTableFunctionScan"/>.
    /// </summary>
    public class ClrDataCursorTableFunctionScanRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorTableFunctionScanRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorTableFunctionScanRule Create()
        {
            return (ClrDataCursorTableFunctionScanRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableFunctionScan), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorTableFunctionScanRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorTableFunctionScanRule>(c => new ClrDataCursorTableFunctionScanRule(c)))
                .toRule(typeof(ClrDataCursorTableFunctionScanRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorTableFunctionScanRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var scan = (TableFunctionScan)rel;
            var traitSet = rel.getTraitSet().replace(ClrDataCursorConvention.Instance);

            return new ClrDataCursorTableFunctionScan(
                rel.getCluster(),
                traitSet,
                convertList(scan.getInputs(), traitSet.getTrait(0)),
                scan.getElementType(),
                scan.getRowType(),
                scan.getCall(),
                scan.getColumnMappings());
        }

    }

}
