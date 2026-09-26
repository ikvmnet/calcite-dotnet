using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableFunctionScan"/> to a
    /// <see cref="ClrCursorTableFunctionScan"/>.
    /// </summary>
    public class ClrCursorTableFunctionScanRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorTableFunctionScanRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorTableFunctionScanRule Create()
        {
            return (ClrCursorTableFunctionScanRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableFunctionScan), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorTableFunctionScanRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorTableFunctionScanRule>(c => new ClrCursorTableFunctionScanRule(c)))
                .toRule(typeof(ClrCursorTableFunctionScanRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorTableFunctionScanRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var scan = (TableFunctionScan)rel;
            var traitSet = rel.getTraitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorTableFunctionScan(
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
