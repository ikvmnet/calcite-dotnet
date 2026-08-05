using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalTableFunctionScan"/> to a
    /// <see cref="ClrEnumerableTableFunctionScan"/>.
    /// </summary>
    public class ClrEnumerableTableFunctionScanRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableTableFunctionScanRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableTableFunctionScanRule Create()
        {
            return (ClrEnumerableTableFunctionScanRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalTableFunctionScan), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableTableFunctionScanRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableTableFunctionScanRule>(c => new ClrEnumerableTableFunctionScanRule(c)))
                .toRule(typeof(ClrEnumerableTableFunctionScanRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableTableFunctionScanRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var scan = (TableFunctionScan)rel;
            var traitSet = rel.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableTableFunctionScan(
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
