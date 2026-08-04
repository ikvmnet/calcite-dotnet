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
        /// <remarks>
        /// A window table function — TUMBLE, HOP, SESSION — is refused, so the planner leaves it in
        /// <c>EnumerableConvention</c> and the converters carry the rows. That path works and this one does
        /// not: see <c>TODO.md</c>. Refused here rather than in <c>Implement</c>, which runs after
        /// <c>findBestExp</c> and so cannot decline anything.
        /// </remarks>
        public override bool matches(RelOptRuleCall call)
        {
            return call.rel(0) is TableFunctionScan scan
                && scan.getCall() is RexCall rexCall
                && IsWindowTableFunction(rexCall) == false;
        }

        /// <summary>
        /// Returns whether the call is one <c>RexImpTable</c> generates rather than one the schema defines.
        /// </summary>
        /// <param name="call"></param>
        /// <returns></returns>
        static bool IsWindowTableFunction(RexCall call)
        {
            return call.getOperator() is org.apache.calcite.sql.SqlWindowTableFunction window
                && org.apache.calcite.adapter.enumerable.RexImpTable.INSTANCE.get(window) != null;
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
