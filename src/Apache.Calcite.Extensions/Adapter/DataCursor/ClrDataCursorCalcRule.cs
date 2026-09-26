using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCalc"/> to a <see cref="ClrDataCursorCalc"/>.
    /// </summary>
    public class ClrDataCursorCalcRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorCalcRule Create()
        {
            // the predicate ensures that if there is a multiset, FarragoMultisetSplitter works on it first
            return (ClrDataCursorCalcRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalCalc),
                    new DelegatePredicate<LogicalCalc>(RelOptUtil.notContainsWindowedAgg),
                    Convention.NONE,
                    ClrDataCursorConvention.Instance,
                    "ClrDataCursorCalcRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorCalcRule>(c => new ClrDataCursorCalcRule(c)))
                .toRule(typeof(ClrDataCursorCalcRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorCalcRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var calc = (Calc)rel;
            var input = calc.getInput();

            return ClrDataCursorCalc.Create(
                convert(input, input.getTraitSet().replace(ClrDataCursorConvention.Instance)),
                calc.getProgram());
        }

    }

}
