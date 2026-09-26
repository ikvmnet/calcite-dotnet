using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCalc"/> to a <see cref="ClrCursorCalc"/>.
    /// </summary>
    public class ClrCursorCalcRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorCalcRule Create()
        {
            // the predicate ensures that if there is a multiset, FarragoMultisetSplitter works on it first
            return (ClrCursorCalcRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalCalc),
                    new DelegatePredicate<LogicalCalc>(RelOptUtil.notContainsWindowedAgg),
                    Convention.NONE,
                    ClrCursorConvention.Instance,
                    "ClrCursorCalcRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorCalcRule>(c => new ClrCursorCalcRule(c)))
                .toRule(typeof(ClrCursorCalcRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorCalcRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var calc = (Calc)rel;
            var input = calc.getInput();

            return ClrCursorCalc.Create(
                convert(input, input.getTraitSet().replace(ClrCursorConvention.Instance)),
                calc.getProgram());
        }

    }

}
