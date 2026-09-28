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
        /// <returns>The rule.</returns>
        public static ClrCursorCalcRule Create()
        {
            // as EnumerableCalcRule, a calc containing a windowed aggregate is not converted
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
        /// <param name="config">The rule configuration.</param>
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
