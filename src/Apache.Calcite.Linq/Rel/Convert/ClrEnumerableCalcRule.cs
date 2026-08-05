using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCalc"/> to a <see cref="ClrEnumerableCalc"/>.
    /// </summary>
    public class ClrEnumerableCalcRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableCalcRule Create()
        {
            // the predicate ensures that if there is a multiset, FarragoMultisetSplitter works on it first
            return (ClrEnumerableCalcRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalCalc),
                    new DelegatePredicate<LogicalCalc>(RelOptUtil.notContainsWindowedAgg),
                    Convention.NONE,
                    ClrEnumerableConvention.Instance,
                    "ClrEnumerableCalcRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableCalcRule>(c => new ClrEnumerableCalcRule(c)))
                .toRule(typeof(ClrEnumerableCalcRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableCalcRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var calc = (Calc)rel;
            var input = calc.getInput();

            return ClrEnumerableCalc.Create(
                convert(input, input.getTraitSet().replace(ClrEnumerableConvention.Instance)),
                calc.getProgram());
        }

    }

}
