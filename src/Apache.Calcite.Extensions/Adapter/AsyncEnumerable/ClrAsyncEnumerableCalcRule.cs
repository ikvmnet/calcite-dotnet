using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalCalc"/> to a <see cref="ClrAsyncEnumerableCalc"/>.
    /// </summary>
    public class ClrAsyncEnumerableCalcRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableCalcRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableCalcRule Create()
        {
            // the predicate ensures that if there is a multiset, FarragoMultisetSplitter works on it first
            return (ClrAsyncEnumerableCalcRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalCalc),
                    new DelegatePredicate<LogicalCalc>(RelOptUtil.notContainsWindowedAgg),
                    Convention.NONE,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrAsyncEnumerableCalcRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableCalcRule>(c => new ClrAsyncEnumerableCalcRule(c)))
                .toRule(typeof(ClrAsyncEnumerableCalcRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableCalcRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var calc = (Calc)rel;
            var input = calc.getInput();

            return ClrAsyncEnumerableCalc.Create(
                convert(input, input.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance)),
                calc.getProgram());
        }

    }

}
