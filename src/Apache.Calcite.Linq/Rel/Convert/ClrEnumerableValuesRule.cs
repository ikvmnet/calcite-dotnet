using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalValues"/> to a <see cref="ClrEnumerableValues"/>.
    /// </summary>
    public class ClrEnumerableValuesRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableValuesRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableValuesRule Create()
        {
            return (ClrEnumerableValuesRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalValues),
                    Convention.NONE,
                    ClrEnumerableConvention.Instance,
                    "ClrEnumerableValuesRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableValuesRule>(c => new ClrEnumerableValuesRule(c)))
                .toRule(typeof(ClrEnumerableValuesRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableValuesRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var values = (Values)rel;
            var clr = ClrEnumerableValues.Create(values.getCluster(), values.getRowType(), values.getTuples());

            return clr.copy(values.getTraitSet().replace(ClrEnumerableConvention.Instance), clr.getInputs());
        }

    }

}
