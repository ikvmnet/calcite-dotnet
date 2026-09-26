using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalValues"/> to a <see cref="ClrDataCursorValues"/>.
    /// </summary>
    public class ClrDataCursorValuesRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorValuesRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorValuesRule Create()
        {
            return (ClrDataCursorValuesRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalValues),
                    Convention.NONE,
                    ClrDataCursorConvention.Instance,
                    "ClrDataCursorValuesRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorValuesRule>(c => new ClrDataCursorValuesRule(c)))
                .toRule(typeof(ClrDataCursorValuesRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorValuesRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// Two statements, exactly as <c>EnumerableValuesRule</c>: build the node, then copy it onto the
        /// logical node's trait set with the convention swapped in. The logical node's traits are what the
        /// rest of the plan was matched against.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var values = (Values)rel;
            var cursorValues = ClrDataCursorValues.Create(values.getCluster(), values.getRowType(), values.getTuples());

            return cursorValues.copy(values.getTraitSet().replace(ClrDataCursorConvention.Instance), cursorValues.getInputs());
        }

    }

}
