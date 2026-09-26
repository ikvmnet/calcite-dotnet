using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalValues"/> to a <see cref="ClrCursorValues"/>.
    /// </summary>
    public class ClrCursorValuesRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorValuesRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorValuesRule Create()
        {
            return (ClrCursorValuesRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalValues),
                    Convention.NONE,
                    ClrCursorConvention.Instance,
                    "ClrCursorValuesRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorValuesRule>(c => new ClrCursorValuesRule(c)))
                .toRule(typeof(ClrCursorValuesRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorValuesRule(Config config) :
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
            var cursorValues = ClrCursorValues.Create(values.getCluster(), values.getRowType(), values.getTuples());

            return cursorValues.copy(values.getTraitSet().replace(ClrCursorConvention.Instance), cursorValues.getInputs());
        }

    }

}
