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
    /// <remarks>
    /// Mirrors <c>EnumerableValuesRule</c>.
    /// </remarks>
    public class ClrCursorValuesRule : ConverterRule
    {

        /// <summary>
        /// Creates the rule with its default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
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
        /// <param name="config">The rule's configuration.</param>
        public ClrCursorValuesRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// As in <c>EnumerableValuesRule</c>, the new node is copied onto the logical node's traits with the
        /// convention replaced, so it keeps the traits the rest of the plan was matched against.
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var values = (Values)rel;
            var cursorValues = ClrCursorValues.Create(values.getCluster(), values.getRowType(), values.getTuples());

            return cursorValues.copy(values.getTraitSet().replace(ClrCursorConvention.Instance), cursorValues.getInputs());
        }

    }

}
