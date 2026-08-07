using java.util.function;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
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
        /// <remarks>
        /// Two statements, exactly as <c>EnumerableValuesRule</c>: build the node, then copy it onto the
        /// logical node's trait set with the convention swapped in. The copy is not redundant — the logical
        /// node's traits are what the rest of the plan was matched against.
        ///
        /// <para>This was once cut down to the first statement, on a misreading of Calcite's rule, to explain
        /// a <c>RelCompositeTrait to RelCollation</c> cast failure. That failure is
        /// <see cref="ClrEnumerableValues.passThrough"/> having been missing, not this.</para>
        /// </remarks>
        public override RelNode convert(RelNode rel)
        {
            var values = (Values)rel;
            var enumerableValues = ClrEnumerableValues.Create(values.getCluster(), values.getRowType(), values.getTuples());

            return enumerableValues.copy(values.getTraitSet().replace(ClrEnumerableConvention.Instance), enumerableValues.getInputs());
        }

    }

}
