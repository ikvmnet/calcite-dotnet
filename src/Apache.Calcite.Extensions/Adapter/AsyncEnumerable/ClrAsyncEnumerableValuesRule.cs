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
    /// Rule that converts a <see cref="LogicalValues"/> to a <see cref="ClrAsyncEnumerableValues"/>.
    /// </summary>
    public class ClrAsyncEnumerableValuesRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableValuesRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableValuesRule Create()
        {
            return (ClrAsyncEnumerableValuesRule)Config.INSTANCE
                .withConversion(
                    (java.lang.Class)typeof(LogicalValues),
                    Convention.NONE,
                    ClrAsyncEnumerableConvention.Instance,
                    "ClrAsyncEnumerableValuesRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableValuesRule>(c => new ClrAsyncEnumerableValuesRule(c)))
                .toRule(typeof(ClrAsyncEnumerableValuesRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableValuesRule(Config config) :
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
        /// <see cref="ClrAsyncEnumerableValues.passThrough"/> having been missing, not this.</para>
        /// </remarks>
        public override RelNode? convert(RelNode rel)
        {
            var values = (Values)rel;
            var enumerableValues = ClrAsyncEnumerableValues.Create(values.getCluster(), values.getRowType(), values.getTuples());

            return enumerableValues.copy(values.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance), enumerableValues.getInputs());
        }

    }

}
