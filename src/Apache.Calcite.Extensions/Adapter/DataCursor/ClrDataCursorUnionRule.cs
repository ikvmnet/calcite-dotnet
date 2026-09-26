using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalUnion"/> to a <see cref="ClrDataCursorUnion"/>.
    /// </summary>
    public class ClrDataCursorUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorUnionRule Create()
        {
            return (ClrDataCursorUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalUnion), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorUnionRule>(c => new ClrDataCursorUnionRule(c)))
                .toRule(typeof(ClrDataCursorUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (Union)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrDataCursorConvention.Instance);

            var newInputs = new java.util.ArrayList();
            for (int i = 0; i < union.getInputs().size(); i++)
                newInputs.add(convert((RelNode)union.getInputs().get(i), traitSet));

            return new ClrDataCursorUnion(rel.getCluster(), traitSet, newInputs, union.all);
        }

    }

}
