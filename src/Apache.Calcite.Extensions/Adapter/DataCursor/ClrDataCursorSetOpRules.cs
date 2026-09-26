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

    /// <summary>
    /// Rule that converts a <see cref="LogicalIntersect"/> to a <see cref="ClrDataCursorIntersect"/>.
    /// </summary>
    public class ClrDataCursorIntersectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorIntersectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorIntersectRule Create()
        {
            return (ClrDataCursorIntersectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalIntersect), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorIntersectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorIntersectRule>(c => new ClrDataCursorIntersectRule(c)))
                .toRule(typeof(ClrDataCursorIntersectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorIntersectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var intersect = (Intersect)rel;
            var traitSet = intersect.getTraitSet().replace(ClrDataCursorConvention.Instance);

            return new ClrDataCursorIntersect(rel.getCluster(), traitSet, convertList(intersect.getInputs(), ClrDataCursorConvention.Instance), intersect.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalMinus"/> to a <see cref="ClrDataCursorMinus"/>.
    /// </summary>
    public class ClrDataCursorMinusRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorMinusRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorMinusRule Create()
        {
            return (ClrDataCursorMinusRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalMinus), Convention.NONE, ClrDataCursorConvention.Instance, "ClrDataCursorMinusRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorMinusRule>(c => new ClrDataCursorMinusRule(c)))
                .toRule(typeof(ClrDataCursorMinusRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorMinusRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var minus = (Minus)rel;
            var traitSet = rel.getTraitSet().replace(ClrDataCursorConvention.Instance);

            return new ClrDataCursorMinus(rel.getCluster(), traitSet, convertList(minus.getInputs(), ClrDataCursorConvention.Instance), minus.all);
        }

    }

}
