using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalUnion"/> to a <see cref="ClrCursorUnion"/>.
    /// </summary>
    public class ClrCursorUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorUnionRule Create()
        {
            return (ClrCursorUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalUnion), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorUnionRule>(c => new ClrCursorUnionRule(c)))
                .toRule(typeof(ClrCursorUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (Union)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrCursorConvention.Instance);

            var newInputs = new java.util.ArrayList();
            for (int i = 0; i < union.getInputs().size(); i++)
                newInputs.add(convert((RelNode)union.getInputs().get(i), traitSet));

            return new ClrCursorUnion(rel.getCluster(), traitSet, newInputs, union.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalIntersect"/> to a <see cref="ClrCursorIntersect"/>.
    /// </summary>
    public class ClrCursorIntersectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorIntersectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorIntersectRule Create()
        {
            return (ClrCursorIntersectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalIntersect), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorIntersectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorIntersectRule>(c => new ClrCursorIntersectRule(c)))
                .toRule(typeof(ClrCursorIntersectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorIntersectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var intersect = (Intersect)rel;
            var traitSet = intersect.getTraitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorIntersect(rel.getCluster(), traitSet, convertList(intersect.getInputs(), ClrCursorConvention.Instance), intersect.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalMinus"/> to a <see cref="ClrCursorMinus"/>.
    /// </summary>
    public class ClrCursorMinusRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorMinusRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrCursorMinusRule Create()
        {
            return (ClrCursorMinusRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalMinus), Convention.NONE, ClrCursorConvention.Instance, "ClrCursorMinusRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorMinusRule>(c => new ClrCursorMinusRule(c)))
                .toRule(typeof(ClrCursorMinusRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrCursorMinusRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var minus = (Minus)rel;
            var traitSet = rel.getTraitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorMinus(rel.getCluster(), traitSet, convertList(minus.getInputs(), ClrCursorConvention.Instance), minus.all);
        }

    }

}
