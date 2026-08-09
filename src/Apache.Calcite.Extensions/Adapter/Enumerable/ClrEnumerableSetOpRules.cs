using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalUnion"/> to a <see cref="ClrEnumerableUnion"/>.
    /// </summary>
    public class ClrEnumerableUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableUnionRule Create()
        {
            return (ClrEnumerableUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalUnion), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableUnionRule>(c => new ClrEnumerableUnionRule(c)))
                .toRule(typeof(ClrEnumerableUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var union = (Union)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrEnumerableConvention.Instance);

            var newInputs = new java.util.ArrayList();
            for (int i = 0; i < union.getInputs().size(); i++)
                newInputs.add(convert((RelNode)union.getInputs().get(i), traitSet));

            return new ClrEnumerableUnion(rel.getCluster(), traitSet, newInputs, union.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalIntersect"/> to a <see cref="ClrEnumerableIntersect"/>.
    /// </summary>
    public class ClrEnumerableIntersectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableIntersectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableIntersectRule Create()
        {
            return (ClrEnumerableIntersectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalIntersect), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableIntersectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableIntersectRule>(c => new ClrEnumerableIntersectRule(c)))
                .toRule(typeof(ClrEnumerableIntersectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableIntersectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var intersect = (Intersect)rel;
            var traitSet = intersect.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableIntersect(rel.getCluster(), traitSet, convertList(intersect.getInputs(), ClrEnumerableConvention.Instance), intersect.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalMinus"/> to a <see cref="ClrEnumerableMinus"/>.
    /// </summary>
    public class ClrEnumerableMinusRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableMinusRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableMinusRule Create()
        {
            return (ClrEnumerableMinusRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalMinus), Convention.NONE, ClrEnumerableConvention.Instance, "ClrEnumerableMinusRule")
                .withRuleFactory(new DelegateFunction<Config, ClrEnumerableMinusRule>(c => new ClrEnumerableMinusRule(c)))
                .toRule(typeof(ClrEnumerableMinusRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableMinusRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var minus = (Minus)rel;
            var traitSet = rel.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableMinus(rel.getCluster(), traitSet, convertList(minus.getInputs(), ClrEnumerableConvention.Instance), minus.all);
        }

    }

}
