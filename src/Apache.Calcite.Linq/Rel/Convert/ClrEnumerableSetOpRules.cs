using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;

namespace Apache.Calcite.Linq.Rel.Convert
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
        public override RelNode convert(RelNode rel)
        {
            var union = (Union)rel;
            var traitSet = union.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableUnion(rel.getCluster(), traitSet, SetOps.Convert(this, union, traitSet), union.all);
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
        public override RelNode convert(RelNode rel)
        {
            var intersect = (Intersect)rel;
            var traitSet = intersect.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableIntersect(rel.getCluster(), traitSet, SetOps.Convert(this, intersect, traitSet), intersect.all);
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
        public override RelNode convert(RelNode rel)
        {
            var minus = (Minus)rel;
            var traitSet = minus.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableMinus(rel.getCluster(), traitSet, SetOps.Convert(this, minus, traitSet), minus.all);
        }

    }

    /// <summary>
    /// Brings the inputs of a set operation into this convention.
    /// </summary>
    static class SetOps
    {

        /// <summary>
        /// Returns the inputs of a set operation, each converted.
        /// </summary>
        /// <param name="rule"></param>
        /// <param name="setOp"></param>
        /// <param name="traitSet"></param>
        /// <returns></returns>
        public static java.util.List Convert(ConverterRule rule, SetOp setOp, RelTraitSet traitSet)
        {
            var inputs = new java.util.ArrayList();
            for (int i = 0; i < setOp.getInputs().size(); i++)
                inputs.add(RelOptRule.convert((RelNode)setOp.getInputs().get(i), traitSet));

            return inputs;
        }

    }

}
