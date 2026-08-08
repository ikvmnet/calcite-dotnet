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
    /// Rule that converts a <see cref="LogicalUnion"/> to a <see cref="ClrAsyncEnumerableUnion"/>.
    /// </summary>
    public class ClrAsyncEnumerableUnionRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableUnionRule Create()
        {
            return (ClrAsyncEnumerableUnionRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalUnion), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableUnionRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableUnionRule>(c => new ClrAsyncEnumerableUnionRule(c)))
                .toRule(typeof(ClrAsyncEnumerableUnionRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableUnionRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var union = (Union)rel;
            var traitSet = rel.getCluster().traitSet().replace(ClrAsyncEnumerableConvention.Instance);

            var newInputs = new java.util.ArrayList();
            for (int i = 0; i < union.getInputs().size(); i++)
                newInputs.add(convert((RelNode)union.getInputs().get(i), traitSet));

            return new ClrAsyncEnumerableUnion(rel.getCluster(), traitSet, newInputs, union.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalIntersect"/> to a <see cref="ClrAsyncEnumerableIntersect"/>.
    /// </summary>
    public class ClrAsyncEnumerableIntersectRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableIntersectRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableIntersectRule Create()
        {
            return (ClrAsyncEnumerableIntersectRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalIntersect), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableIntersectRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableIntersectRule>(c => new ClrAsyncEnumerableIntersectRule(c)))
                .toRule(typeof(ClrAsyncEnumerableIntersectRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableIntersectRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var intersect = (Intersect)rel;
            var traitSet = intersect.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance);

            return new ClrAsyncEnumerableIntersect(rel.getCluster(), traitSet, convertList(intersect.getInputs(), ClrAsyncEnumerableConvention.Instance), intersect.all);
        }

    }

    /// <summary>
    /// Rule that converts a <see cref="LogicalMinus"/> to a <see cref="ClrAsyncEnumerableMinus"/>.
    /// </summary>
    public class ClrAsyncEnumerableMinusRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableMinusRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrAsyncEnumerableMinusRule Create()
        {
            return (ClrAsyncEnumerableMinusRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(LogicalMinus), Convention.NONE, ClrAsyncEnumerableConvention.Instance, "ClrAsyncEnumerableMinusRule")
                .withRuleFactory(new DelegateFunction<Config, ClrAsyncEnumerableMinusRule>(c => new ClrAsyncEnumerableMinusRule(c)))
                .toRule(typeof(ClrAsyncEnumerableMinusRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrAsyncEnumerableMinusRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode convert(RelNode rel)
        {
            var minus = (Minus)rel;
            var traitSet = rel.getTraitSet().replace(ClrAsyncEnumerableConvention.Instance);

            return new ClrAsyncEnumerableMinus(rel.getCluster(), traitSet, convertList(minus.getInputs(), ClrAsyncEnumerableConvention.Instance), minus.all);
        }

    }

}
