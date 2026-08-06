using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Linq.Convert
{

    /// <summary>
    /// Relational operator that reads the result of an <c>EnumerableConvention</c> sub-plan as a
    /// <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    /// <remarks>
    /// The rows are not touched. Both conventions ask the same <c>JavaTypeFactory</c> what a field is, so a
    /// row that crossed the boundary is the row that arrived; only the sequence around it changes.
    ///
    /// <para>Calcite's own implementor runs the sub-plan, producing the linq4j block it would have handed to
    /// Janino. That block is translated rather than compiled, which is the whole of what this convention does
    /// differently.</para>
    /// </remarks>
    public class EnumerableToClrEnumerableConverter : ConverterImpl, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public EnumerableToClrEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new EnumerableToClrEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost == null ? null! : cost.multiplyBy(ClrEnumerableConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            // the same map, so a value Calcite stashes reaches the DataContext this plan is bound with
            var enumerable = new EnumerableRelImplementor(implementor.RexBuilder, implementor.Map);
            var result = enumerable.visitChild(null, 0, (EnumerableRel)getInput(), pref.ToCalcite());

            var rowType = ClrEnumerableRelImplementor.RowType(result.physType);
            var source = implementor.Translator.TranslateBody(result.block, typeof(Enumerable));

            return implementor.Result(result.physType,
                Expression.Call(null, ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType), source));
        }

    }

}
