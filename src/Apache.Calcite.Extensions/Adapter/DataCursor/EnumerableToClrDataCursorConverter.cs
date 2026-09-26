using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Relational operator that reads the result of an <c>EnumerableConvention</c> sub-plan as a
    /// <see cref="ClrDataCursorConvention"/> one.
    /// </summary>
    /// <remarks>
    /// <see cref="EnumerableToClrEnumerableConverter"/> with the last hop changed: Calcite's own implementor
    /// runs the sub-plan, the linq4j block it would have handed to Janino is translated rather than compiled,
    /// and a cursor is opened over the <c>Enumerable</c> it yields. The rows are not touched, and the open
    /// is where the sub-plan's <c>enumerator()</c> runs.
    /// </remarks>
    public class EnumerableToClrDataCursorConverter : ConverterImpl, ClrDataCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public EnumerableToClrDataCursorConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new EnumerableToClrDataCursorConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            // dearer than the crossing from the sequence convention, so that a node this convention lacks
            // is the sequence convention's rather than Calcite's -- see the multiplier
            return cost?.multiplyBy(ClrDataCursorConvention.JavaCrossingCostMultiplier);
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var (physType, source) = Translate(implementor, pref);

            return implementor.Result(physType,
                Expression.Call(null, ClrDataCursorBuiltInMethod.FromJava.MakeGenericMethod(physType.RowType), source));
        }

        /// <inheritdoc />
        /// <remarks>
        /// Nothing here awaits, and cannot: a linq4j <c>Enumerator</c> is pulled. The open completes at once
        /// and the cursor's <c>ReadAsync</c> completes synchronously, which is the honest shape of a plan
        /// that is not asynchronous over this part of itself.
        /// </remarks>
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var (physType, source) = Translate(implementor, pref);

            return implementor.ResultAsync(physType,
                ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.FromJavaAsync.MakeGenericMethod(physType.RowType), source));
        }

        /// <summary>
        /// Runs Calcite's implementor over the sub-plan and translates the block it produces.
        /// </summary>
        (ClrPhysType PhysType, Expression Source) Translate(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            // the same map, so a value Calcite stashes reaches the DataContext this plan is bound with
            var enumerable = new EnumerableRelImplementor(implementor.RexBuilder, implementor.Map);

            // and the same correlation variables, because a sub-plan of Calcite's under a correlate of this
            // convention reads the outer row through them
            implementor.ReplayCorrelVariables(enumerable);

            var result = enumerable.visitChild(null, 0, (EnumerableRel)getInput(), pref.ToCalcite());

            // a physical type is a type factory, a row type and a format, and theirs answers all three
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, result.physType.getRowType(), result.physType.getFormat(), false);
            var source = implementor.Translator.TranslateBody(result.block, typeof(org.apache.calcite.linq4j.Enumerable));

            return (physType, source);
        }

    }

}
