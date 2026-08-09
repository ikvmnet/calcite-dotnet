using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Relational operator that reads the result of an <c>EnumerableConvention</c> sub-plan as a
    /// <see cref="ClrAsyncEnumerableConvention"/> one.
    /// </summary>
    /// <remarks>
    /// <see cref="EnumerableToClrEnumerableConverter"/> for the asynchronous convention, and the cheap
    /// direction. The rows are not touched — both conventions ask the same <c>JavaTypeFactory</c> what a
    /// field is — and the sequence that comes out never suspends, because a linq4j <c>Enumerable</c> is
    /// pulled. That costs a state machine and no thread.
    ///
    /// <para><b>What it buys is that the convention no longer has to plan a query whole or not at all.</b>
    /// Before this existed a query touching anything the convention had no node for — a table that is not
    /// an <see cref="Schema.IClrAsyncScannableTable"/>, a table function, a MATCH_RECOGNIZE, a recursive
    /// query's transient scan — could not be planned at all. Now the planner puts Calcite's implementation
    /// under a converter, exactly as it does for the synchronous convention, and the part of the query that
    /// can be asynchronous still is.</para>
    ///
    /// <para>The part under the converter is not, and nothing can make it so; that is a fact about the
    /// sub-plan rather than about this node.</para>
    /// </remarks>
    public class EnumerableToClrAsyncEnumerableConverter : ConverterImpl, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public EnumerableToClrAsyncEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new EnumerableToClrAsyncEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(ClrAsyncEnumerableConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            // the same map, so a value Calcite stashes reaches the DataContext this plan is bound with
            var enumerable = new EnumerableRelImplementor(implementor.RexBuilder, implementor.Map);

            // and the same correlation variables, because a sub-plan of Calcite's under a correlate of this
            // convention reads the outer row through them
            implementor.ReplayCorrelVariables(enumerable);

            var result = enumerable.visitChild(null, 0, (EnumerableRel)getInput(), pref.ToCalcite());

            // a physical type is a type factory, a row type and a format, and theirs answers all three
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, result.physType.getRowType(), result.physType.getFormat(), false);
            var rowType = physType.RowType;
            var source = implementor.Translator.TranslateBody(result.block, typeof(org.apache.calcite.linq4j.Enumerable));

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.FromJavaAsync.MakeGenericMethod(rowType), source));
        }

    }

}
