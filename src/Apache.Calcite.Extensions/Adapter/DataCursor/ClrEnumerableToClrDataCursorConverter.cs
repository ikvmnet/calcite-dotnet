using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrEnumerableConvention"/> sub-plan as a
    /// <see cref="ClrDataCursorConvention"/> one.
    /// </summary>
    /// <remarks>
    /// The converter that gives a cursor-rooted plan every node the sequence convention has. Both sides are
    /// <see cref="System.Linq.Expressions"/>, so the sub-plan is spliced into the tree being built rather
    /// than stashed and called back into: the sequence convention's implementor runs over the input with
    /// this plan's own <c>root</c> parameter, and a cursor is opened over the sequence it yields. Opening
    /// is where the sequence's <c>GetEnumerator</c> runs, which is where its acquisition happens, so the
    /// timing is the sequence's own.
    ///
    /// <para>Each body reaches the sequence convention's body of the same kind: the synchronous open
    /// enumerates an <see cref="System.Collections.Generic.IEnumerable{T}"/>, the awaiting open an
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>, and neither reads across. An awaited
    /// sequence still takes its token once, at <c>GetAsyncEnumerator</c>, and that is the open's; an
    /// advance's own token is checked before each advance and can reach no further into a sequence.</para>
    /// </remarks>
    public class ClrEnumerableToClrDataCursorConverter : ConverterImpl, ClrDataCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public ClrEnumerableToClrDataCursorConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableToClrDataCursorConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(ClrDataCursorConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var enumerable = Enumerable(implementor);
            var result = enumerable.VisitChild(null, 0, (ClrEnumerableRel)getInput(), pref);

            return implementor.Result(result.PhysType,
                Expression.Call(null, ClrDataCursorBuiltInMethod.AsCursor.MakeGenericMethod(result.PhysType.RowType), result.Expression));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var enumerable = Enumerable(implementor);
            var result = enumerable.VisitChildAsync(null, 0, (ClrEnumerableRel)getInput(), pref);

            return implementor.ResultAsync(result.PhysType,
                ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.AsCursorAsync.MakeGenericMethod(result.PhysType.RowType), result.Expression));
        }

        /// <summary>
        /// Builds the sequence convention's implementor for the sub-plan, over this plan's own parameter,
        /// map and translator, with the correlation variables in scope carried across.
        /// </summary>
        /// <remarks>
        /// The translator is this plan's because a variable is keyed by the linq4j object it was declared
        /// from: a correlate of this convention declares the fields of its outer row into a block it
        /// translates itself, and a sub-plan under it that reads them has to name the same variables.
        /// </remarks>
        static ClrEnumerableRelImplementor Enumerable(ClrDataCursorRelImplementor implementor)
        {
            var enumerable = new ClrEnumerableRelImplementor(implementor.RexBuilder, implementor.Map, implementor.Root, implementor.Translator);
            implementor.ReplayCorrelVariables(enumerable);

            return enumerable;
        }

    }

}
