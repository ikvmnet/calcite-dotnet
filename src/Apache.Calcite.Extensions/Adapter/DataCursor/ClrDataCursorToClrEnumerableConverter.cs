using System.Linq.Expressions;
using System.Threading;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrDataCursorConvention"/> sub-plan as a
    /// <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    /// <remarks>
    /// The other direction of <see cref="ClrEnumerableToClrDataCursorConverter"/>, and spliced the same way.
    /// The sub-plan's open is deferred into the sequence — a sequence acquires at <c>GetEnumerator</c>, and
    /// an open evaluated where the sequence is built would have run the sub-plan while the plan was still
    /// being assembled — so what the sequence holds is the opener, and it opens once per enumeration.
    ///
    /// <para>The awaiting side carries the one divergence the CLR imposes: <c>GetAsyncEnumerator</c> cannot
    /// await, so an open that awaits runs inside the first <c>MoveNextAsync</c>. That is the same exception
    /// the sequence convention states at every awaited drain.</para>
    /// </remarks>
    public class ClrDataCursorToClrEnumerableConverter : ConverterImpl, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public ClrDataCursorToClrEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrDataCursorToClrEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(ClrEnumerableConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var cursor = Cursor(implementor);
            var result = cursor.VisitChild(null, 0, (ClrDataCursorRel)getInput(), pref);

            return implementor.Result(result.PhysType,
                Expression.Call(null, ClrDataCursorBuiltInMethod.AsEnumerable.MakeGenericMethod(result.PhysType.RowType), cursor.Opener(result)));
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult ImplementAsync(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var cursor = Cursor(implementor);
            var result = cursor.VisitChildAsync(null, 0, (ClrDataCursorRel)getInput(), pref);

            return implementor.ResultAsync(result.PhysType,
                ClrBuiltInMethod.CallAsync(ClrDataCursorBuiltInMethod.AsAsyncEnumerable.MakeGenericMethod(result.PhysType.RowType), cursor.OpenerAsync(result)));
        }

        /// <summary>
        /// Builds the cursor convention's implementor for the sub-plan, over this plan's own parameter,
        /// map and translator, with the correlation variables in scope carried across.
        /// </summary>
        /// <remarks>
        /// The translator is shared and not merely the variables' names: a correlate of the enclosing plan
        /// declares the field read for a correlation variable in its own block, and the sub-plan's reference
        /// to it resolves to that declaration only through the translator that made it.
        ///
        /// <para>The token parameter is fresh, because the sequence convention's plan declares none: it is
        /// declared only by the deferred opener the awaiting body builds, which is where the sub-plan's
        /// awaiting opens read it.</para>
        /// </remarks>
        static ClrDataCursorRelImplementor Cursor(ClrEnumerableRelImplementor implementor)
        {
            var cursor = new ClrDataCursorRelImplementor(implementor.RexBuilder, implementor.Map, implementor.Root, Expression.Parameter(typeof(CancellationToken), "cancellationToken"), implementor.Translator);
            implementor.ReplayCorrelVariables(cursor);

            return cursor;
        }

    }

}
