using System;
using System.Collections.Generic;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrEnumerableConvention"/> sub-plan as a
    /// <see cref="ClrAsyncEnumerableConvention"/> one.
    /// </summary>
    /// <remarks>
    /// The cheap direction, and the exact counterpart of
    /// <see cref="EnumerableToClrAsyncEnumerableConverter"/> over a sub-plan that is already
    /// <see cref="System.Linq.Expressions"/>: <see cref="ClrSequences.ToAsyncEnumerable{TSource}"/> costs a
    /// state machine and no thread, because nothing under it ever suspends.
    ///
    /// <para><b>The sub-plan is not asynchronous and this does not make it so.</b> A sequence that always
    /// completes synchronously is not the sync-over-async the convention refuses — that is a caller blocked
    /// waiting, which is <see cref="ClrAsyncEnumerableToClrEnumerableConverter"/>. A plan reading a
    /// synchronous sub-plan this way is simply not asynchronous over that part of itself, and the part above
    /// it still is.</para>
    ///
    /// <para>Nothing is translated, stashed or rebuilt. Both sides are
    /// <see cref="System.Linq.Expressions"/> and both share <c>ClrPhysType</c>, so the sub-plan's expression
    /// is spliced into the tree being built — which is why the sub-implementor is given the enclosing plan's
    /// <c>Root</c>.</para>
    /// </remarks>
    public class ClrEnumerableToClrAsyncEnumerableConverter : ConverterImpl, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public ClrEnumerableToClrAsyncEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableToClrAsyncEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
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
            // the same map, so a value the sub-plan stashes reaches the DataContext this plan is bound with,
            // and the same root, so the spliced expression reads the parameter this plan's lambda declares
            var clr = new ClrEnumerableRelImplementor(implementor.RexBuilder, implementor.Map, implementor.Root);

            // and the same correlation variables, because a synchronous sub-plan under a correlate of this
            // convention reads the outer row through them
            implementor.ReplayCorrelVariables(clr);

            var result = clr.VisitChild(null, 0, (ClrEnumerableRel)getInput(), pref);

            // the physical type crosses unchanged. It is not merely equivalent -- ClrPhysType is one class,
            // shared by both conventions, because a row is the same thing in each
            var physType = result.PhysType;

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ToAsyncEnumerableMethod.MakeGenericMethod(physType.RowType), result.Expression));
        }

        /// <summary>
        /// <see cref="ClrSequences.ToAsyncEnumerable{TSource}"/>, which yields the sub-plan's rows without
        /// suspending.
        /// </summary>
        /// <remarks>
        /// Called through <see cref="ClrAsyncBuiltInMethod.Call"/> rather than
        /// <c>Expression.Call</c>, because it ends in a <see cref="System.Threading.CancellationToken"/> like
        /// every other operator of this convention, and that is what appends the <c>default</c> the
        /// <c>[EnumeratorCancellation]</c> attribute reads.
        /// </remarks>
        static readonly MethodInfo ToAsyncEnumerableMethod = typeof(ClrSequences).GetMethod(nameof(ClrSequences.ToAsyncEnumerable), BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"'{nameof(ClrSequences.ToAsyncEnumerable)}' is missing from {nameof(ClrSequences)}.");

    }

}
