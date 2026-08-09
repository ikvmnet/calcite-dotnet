using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrAsyncEnumerableConvention"/> sub-plan as
    /// a <see cref="ClrEnumerableConvention"/> one.
    /// </summary>
    /// <remarks>
    /// The expensive direction, and the one the asynchronous convention was written to make unnecessary:
    /// <see cref="ClrSequences.ToEnumerable{TSource}"/> blocks the calling thread once per row, because an
    /// <see cref="IEnumerable{T}"/> has nowhere to suspend. Nothing about the node hides that — a plan
    /// holding one is a plan that blocks, and the cost model does not know it.
    ///
    /// <para><b>It is nonetheless the cheapest converter in the tree to build.</b> Both sides are
    /// <see cref="System.Linq.Expressions"/> and both sides share <c>ClrPhysType</c>, so there is no
    /// translation, no stash, no callback and no physical type to rebuild: the sub-plan's expression is
    /// spliced into the tree being built and wrapped in one call. That is why the sub-implementor is given
    /// the enclosing plan's <c>Root</c> — the two trees are one tree.</para>
    ///
    /// <para>What it buys is that a query whose leaves are asynchronous can still be read by a caller that
    /// wants an <see cref="IEnumerable{T}"/> — an ADO.NET reader driven synchronously, most of all — instead
    /// of failing to plan.</para>
    /// </remarks>
    public class ClrAsyncEnumerableToClrEnumerableConverter : ConverterImpl, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public ClrAsyncEnumerableToClrEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrAsyncEnumerableToClrEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        /// <remarks>
        /// What a converter costs is what the convention it produces costs against a typical one, which is
        /// what every other converter here charges. The blocking is not in the cost, and cannot honestly be
        /// put there: <c>VolcanoCost.isLt</c> compares the row count and nothing else — cpu and io are dead
        /// code behind <c>if (true)</c> — so a multiplier is the only lever, and one large enough to express
        /// "this blocks a thread" would also stop the planner choosing the converter where it is the only way
        /// to plan the query at all.
        /// </remarks>
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(ClrEnumerableConvention.CostMultiplier);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            // the same map, so a value the sub-plan stashes reaches the DataContext this plan is bound with,
            // and the same root, so the spliced expression reads the parameter this plan's lambda declares
            var async = new ClrAsyncEnumerableRelImplementor(implementor.RexBuilder, implementor.Map, implementor.Root);

            // and the same correlation variables, because an asynchronous sub-plan under a correlate of this
            // convention reads the outer row through them
            implementor.ReplayCorrelVariables(async);

            var result = async.VisitChild(null, 0, (ClrAsyncEnumerableRel)getInput(), pref);

            // the physical type crosses unchanged. It is not merely equivalent -- ClrPhysType is one class,
            // shared by both conventions, because a row is the same thing in each
            var physType = result.PhysType;

            return implementor.Result(physType,
                Expression.Call(null, ToEnumerableMethod.MakeGenericMethod(physType.RowType), result.Expression));
        }

        /// <summary>
        /// <see cref="ClrSequences.ToEnumerable{TSource}"/>, which blocks on each row of the sub-plan.
        /// </summary>
        static readonly MethodInfo ToEnumerableMethod = typeof(ClrSequences).GetMethod(nameof(ClrSequences.ToEnumerable), BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"'{nameof(ClrSequences.ToEnumerable)}' is missing from {nameof(ClrSequences)}.");

    }

}
