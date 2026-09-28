using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The state of one aggregate call while a node of the CLR convention is being implemented.
    /// </summary>
    /// <remarks>
    /// Extends <c>AggImpState</c> with <see cref="Implementor"/>, which is the base's <c>implementor</c> except
    /// for a call of type ANY, where it is the one <see cref="ClrAnyAggImplementors"/> supplies. The choice
    /// depends on the call's result type, which a <c>RexImplementorTable</c> cannot see because every lookup on
    /// it is keyed by operator alone.
    ///
    /// <para>The base field is Java <c>final</c>, which IKVM exposes as a get-only property, so it cannot be
    /// replaced from a subclass. Nodes of this convention therefore read <see cref="Implementor"/> and never
    /// <c>implementor</c>, and every <c>AggImpState</c> they create is one of these.</para>
    /// </remarks>
    sealed class ClrAggImpState : AggImpState
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="aggIdx">The index of the call among the node's aggregate calls.</param>
        /// <param name="call">The aggregate call.</param>
        /// <param name="windowContext">Whether the call is implemented as a window aggregate.</param>
        /// <param name="implementorTable">
        /// The table the base resolves its implementor from; the cluster's, as <c>EnumerableAggregate</c> passes
        /// <c>RexImplementorTables.of(getCluster())</c>, so implementors a caller registered are found.
        /// </param>
        public ClrAggImpState(int aggIdx, AggregateCall call, bool windowContext, RexImplementorTable implementorTable) :
            base(aggIdx, call, windowContext, implementorTable)
        {
            // one substitution also serves a window: a window context falls back to the regular implementor for
            // any function without a window implementor of its own, and none of the ANY functions has one
            Implementor = ClrAnyAggImplementors.For(call) ?? implementor;
        }

        /// <summary>
        /// Gets the implementor this call is written with.
        /// </summary>
        /// <remarks>
        /// Fixed at construction because <c>StrictAggImplementor</c> keeps state across phases: it sizes its state
        /// in <c>getStateType</c> and reads that back in <c>implementAdd</c> and <c>implementResult</c>, so every
        /// phase must see the same instance.
        /// </remarks>
        public AggImplementor Implementor { get; }

    }

}
