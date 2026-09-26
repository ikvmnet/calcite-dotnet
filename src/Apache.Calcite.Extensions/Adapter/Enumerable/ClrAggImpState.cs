using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The state of one aggregate call while a node of the CLR convention is being implemented.
    /// </summary>
    /// <remarks>
    /// <c>AggImpState</c> with a second implementor beside the one it already has, which is not a shape anyone
    /// would choose. Every value in <c>AggImpState.implementor</c> is the right one but for a call of type
    /// ANY, which needs <see cref="ClrAnyAggImplementors"/>'s.
    ///
    /// <para><b>A table of our own cannot express it, on 1.43 either.</b> <c>RexImplementorTable</c> is a
    /// public interface now and <c>AggImpState</c> takes one, which is the extension point 1.42 had none of
    /// — and it is keyed on the wrong thing. Every one of its four <c>get</c> overloads takes the
    /// <em>operator</em>; none takes the <c>AggregateCall</c>. <see cref="ClrAnyAggImplementors.For"/>
    /// decides on <c>call.getType().getSqlTypeName() == ANY</c>, the call's result type, which a table asked
    /// only about <c>SqlAggFunction</c> cannot see. <c>MIN</c> over an <c>INTEGER</c> and <c>MIN</c> over an
    /// <c>ANY</c> are the same aggregation and want different implementors. So the seam stays where it is:
    /// which object a node reads the implementor back out of.</para>
    ///
    /// <para><b>And the base field cannot simply be corrected.</b> It is <c>final</c> in Java, and IKVM emits
    /// a Java <c>public final</c> field as a get-only property — so it is read only from C# even here, in a
    /// subclass. Measured rather than assumed: assigning it is CS0200. Hence a second field, and hence every
    /// node reading <see cref="Implementor"/> rather than <c>implementor</c>. The base field is left visible
    /// rather than hidden, so that what <c>RexImpTable</c> answered can be read next to what is being used
    /// instead; for every call but an ANY one the two are the same object.</para>
    ///
    /// <para><b>What 1.43 did change is where the base looks.</b> <c>AggImpState</c>'s three argument
    /// constructor is deprecated in favour of one taking a <c>RexImplementorTable</c>, and
    /// <c>EnumerableAggregate</c> passes <c>RexImplementorTables.of(getCluster())</c> — the cluster's table,
    /// so that a caller which registered implementors of its own is asked about them. This takes one and
    /// hands it on for exactly that reason; going through the deprecated constructor would consult
    /// <c>RexImpTable.INSTANCE</c> and ignore the caller.</para>
    ///
    /// <para>Calcite never sees one of these: nothing upstream builds this project's nodes or reads their
    /// state, so every <c>AggImpState</c> in either convention is one of these and the cast at each read
    /// site is total.</para>
    /// </remarks>
    sealed class ClrAggImpState : AggImpState
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="aggIdx"></param>
        /// <param name="call"></param>
        /// <param name="windowContext"></param>
        /// <param name="implementorTable">The table the base resolves from, which is the cluster's.</param>
        public ClrAggImpState(int aggIdx, AggregateCall call, bool windowContext, RexImplementorTable implementorTable) :
            base(aggIdx, call, windowContext, implementorTable)
        {
            // one substitution serves a window as well, because a window context falls through to the regular
            // implementor for any function without one of its own, and none of the ANY functions has one
            Implementor = ClrAnyAggImplementors.For(call) ?? implementor;
        }

        /// <summary>
        /// Gets the implementor this call is to be written with.
        /// </summary>
        /// <remarks>
        /// Settled once rather than worked out per read, which is the other half of why this is a field and
        /// not a method. <c>StrictAggImplementor</c> keeps state across the phases it is called in: it works
        /// out its state size and whether it has to track an empty set in <c>getStateType</c>, and reads both
        /// back in <c>implementAdd</c> and <c>implementResult</c>. An implementor built afresh at each phase
        /// would answer from a state that had never been sized.
        /// </remarks>
        public AggImplementor Implementor { get; }

    }

}
