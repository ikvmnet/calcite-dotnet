using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The state of one aggregate call while a node of either CLR convention is being implemented.
    /// </summary>
    /// <remarks>
    /// <c>AggImpState</c> with a second implementor beside the one it already has, which is not a shape anyone
    /// would choose. Every value in <c>AggImpState.implementor</c> is the right one but for a call of type
    /// ANY, which needs <see cref="ClrAnyAggImplementors"/>'s.
    ///
    /// <para><b>1.42 leaves nothing to extend.</b> <c>RexImpTable</c>'s constructor is private, its
    /// <c>Builder</c> is a private static class, <c>INSTANCE</c> is the only instance there will ever be, and
    /// <c>AggImpState</c>'s only constructor calls <c>RexImpTable.INSTANCE.get(...)</c> by name. So a table of
    /// our own could neither be built nor consulted. The one seam left is which object a node reads the
    /// implementor back out of, and this is that.</para>
    ///
    /// <para><b>And the base field cannot simply be corrected.</b> It is <c>final</c> in Java, and IKVM emits
    /// a Java <c>public final</c> field as a get-only property — so it is read only from C# even here, in a
    /// subclass. Measured rather than assumed: assigning it is CS0200. Hence a second field, and hence every
    /// node reading <see cref="Implementor"/> rather than <c>implementor</c>. The base field is left visible
    /// rather than hidden, so that what <c>RexImpTable</c> answered can be read next to what is being used
    /// instead; for every call but an ANY one the two are the same object.</para>
    ///
    /// <para><b>1.43 adds the extension point, and this class is what to delete when we take it.</b>
    /// <c>RexImplementorTable</c> is a public interface with <c>get(SqlAggFunction, boolean)</c>, and
    /// <c>AggImpState</c> gains an <c>(int, AggregateCall, boolean, RexImplementorTable)</c> constructor —
    /// with the three argument one deprecated in its favour. Present at <c>main</c>, absent at both
    /// <c>calcite-1.41.0</c> and <c>calcite-1.42.0</c>, checked with <c>git cat-file -e</c>. Then the right
    /// shape is a <c>RexImplementorTable</c> that delegates to <c>RexImpTable.INSTANCE</c> and answers
    /// <see cref="ClrAnyAggImplementors"/> for an ANY call, handed to that constructor — at which point this
    /// type goes away and every site reads <c>agg.implementor</c> again.</para>
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
        public ClrAggImpState(int aggIdx, AggregateCall call, bool windowContext) :
            base(aggIdx, call, windowContext)
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
