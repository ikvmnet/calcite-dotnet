using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// A relational expression of the <see cref="ClrDataCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="EnumerableRel"/>. Implement this to add a node to the convention:
    /// <see cref="Implement"/> builds the plan, and the trait methods have the defaults Calcite gives them,
    /// so a node overrides only what it does differently.
    ///
    /// <para><b>A node's expression is an <em>open</em>, not a sequence.</b> Where a node of
    /// <c>ClrEnumerableConvention</c> hands up an expression whose value is a lazy sequence, a node here
    /// hands up an expression whose value is an opened cursor: evaluating it is the acquisition, so the
    /// plan's tree of calls <em>is</em> the cascade linq4j runs at <c>enumerator()</c>. A sort drains its
    /// input where its open is evaluated; a leaf executes its statement there. An operator that defers
    /// an acquisition — a concat acquires each source at its turn — takes that source as a delegate,
    /// built with <see cref="ClrDataCursorRelImplementor.Opener"/>, and the deferral reads at the site.</para>
    ///
    /// <para><b>A node has two bodies, one per way of opening.</b> <see cref="Implement"/> composes the
    /// opens that acquire synchronously, named through the unsuffixed members of
    /// <c>ClrDataCursorBuiltInMethod</c>; <see cref="ImplementAsync"/> composes the opens that await their
    /// acquisition, named through the <c>Async</c>-suffixed members of the same table and built with
    /// <c>ClrDataCursorBuiltInMethod.CallAsync</c>, which appends the token the awaiting root takes. Both
    /// bodies produce the <em>same cursor</em>: what differs is whether a drain or a statement is waited for
    /// or awaited on the way to it. The cursor has <c>Read</c> and <c>ReadAsync</c> whichever way it was
    /// opened, and <see cref="ClrDataCursorRelImplementor.ImplementRoot"/> calls both bodies and puts the
    /// two opens on one <see cref="ClrDataCursorFactory"/>.</para>
    ///
    /// <para><b>The two bodies are two call hierarchies for what is acquired at open</b>, kept apart the
    /// way <c>ClrEnumerableConvention</c> keeps its two: <see cref="Implement"/> reaches an input through
    /// <see cref="ClrDataCursorRelImplementor.VisitChild"/> and <see cref="ImplementAsync"/> through
    /// <see cref="ClrDataCursorRelImplementor.VisitChildAsync"/>, so an eager input is always of the body's
    /// own kind and nothing consults a mode. <b>A deferred input is the exception, and it is visited both
    /// ways from both bodies.</b> A source acquired later, inside <c>Read</c> or <c>ReadAsync</c>, has to be
    /// acquirable either way, because which member the consumer calls at that moment is not known when
    /// the plan is built. So a node with a deferred source hands its cursor both openers, and the cursor
    /// calls the one matching the advance it is in. That is not a mode: it is the consumer's per-advance
    /// choice reaching an acquisition that happens per advance.</para>
    ///
    /// <para><b><see cref="Implement"/> is required and <see cref="ImplementAsync"/> is optional</b>, which
    /// is .NET's own shape for the pair and the reason the enumerable convention gives: two defaults calling
    /// each other would compile for a node that overrides neither and then recurse until the process dies.
    /// The default is safe exactly when a body does not compose an eager input: it would otherwise run the
    /// synchronous visit and compose a synchronously opened input into an awaiting operator, which
    /// <c>Expression.Call</c> refuses. Every node whose body visits an eager child writes both.</para>
    ///
    /// <para><b>Each fork has its own result type.</b> <see cref="Implement"/> answers a
    /// <see cref="ClrDataCursorResult"/>, whose expression is a <c>ClrDataCursor&lt;TRow&gt;</c>, and
    /// <see cref="ImplementAsync"/> a <see cref="ClrDataCursorAsyncResult"/>, whose expression is a
    /// <c>ValueTask&lt;ClrDataCursor&lt;TRow&gt;&gt;</c>; the factory for each refuses the other kind by
    /// name. Crossing is <see cref="ClrDataCursorRelImplementor.Awaited"/>, which costs nothing, and
    /// <see cref="ClrDataCursorRelImplementor.Pulled"/>, which blocks a thread for the length of the
    /// acquisition and is written where that can be read.</para>
    /// </remarks>
    public interface ClrDataCursorRel : PhysicalNode
    {

        /// <summary>
        /// Builds the plan for this node, as an open that acquires synchronously.
        /// </summary>
        /// <param name="implementor">Reach the inputs through
        /// <see cref="ClrDataCursorRelImplementor.VisitChild"/>, and build the return value with
        /// <see cref="ClrDataCursorRelImplementor.Result"/>.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented. A node may return
        /// another format; the result says which it chose.</param>
        /// <returns>The open, the physical type of the rows it yields, and their format.</returns>
        ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref);

        /// <summary>
        /// Builds the plan for this node, as an open that awaits its acquisition.
        /// </summary>
        /// <param name="implementor">Reach the inputs through
        /// <see cref="ClrDataCursorRelImplementor.VisitChildAsync"/>, never its
        /// <see cref="ClrDataCursorRelImplementor.VisitChild"/>: this body is the awaiting hierarchy, and
        /// the other visit would hand it an input that was already acquired by blocking.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented.</param>
        /// <returns>The open, the physical type of the rows it yields, and their format.</returns>
        /// <remarks>
        /// Optional, and by default <see cref="ClrDataCursorRelImplementor.Awaited"/> over
        /// <see cref="Implement"/>. That default holds only for a body that never visits an eager child;
        /// every node whose body does writes this one.
        /// </remarks>
        ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref) => implementor.Awaited(Implement(implementor, pref));

        /// <inheritdoc cref="PhysicalNode.passThroughTraits" />
        Pair? PhysicalNode.passThroughTraits(RelTraitSet required) => null;

        /// <inheritdoc cref="PhysicalNode.deriveTraits" />
        Pair? PhysicalNode.deriveTraits(RelTraitSet childTraits, int childId) => null;

        /// <inheritdoc cref="PhysicalNode.getDeriveMode" />
        DeriveMode PhysicalNode.getDeriveMode() => DeriveMode.LEFT_FIRST;

        /// <inheritdoc cref="PhysicalNode.passThrough" />
        RelNode? PhysicalNode.passThrough(RelTraitSet required) => PhysicalNode.__DefaultMethods.passThrough(this, required);

        /// <inheritdoc cref="PhysicalNode.derive(RelTraitSet, int)" />
        RelNode? PhysicalNode.derive(RelTraitSet childTraits, int childId) => PhysicalNode.__DefaultMethods.derive(this, childTraits, childId);

        /// <inheritdoc cref="PhysicalNode.derive(java.util.List)" />
        java.util.List PhysicalNode.derive(java.util.List inputTraits) => PhysicalNode.__DefaultMethods.derive(this, inputTraits);

    }

}
