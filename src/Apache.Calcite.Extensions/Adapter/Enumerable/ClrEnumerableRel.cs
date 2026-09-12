using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// A relational expression of the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="EnumerableRel"/>. Implement this to add a node to the convention:
    /// <see cref="Implement"/> builds the plan, and the trait methods have the defaults Calcite gives them,
    /// so a node overrides only what it does differently.
    ///
    /// <para><see cref="Implement"/> returns an expression, where Calcite returns a block of generated Java.
    /// A node composes its inputs' expressions into its own rather than appending statements to a method;
    /// where a node needs statements it uses <c>Expression.Block</c>, whose value is its last expression.
    /// Nothing here runs a query — the result is a plan, which
    /// <see cref="ClrEnumerableRelImplementor.ImplementRoot"/> turns into a lambda to compile.</para>
    ///
    /// <para><b>A node has two bodies, one per kind of sequence.</b> <see cref="Implement"/> is written
    /// against the pulled operators — <c>ClrEnumerableDefaults</c>, reached by the unsuffixed members of
    /// <c>ClrBuiltInMethod</c> — and <see cref="ImplementAsync"/> against the awaiting ones —
    /// <c>ClrAsyncEnumerableDefaults</c>, reached by the <c>Async</c>-suffixed members of the same table.
    /// They are two static bodies naming two static operator sets, not one body over a dispatch: the
    /// operator a node calls is decided where the node is written and read there.
    /// <see cref="ClrEnumerableRelImplementor"/> is constructed for one kind and calls only that body, so
    /// whoever builds the implementor chooses; nothing in the plan, and nothing in the planner, knows the
    /// difference. That is why there is one convention and one set of rules rather than two of each.</para>
    ///
    /// <para><b><see cref="Implement"/> is required and <see cref="ImplementAsync"/> is optional</b>, which is
    /// the shape .NET itself uses wherever a type does both: <c>DbCommand.ExecuteDbDataReader</c>,
    /// <c>DbDataReader.Read</c>, <c>DbConnection.Open</c> and <c>Stream.Read</c> are all abstract and their
    /// <c>Async</c> counterparts are virtual over them — measured against the 10.0 reference assemblies, not
    /// remembered. Two defaults calling each other would compile for a node that overrides neither and then
    /// recurse until the process dies, and a <c>StackOverflowException</c> cannot be caught.</para>
    ///
    /// <para><b>What a node hands up is an expression yielding one kind of sequence or the other, and
    /// converting between them is wrapping that expression.</b> The implementor does it: what a node hands
    /// up that is not the kind being built is wrapped, once. Going to asynchronous costs a state machine and
    /// no thread; going to synchronous blocks a thread per row, because an
    /// <see cref="System.Collections.Generic.IEnumerable{T}"/> has nowhere to suspend.</para>
    ///
    /// <para><b>That wrapping is what makes the default safe, and it is only safe for a leaf.</b>
    /// <see cref="ClrEnumerableRelImplementor.VisitChild"/> answers in the kind the plan is being built
    /// with, so a node with inputs that inherits the default composes an awaited input into a pulled
    /// operator, and <c>Expression.Call</c> refuses it — measured, in both directions. A leaf has no input
    /// to be handed the wrong kind, so it may write one body and let the implementor read its rows across;
    /// every node here that has an input writes both. Where only the awaiting body exists at all — an
    /// adapter whose client is asynchronous — write <see cref="Implement"/> as the delegation and let it be
    /// the side that is read across.</para>
    /// </remarks>
    public interface ClrEnumerableRel : PhysicalNode
    {

        /// <summary>
        /// Builds the plan for this node.
        /// </summary>
        /// <param name="implementor">Reach the inputs through
        /// <see cref="ClrEnumerableRelImplementor.VisitChild"/>, and build the return value with
        /// <see cref="ClrEnumerableRelImplementor.Result"/>.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented. A node may return
        /// another format; the result says which it chose.</param>
        /// <returns>The plan, the physical type of its rows, and their format.</returns>
        /// <remarks>
        /// The one member a node must write, and the body of the pulled pair: it names
        /// <c>ClrEnumerableDefaults</c> through the unsuffixed members of <c>ClrBuiltInMethod</c> and its
        /// inputs arrive as <see cref="System.Collections.Generic.IEnumerable{T}"/>. It may hand up either
        /// kind of sequence — what does not match is read across, once, and the node is told nothing about
        /// it.
        /// </remarks>
        ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref);

        /// <summary>
        /// Builds the plan for this node where the plan being built awaits its rows.
        /// </summary>
        /// <param name="implementor">The implementor of the plan being built, whose
        /// <see cref="ClrEnumerableRelImplementor.VisitChild"/> answers in
        /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented.</param>
        /// <returns>The plan, the physical type of its rows, and their format.</returns>
        /// <remarks>
        /// The awaiting body: the same algorithm named against <c>ClrAsyncEnumerableDefaults</c>, through
        /// the <c>Async</c>-suffixed members of <c>ClrBuiltInMethod</c>, and built with
        /// <c>ClrBuiltInMethod.CallAsync</c> so that the trailing cancellation token an expression tree will
        /// not default is passed.
        ///
        /// <para>Optional, and by default <see cref="Implement"/>, whose sequence is then read across. That
        /// default is for a leaf: a node with inputs that takes it composes an awaited input into a pulled
        /// operator and <c>Expression.Call</c> refuses it. Every node here that has an input writes this
        /// body.</para>
        /// </remarks>
        ClrEnumerableResult ImplementAsync(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref) => Implement(implementor, pref);

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
