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
    /// <para><b>A node has two bodies, one per kind of sequence.</b> Both name <c>ClrEnumerableDefaults</c>,
    /// which holds both operator sets: <see cref="Implement"/> takes its pulled operators, reached by the
    /// unsuffixed members of <c>ClrBuiltInMethod</c>, and <see cref="ImplementAsync"/> the awaiting ones,
    /// reached by the <c>Async</c>-suffixed members of the same table.
    /// They are two static bodies naming two static operator sets, not one body over a dispatch: the
    /// operator a node calls is decided where the node is written and read there.</para>
    ///
    /// <para><b>The two bodies are two call hierarchies, kept apart the whole way down.</b>
    /// <see cref="Implement"/> reaches its inputs through
    /// <see cref="ClrEnumerableRelImplementor.VisitChild"/>, which calls the input's
    /// <see cref="Implement"/>; <see cref="ImplementAsync"/> reaches them through
    /// <see cref="ClrEnumerableRelImplementor.VisitChildAsync"/>, which calls the input's
    /// <see cref="ImplementAsync"/>. So a body always sees inputs of its own kind and the implementor holds
    /// no mode at all. Whoever calls the root member chooses; nothing in the plan, and nothing in the
    /// planner, knows the difference. That is why there is one convention and one set of rules rather than
    /// two of each.</para>
    ///
    /// <para><b><see cref="Implement"/> is required and <see cref="ImplementAsync"/> is optional</b>, which is
    /// the shape .NET itself uses wherever a type does both: <c>DbCommand.ExecuteDbDataReader</c>,
    /// <c>DbDataReader.Read</c>, <c>DbConnection.Open</c> and <c>Stream.Read</c> are all abstract and their
    /// <c>Async</c> counterparts are virtual over them — measured against the 10.0 reference assemblies, not
    /// remembered. Two defaults calling each other would compile for a node that overrides neither and then
    /// recurse until the process dies, and a <c>StackOverflowException</c> cannot be caught.</para>
    ///
    /// <para><b>Each fork has its own result type, so the kind is checked rather than inferred.</b>
    /// <see cref="Implement"/> answers a <see cref="ClrEnumerableResult"/> and
    /// <see cref="ImplementAsync"/> a <see cref="ClrAsyncEnumerableResult"/>, and the factory for each
    /// refuses a sequence of the other kind by name. Crossing between them is
    /// <see cref="ClrEnumerableRelImplementor.Awaited"/> and
    /// <see cref="ClrEnumerableRelImplementor.Pulled"/>, written at the site that wants it. Going to
    /// asynchronous costs a state machine and no thread; going to synchronous blocks a thread per row,
    /// because an <see cref="System.Collections.Generic.IEnumerable{T}"/> has nowhere to suspend.</para>
    ///
    /// <para><b>That is what makes the default safe, and it is safe exactly when a body does not compose an
    /// input.</b> The awaiting hierarchy hands a body awaited inputs, so a node that inherits the default
    /// runs its pulled body there and composes an awaited input into a pulled operator, which
    /// <c>Expression.Call</c> refuses — measured, in both directions. The test is the child visit rather
    /// than the input count: a node with an input it never asks for as a sequence is safe with one body,
    /// which is why the interpreter has one, and every node whose body visits a child writes both. Where
    /// only the awaiting body exists at all — an adapter whose client is asynchronous — write
    /// <see cref="Implement"/> as the delegation and let it be the side that is read across.</para>
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
        /// <c>ClrEnumerableDefaults</c> through the unsuffixed members of <c>ClrBuiltInMethod</c>, and it
        /// reaches its inputs through <see cref="ClrEnumerableRelImplementor.VisitChild"/>, which always
        /// answers an <see cref="System.Collections.Generic.IEnumerable{T}"/>. Build the return value with
        /// <see cref="ClrEnumerableRelImplementor.Result"/>, which refuses anything that is not one.
        /// </remarks>
        ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref);

        /// <summary>
        /// Builds the plan for this node where the plan being built awaits its rows.
        /// </summary>
        /// <param name="implementor">The implementor of the plan being built. Reach the inputs through its
        /// <see cref="ClrEnumerableRelImplementor.VisitChildAsync"/>, never its
        /// <see cref="ClrEnumerableRelImplementor.VisitChild"/>: this body is the awaiting hierarchy and the
        /// pulled visit would hand it a pulled input and a pulled subtree beneath.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented.</param>
        /// <returns>The plan, the physical type of its rows, and their format.</returns>
        /// <remarks>
        /// The awaiting body: the same algorithm named against the <c>Async</c>-suffixed operators, through
        /// the <c>Async</c>-suffixed members of <c>ClrBuiltInMethod</c>, and built with
        /// <c>ClrBuiltInMethod.CallAsync</c> so that the trailing cancellation token an expression tree will
        /// not default is passed.
        ///
        /// <para>Optional, and by default <see cref="ClrEnumerableRelImplementor.Awaited"/> over
        /// <see cref="Implement"/>. That default holds only for a body that never visits a child: one that
        /// does would run the pulled visit, and its own pulled operators would then be handed inputs this
        /// hierarchy had already made awaited. Every node whose body visits a child writes this one.</para>
        /// </remarks>
        ClrAsyncEnumerableResult ImplementAsync(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref) => implementor.Awaited(Implement(implementor, pref));

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
