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
    /// <para><b>The sequence a plan runs as is the implementor's, not the node's.</b> A node builds its call
    /// through <see cref="ClrEnumerableRelImplementor.Call"/> over
    /// <see cref="ClrEnumerableRelImplementor.Methods"/>, and the same <see cref="Implement"/> then yields an
    /// <see cref="System.Collections.Generic.IEnumerable{T}"/> or an
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> according to the implementor it was given.
    /// Whoever constructs the implementor chooses; nothing in the plan, and nothing in the planner, knows the
    /// difference. That is why there is one convention and one set of rules rather than two of each.</para>
    ///
    /// <para><b><see cref="Implement"/> is required and <see cref="ImplementAsync"/> is optional</b>, which is
    /// the shape .NET itself uses wherever a type does both: <c>DbCommand.ExecuteDbDataReader</c>,
    /// <c>DbDataReader.Read</c>, <c>DbConnection.Open</c> and <c>Stream.Read</c> are all abstract and their
    /// <c>Async</c> counterparts are virtual over them — measured against the 10.0 reference assemblies, not
    /// remembered. Two defaults calling each other would compile for a node that overrides neither and then
    /// recurse until the process dies, and a <c>StackOverflowException</c> cannot be caught.</para>
    ///
    /// <para><b>Whatever a node hands up is read across if it is not the kind being built.</b> A node whose
    /// body serves both — every node of this convention, because they build through
    /// <see cref="ClrEnumerableRelImplementor.Call"/> — writes <see cref="Implement"/> alone and nothing is
    /// wrapped: forwarded through the default, the body is handed the awaiting implementor and builds the
    /// awaiting operators. A <em>leaf</em> that can only build one kind also writes that one member alone and
    /// hands up its own kind, which the implementor reads across.</para>
    ///
    /// <para><b>A node with inputs that can only build one kind needs the other member and one line of it</b>,
    /// and the default is not that line. Measured, in both directions: the default hands the node the
    /// implementor the plan is being built with, so <see cref="ClrEnumerableRelImplementor.VisitChild"/>
    /// returns children of <em>that</em> kind, and a body naming the other operator set is refused by
    /// <c>Expression.Call</c> with an <see cref="System.ArgumentException"/> about a parameter. What such a
    /// node writes instead is <c>ImplementAsync(implementor, pref) =&gt;
    /// implementor.Synchronously(this, pref)</c>, or
    /// <c>Implement(implementor, pref) =&gt; implementor.Asynchronously(this, pref)</c> for an adapter with
    /// no blocking client. Its whole subtree is then implemented in that node's kind and the crossing is at
    /// the node — a state machine going one way, a blocked thread per row going the other.</para>
    /// </remarks>
    public interface ClrEnumerableRel : PhysicalNode
    {

        /// <summary>
        /// Builds the plan for this node.
        /// </summary>
        /// <param name="implementor">Reach the inputs through
        /// <see cref="ClrEnumerableRelImplementor.VisitChild"/>, and build the return value with
        /// <see cref="ClrEnumerableRelImplementor.Result"/>. Its
        /// <see cref="ClrEnumerableRelImplementor.Methods"/> and
        /// <see cref="ClrEnumerableRelImplementor.Call"/> are what make one body serve both kinds of
        /// sequence.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented. A node may return
        /// another format; the result says which it chose.</param>
        /// <returns>The plan, the physical type of its rows, and their format.</returns>
        /// <remarks>
        /// The one member a node must write. It may hand up either kind of sequence whatever the implementor
        /// is building: what does not match is read across, once, and the node is told nothing about it. A
        /// node whose rows can only be awaited, and which has inputs, writes this as
        /// <c>implementor.Asynchronously(this, pref)</c>.
        /// </remarks>
        ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref);

        /// <summary>
        /// Builds the plan for this node where the plan being built awaits its rows.
        /// </summary>
        /// <param name="implementor">An implementor whose <see cref="ClrEnumerableRelImplementor.Async"/> is
        /// set, so that <see cref="ClrEnumerableRelImplementor.Call"/> lands on the awaiting operators.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented.</param>
        /// <returns>The plan, the physical type of its rows, and their format.</returns>
        /// <remarks>
        /// Optional, and by default <see cref="Implement"/> on the same implementor: a body written through
        /// <see cref="ClrEnumerableRelImplementor.Call"/> is already the awaiting one, and a leaf that is not
        /// hands up a synchronous sequence which is then read across. Override it where the node's awaiting
        /// body is genuinely different code — an adapter with a separate awaiting client, a leaf whose SPI
        /// has two halves — or, where the node has inputs and only a synchronous body, as the one line
        /// <c>implementor.Synchronously(this, pref)</c>.
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
