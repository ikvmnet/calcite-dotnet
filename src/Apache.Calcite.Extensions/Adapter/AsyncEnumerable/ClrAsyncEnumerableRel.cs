using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// A relational expression of the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableRel"/>, member for member, for a plan whose sequences are
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>.
    ///
    /// <para><see cref="Implement"/> is not asynchronous and does not become so. It runs while the statement
    /// is being prepared, builds an expression tree, and has nothing to await; what differs from the
    /// synchronous convention is the type of the sequence the tree it builds yields, and which operator
    /// table the node calls. The <c>async</c> and the <c>await foreach</c> are all in
    /// <see cref="ClrAsyncEnumerableDefaults"/>, which is what that tree calls into.</para>
    /// </remarks>
    public interface ClrAsyncEnumerableRel : PhysicalNode
    {

        /// <summary>
        /// Builds the plan for this node.
        /// </summary>
        /// <param name="implementor">Reach the inputs through
        /// <see cref="ClrAsyncEnumerableRelImplementor.VisitChild"/>, and build the return value with
        /// <see cref="ClrAsyncEnumerableRelImplementor.Result"/>.</param>
        /// <param name="pref">How the parent would prefer this node's rows represented. A node may return
        /// another format; the result says which it chose.</param>
        /// <returns>The plan, the physical type of its rows, and their format.</returns>
        ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref);

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
