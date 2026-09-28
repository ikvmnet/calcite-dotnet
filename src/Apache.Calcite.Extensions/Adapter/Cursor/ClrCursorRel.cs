
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// A relational expression of the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <see cref="EnumerableRel"/>. Implement this interface to add a node to the convention. The
    /// trait methods of <see cref="PhysicalNode"/> have the defaults Calcite gives them, so a node overrides
    /// only those it needs.
    ///
    /// <para>A node's implementation is an expression that, when evaluated, opens a cursor over the node's
    /// rows: evaluating it acquires the inputs, as linq4j's <c>enumerator()</c> does. A sort drains its input
    /// and a leaf executes its statement at that point. An operator that acquires an input later, such as a
    /// concatenation reaching its next source, takes that input as an opener built with
    /// <see cref="ClrCursorRelImplementor.Opener"/> or <see cref="ClrCursorRelImplementor.OpenerAsync"/>.</para>
    ///
    /// <para>A node has two implementations. <see cref="Implement"/> builds an open that acquires its inputs
    /// synchronously and reaches them through <see cref="ClrCursorRelImplementor.VisitChild"/>;
    /// <see cref="ImplementAsync"/> builds one that awaits them and reaches them through
    /// <see cref="ClrCursorRelImplementor.VisitChildAsync"/>. Both produce the same kind of cursor, which can
    /// be read with either <c>Read</c> or <c>ReadAsync</c>, and
    /// <see cref="ClrCursorRelImplementor.ImplementRoot"/> calls both to build a <see cref="ClrCursorFactory"/>.
    /// An input acquired during reading, rather than at open, is visited both ways from both implementations,
    /// because whether the consumer is reading synchronously is known only at that advance; the cursor is given
    /// both openers and calls the matching one.</para>
    ///
    /// <para><see cref="Implement"/> is required. <see cref="ImplementAsync"/> defaults to wrapping it with
    /// <see cref="ClrCursorRelImplementor.Awaited"/>, which is correct only for a node that visits no input at
    /// open; a node that does must implement both.</para>
    /// </remarks>
    public interface ClrCursorRel : PhysicalNode
    {

        /// <summary>
        /// Builds the expression that opens this node's cursor, acquiring its inputs synchronously.
        /// </summary>
        /// <param name="implementor">The implementor. Visit inputs with
        /// <see cref="ClrCursorRelImplementor.VisitChild"/> and build the return value with
        /// <see cref="ClrCursorRelImplementor.Result"/>.</param>
        /// <param name="pref">The row representation the parent prefers. A node may choose another; its
        /// result's physical type says which.</param>
        /// <returns>The open expression and the physical type of the rows it yields.</returns>
        ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref);

        /// <summary>
        /// Builds the expression that opens this node's cursor, awaiting the acquisition of its inputs.
        /// </summary>
        /// <param name="implementor">The implementor. Visit inputs with
        /// <see cref="ClrCursorRelImplementor.VisitChildAsync"/>, not
        /// <see cref="ClrCursorRelImplementor.VisitChild"/>, and build the return value with
        /// <see cref="ClrCursorRelImplementor.ResultAsync"/>.</param>
        /// <param name="pref">The row representation the parent prefers.</param>
        /// <returns>The awaiting open expression and the physical type of the rows it yields.</returns>
        /// <remarks>
        /// By default, <see cref="Implement"/> wrapped with <see cref="ClrCursorRelImplementor.Awaited"/>. That
        /// is correct only for a node that visits no input at open; any other node must override this.
        /// </remarks>
        ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref) => implementor.Awaited(Implement(implementor, pref));

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
