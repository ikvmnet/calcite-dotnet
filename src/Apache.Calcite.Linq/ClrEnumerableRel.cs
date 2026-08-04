using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.util;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// A relational expression of the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="EnumerableRel"/>. <see cref="Implement"/> returns a
    /// <see cref="ClrEnumerableResult"/> against Calcite's <c>EnumerableRel.Result</c>, and the two carry the
    /// same three things — the physical type, the row format, and the plan. Only the last differs, and only in
    /// kind: a <see cref="System.Linq.Expressions.Expression"/> whose value is the sequence, against a linq4j
    /// <c>BlockStatement</c> whose value comes from a <c>return</c> inside it.
    ///
    /// <para>That difference is forced, not chosen. Calcite generates one Java method per plan, so a node can
    /// only contribute statements to it and the single way to give a value back is to return; a parent then
    /// appends the child's statements to its own. An expression tree has no method to append to, and a parent
    /// composes: the child's expression is an argument of the operator its own becomes. Where a node does need
    /// statements it uses <c>Expression.Block</c>, whose value is its last expression — so a block is still a
    /// block, it is just an expression as well, which is exactly what removes the need to flatten.</para>
    ///
    /// <para>Nothing here runs a query. The result is the plan, and it is a plan of an
    /// <see cref="System.Collections.Generic.IEnumerable{T}"/> only in the sense that the expression's type is
    /// one; there is no <c>DataContext</c> to enumerate against until <c>ClrEnumerableInterpretable</c> compiles the
    /// whole thing and a caller binds it.</para>
    ///
    /// <para>The trait derivation Calcite gives <see cref="EnumerableRel"/> as interface defaults is repeated
    /// here, because C# does not inherit the defaults of an interface IKVM compiled. Doing it once means a
    /// node carries nothing but its own work.</para>
    /// </remarks>
    public interface ClrEnumerableRel : PhysicalNode
    {

        /// <summary>
        /// Creates a plan for this expression according to a calling convention.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref">Preferred representation for rows in the result expression.</param>
        /// <returns></returns>
        ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref);

        /// <inheritdoc cref="PhysicalNode.passThroughTraits" />
        Pair PhysicalNode.passThroughTraits(RelTraitSet required) => null!;

        /// <inheritdoc cref="PhysicalNode.deriveTraits" />
        Pair PhysicalNode.deriveTraits(RelTraitSet childTraits, int childId) => null!;

        /// <inheritdoc cref="PhysicalNode.getDeriveMode" />
        DeriveMode PhysicalNode.getDeriveMode() => DeriveMode.LEFT_FIRST;

        /// <inheritdoc cref="PhysicalNode.passThrough" />
        RelNode PhysicalNode.passThrough(RelTraitSet required) => PhysicalNode.__DefaultMethods.passThrough(this, required);

        /// <inheritdoc cref="PhysicalNode.derive(RelTraitSet, int)" />
        RelNode PhysicalNode.derive(RelTraitSet childTraits, int childId) => PhysicalNode.__DefaultMethods.derive(this, childTraits, childId);

        /// <inheritdoc cref="PhysicalNode.derive(java.util.List)" />
        java.util.List PhysicalNode.derive(java.util.List inputTraits) => PhysicalNode.__DefaultMethods.derive(this, inputTraits);

    }

}
