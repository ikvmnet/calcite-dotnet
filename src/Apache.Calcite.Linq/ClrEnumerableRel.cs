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
    /// The counterpart of <see cref="EnumerableRel"/>, differing in what <see cref="Implement"/> produces: a
    /// <see cref="System.Linq.Expressions.Expression"/> yielding an <see cref="System.Collections.Generic.IEnumerable{T}"/>
    /// rather than a linq4j block for Janino to compile.
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
        ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref);

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
