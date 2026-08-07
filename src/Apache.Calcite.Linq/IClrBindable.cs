using System.Collections.Generic;

using org.apache.calcite;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// A compiled plan of the <see cref="ClrEnumerableConvention"/> calling convention, bound to a
    /// <see cref="DataContext"/> when it is run.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>Bindable</c>, which is one method — <c>Enumerable&lt;T&gt;
    /// bind(DataContext)</c> — and of <c>Typed</c>, which is one more. Calcite's compiled plan is a Janino
    /// class declaring both; ours is a delegate, and this is what carries it.
    ///
    /// <para>The two are merged into one interface rather than ported separately. Calcite keeps them apart
    /// because a generated class may declare either, and <c>ArrayBindable</c> exists to combine them; here
    /// there is one implementation and one caller, and nothing ever holds the element type alone.</para>
    ///
    /// <para><see cref="Bind"/> is not generic where Calcite's is. A plan's rows leave this convention as
    /// objects — <see cref="ClrEnumerableRelImplementor.ImplementRoot"/> boxes a sequence of primitives on
    /// the way out, because that is what every reader of a Calcite result expects — so the one type
    /// argument Calcite's interface takes would always be <see cref="object"/>.</para>
    /// </remarks>
    public interface IClrBindable
    {

        /// <summary>
        /// Runs the plan against a <see cref="DataContext"/> and returns its rows.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <returns>The rows, produced as the sequence is enumerated.</returns>
        IEnumerable<object> Bind(DataContext root);

        /// <summary>
        /// Gets the Java type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Typed.getElementType</c>, and it has a caller rather than being carried for
        /// symmetry: Calcite's prepare framework reads it off the prepared result and hands it to
        /// <c>Meta.CursorFactory.deduce</c>, which decides how a row is read back.
        /// </remarks>
        java.lang.reflect.Type ElementType { get; }

    }

}
