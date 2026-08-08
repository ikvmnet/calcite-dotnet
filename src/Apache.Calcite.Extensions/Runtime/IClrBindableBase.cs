using System;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// What every compiled plan of these conventions has in common.
    /// </summary>
    /// <remarks>
    /// One member, because it is the only one a caller reads without knowing which convention prepared the
    /// statement: <c>ClrSignature</c> holds a plan as this, and <c>ClrPrepareImpl.Describe</c> reads nothing
    /// else off it. Binding is on <see cref="IClrBindable"/> and <see cref="IClrAsyncBindable"/>, because
    /// the two return different sequences and a common <c>Bind</c> could only return one neither of them
    /// wants.
    ///
    /// <para>It names no convention and depends on neither, which is what lets it live here rather than in
    /// one of their namespaces.</para>
    /// </remarks>
    public interface IClrBindableBase
    {

        /// <summary>
        /// Gets the CLR type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Typed.getElementType</c>, as a <see cref="Type"/> rather than a
        /// <c>java.lang.reflect.Type</c>. A compiled plan is a delegate over CLR types and its rows are CLR
        /// objects; what the type factory called the row is the prepare pipeline's business, and
        /// <c>ClrPrepareResult.ElementType</c> is where that answer stays for
        /// <c>Meta.CursorFactory.deduce</c>. Handing a Java type out of a runtime interface would make every
        /// caller convert one back.
        /// </remarks>
        Type ElementType { get; }

    }

}
