using System.Threading;
using System.Threading.Tasks;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A compiled plan of the <c>ClrCursorConvention</c> calling convention, opened against a
    /// <see cref="DataContext"/> when it is run.
    /// </summary>
    /// <remarks>
    /// Calcite's <c>Bindable</c> with the sequence replaced by the cursor, and what a prepared statement is
    /// read through by the ADO.NET provider: <c>ExecuteReader</c> is
    /// <see cref="Open"/>, <c>ExecuteReaderAsync(token)</c> is <see cref="OpenAsync"/>, and the reader's
    /// two advances are the cursor's. Both members are on one interface because both are the contract —
    /// the cursor either hands back has both advances, so a plan that could only be opened one way would
    /// still be read either way.
    /// </remarks>
    public interface IClrCursorFactory
    {

        /// <summary>
        /// Opens a cursor over the plan's rows, running its acquisition on the calling thread.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        IClrCursor Open(DataContext root);

        /// <summary>
        /// Opens a cursor over the plan's rows, awaiting its acquisition.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameters and stashed values
        /// from.</param>
        /// <param name="cancellationToken">The token for the acquisition; each advance takes its own.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        ValueTask<IClrCursor> OpenAsync(DataContext root, CancellationToken cancellationToken);


        /// <summary>
        /// Gets the CLR type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Typed.getElementType</c>, as a <see cref="System.Type"/> rather than a
        /// <c>java.lang.reflect.Type</c>. A compiled plan is a delegate over CLR types and its rows are CLR
        /// objects; what the type factory called the row is the prepare pipeline's business, and
        /// <c>ClrPrepare.PreparedResultImpl.ElementType</c> is where that answer stays for
        /// <c>Meta.CursorFactory.deduce</c>. Handing a Java type out of a runtime interface would make every
        /// caller convert one back.
        /// </remarks>
        System.Type ElementType { get; }

    }

}
