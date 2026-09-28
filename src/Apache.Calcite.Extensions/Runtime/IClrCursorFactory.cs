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
    /// The counterpart of Calcite's <c>Bindable</c>, returning a cursor rather than a sequence. A factory
    /// can be opened any number of times; each open returns an independent cursor. Whichever open is used,
    /// the cursor it returns can be advanced either synchronously or asynchronously.
    /// </remarks>
    public interface IClrCursorFactory
    {

        /// <summary>
        /// Opens a cursor over the plan's rows synchronously.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameter values and other runtime
        /// values from.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        /// <remarks>
        /// Opening does the work the plan needs before its first row, such as draining a sort's input or
        /// executing a leaf's statement, on the calling thread.
        /// </remarks>
        IClrCursor Open(DataContext root);

        /// <summary>
        /// Opens a cursor over the plan's rows asynchronously.
        /// </summary>
        /// <param name="root">The context the query reads its schema, parameter values and other runtime
        /// values from.</param>
        /// <param name="cancellationToken">The token that cancels the open. Each later advance of the cursor
        /// takes its own token.</param>
        /// <returns>The cursor, positioned before the first row.</returns>
        ValueTask<IClrCursor> OpenAsync(DataContext root, CancellationToken cancellationToken);


        /// <summary>
        /// Gets the CLR type of one row.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Typed.getElementType</c>, as a <see cref="System.Type"/>.
        /// </remarks>
        System.Type ElementType { get; }

    }

}
