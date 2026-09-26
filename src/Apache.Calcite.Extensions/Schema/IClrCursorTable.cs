using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table whose rows are read through a forward-only cursor it opens.
    /// </summary>
    /// <remarks>
    /// The third of this project's table SPIs, for the table whose natural shape is a cursor rather than a
    /// sequence: a <c>DbDataReader</c>, opened with <c>ExecuteReader</c> or <c>ExecuteReaderAsync</c> and
    /// advanced with <c>Read</c> or <c>ReadAsync(token)</c>. An <see cref="IClrScannableTable"/> hands back
    /// a sequence, and a sequence states once, at its enumerator, whether it will be pulled or awaited and
    /// takes its token there; the cursor convention's scan opened a cursor over it and the token an advance
    /// was given stopped at that cursor. A table implementing this hands the cursor itself in, and every
    /// <see cref="ClrCursor.ReadAsync"/> reaches it with the token of that advance.
    ///
    /// <para><b><see cref="Open"/> is required and <see cref="OpenAsync"/> is optional</b>, the shape the
    /// other two SPIs have and the reason they have it: one interface, both halves on it, so that a scan
    /// never has to ask which kind a table is. A table whose open can only be awaited writes both, the
    /// synchronous one by blocking with the context suppressed, and does not leave the default in place: the
    /// default would hand a blocking open to a caller who asked to await.</para>
    ///
    /// <para>The values in each row are Java's, exactly as an <see cref="IClrScannableTable"/>'s are and for
    /// the same reason: everything downstream is Calcite's.</para>
    /// </remarks>
    public interface IClrCursorTable : Table
    {

        /// <summary>
        /// Opens a cursor over this table's rows.
        /// </summary>
        /// <param name="root">The context the query is being run against, which is where a table reaches
        /// the schema, the query's parameters and its cancel flag.</param>
        /// <returns>The cursor, positioned before its first row, one <c>object?[]</c> per row.</returns>
        /// <remarks>
        /// Opening is the acquisition: a table over a statement sends the statement here, as
        /// <c>ScannableTable.scan</c>'s enumerator does at <c>enumerator()</c>.
        /// </remarks>
        IClrCursor<object?[]> Open(DataContext root);

        /// <summary>
        /// Opens a cursor over this table's rows, awaiting the acquisition.
        /// </summary>
        /// <param name="root">The context the query is being run against.</param>
        /// <param name="cancellationToken">The token the open runs under; each advance brings its own.</param>
        /// <returns>The cursor, positioned before its first row.</returns>
        /// <remarks>
        /// By default <see cref="Open"/> completed, which is right for a table whose open does not wait on
        /// anything and wrong for one whose <see cref="Open"/> blocks.
        /// </remarks>
        ValueTask<IClrCursor<object?[]>> OpenAsync(DataContext root, CancellationToken cancellationToken) => new(Open(root));

    }

}
