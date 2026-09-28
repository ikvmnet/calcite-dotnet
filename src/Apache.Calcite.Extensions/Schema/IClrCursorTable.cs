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
    /// For a source that is naturally a cursor, such as a <c>DbDataReader</c>. The cursor the table opens
    /// becomes the leaf of the plan, so each <see cref="IClrCursor.ReadAsync"/> on the plan's cursor reaches
    /// the table's cursor with that advance's token. A sequence from an <see cref="IClrScannableTable"/>
    /// takes a token only once, when it is enumerated.
    ///
    /// <para><see cref="Open"/> is required and <see cref="OpenAsync"/> defaults to it. A table whose open
    /// can only be completed asynchronously implements <see cref="OpenAsync"/> and writes <see cref="Open"/>
    /// by blocking on it. Blocking must be done with <see cref="System.Threading.SynchronizationContext"/>
    /// cleared before the asynchronous call is made, or it can deadlock under a single-threaded context.</para>
    ///
    /// <para>The values in each row must be Java values of the types Calcite's type factory uses for the
    /// columns — <c>java.lang.Integer</c>, <c>java.lang.String</c>, <c>java.math.BigDecimal</c> and so
    /// on — as for any table Calcite reads.</para>
    /// </remarks>
    public interface IClrCursorTable : Table
    {

        /// <summary>
        /// Opens a cursor over this table's rows.
        /// </summary>
        /// <param name="root">The context the query is being run against, through which a table reaches the
        /// schema, the query's parameter values and its cancel flag.</param>
        /// <returns>The cursor, positioned before its first row, one <c>object?[]</c> per row.</returns>
        /// <remarks>
        /// Called when the plan is opened. A table over a statement executes the statement here.
        /// </remarks>
        IClrCursor<object?[]> Open(DataContext root);

        /// <summary>
        /// Opens a cursor over this table's rows asynchronously.
        /// </summary>
        /// <param name="root">The context the query is being run against.</param>
        /// <param name="cancellationToken">The token that cancels the open. Each advance of the cursor takes
        /// its own.</param>
        /// <returns>The cursor, positioned before its first row.</returns>
        /// <remarks>
        /// By default calls <see cref="Open"/> and returns its cursor as a completed task. Override it when
        /// opening waits on I/O, so that an asynchronous caller is not blocked.
        /// </remarks>
        ValueTask<IClrCursor<object?[]>> OpenAsync(DataContext root, CancellationToken cancellationToken) => new(Open(root));

    }

}
