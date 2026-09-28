using System.Collections.Generic;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table that produces its rows as a .NET sequence, pulled or awaited.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="ScannableTable"/>, returning a .NET sequence instead of a linq4j
    /// <c>Enumerable</c>, so a table written in .NET does not have to implement Calcite's
    /// <c>Enumerator</c>. Implementing <see cref="ScannableTable"/> still works and suits a table that
    /// already has a linq4j sequence.
    ///
    /// <para><see cref="Scan"/> is required and <see cref="ScanAsync"/> defaults to it. A table whose rows are
    /// already in memory implements <see cref="Scan"/> only. A table whose rows arrive over I/O implements
    /// both. A table whose rows can only be produced asynchronously implements <see cref="ScanAsync"/> and
    /// writes <see cref="Scan"/> by draining it; it should not leave <see cref="ScanAsync"/> to the default,
    /// which would then block a thread for an asynchronous caller.</para>
    ///
    /// <para>The values in each row must be Java values of the types Calcite's type factory uses for the
    /// columns — <c>java.lang.Integer</c>, <c>java.lang.String</c>, <c>java.math.BigDecimal</c> and so
    /// on — as for a <see cref="ScannableTable"/>. They are not converted or checked; a value of the wrong
    /// type makes the query fail when the field is read.</para>
    ///
    /// <para>See also <see cref="IClrCursorTable"/>, which returns a cursor.</para>
    /// </remarks>
    public interface IClrScannableTable : Table
    {

        /// <summary>
        /// Returns this table's rows.
        /// </summary>
        /// <param name="root">The context the query is being run against, through which a table reaches the
        /// schema, the query's parameter values and its cancel flag.</param>
        /// <returns>The rows, one <c>object?[]</c> per row.</returns>
        /// <remarks>
        /// The counterpart of <c>ScannableTable.scan</c>. Every row is an array, including the rows of a
        /// one-column table. A field is <see langword="null"/> where its column's value is null.
        /// </remarks>
        IEnumerable<object?[]> Scan(DataContext root);

        /// <summary>
        /// Returns this table's rows as an asynchronous sequence.
        /// </summary>
        /// <param name="root">The context the query is being run against, through which a table reaches the
        /// schema, the query's parameter values and its cancel flag.</param>
        /// <returns>The rows, one <c>object?[]</c> per row.</returns>
        /// <remarks>
        /// Called when the plan is opened asynchronously. By default reads <see cref="Scan"/> as an
        /// asynchronous sequence that never suspends; override it when the rows arrive asynchronously.
        ///
        /// <para>The cancellation token of the asynchronous open is passed to
        /// <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>; an iterator method receives it through a
        /// parameter marked <c>[EnumeratorCancellation]</c>.</para>
        /// </remarks>
        IAsyncEnumerable<object?[]> ScanAsync(DataContext root) => ClrSequences.ToAsyncEnumerable(Scan(root));

    }

}
