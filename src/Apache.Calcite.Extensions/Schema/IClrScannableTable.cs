using System.Collections.Generic;

using org.apache.calcite;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table that produces its rows as an <see cref="IEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="ScannableTable"/>, member for member, with the sequence swapped — and
    /// the synchronous half of the pair whose other half is <see cref="IClrAsyncScannableTable"/>.
    ///
    /// <para>It exists because a <see cref="ScannableTable"/> written in .NET has to hand back a linq4j
    /// <c>Enumerable</c>, which means building one of Calcite's <c>Enumerator</c>s by hand, and then
    /// <c>ClrEnumerableTableScan</c> reads it straight back out through <c>FromJava</c>. The rows make a
    /// round trip through a runtime neither end of it belongs to. A table that implements this is read
    /// directly.</para>
    ///
    /// <para>Implementing Calcite's <see cref="ScannableTable"/> still works and is still the right thing
    /// for a table that has a linq4j sequence to give — an adapter over a Java source, say. This is for the
    /// ordinary case of a table whose rows are already .NET's.</para>
    ///
    /// <para>The values in each row are Java's — <c>java.lang.Integer</c>, <c>java.lang.String</c>,
    /// <c>BigDecimal</c> — exactly as a <see cref="ScannableTable"/>'s are. What is avoided is the hop the
    /// <em>sequence</em> makes, not the conversion each value needs: everything downstream is Calcite's, and
    /// every boundary where a value crosses between the two runtimes is an adapter.</para>
    /// </remarks>
    public interface IClrScannableTable : Table
    {

        /// <summary>
        /// Returns this table's rows.
        /// </summary>
        /// <param name="root">The context the query is being run against, which is where a table reaches
        /// the schema, the query's parameters and its cancel flag.</param>
        /// <returns>The rows, one <c>object?[]</c> per row, produced as the sequence is enumerated.</returns>
        /// <remarks>
        /// <c>ScannableTable.scan</c>. A row is an array whatever the table's row type is; a one column
        /// table still yields a one element array, and the convention's scan is what turns that into the
        /// value.
        ///
        /// <para>The elements are nullable, as Calcite declares them —
        /// <c>Enumerable&lt;@Nullable Object[]&gt;</c> — because a field of a row is null wherever its
        /// column is.</para>
        /// </remarks>
        IEnumerable<object?[]> Scan(DataContext root);

    }

}
