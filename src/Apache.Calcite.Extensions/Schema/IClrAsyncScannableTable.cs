using System.Collections.Generic;

using org.apache.calcite;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A table that produces its rows as an <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="ScannableTable"/>, member for member, with the sequence swapped. It is
    /// the only leaf a plan of the <c>ClrAsyncEnumerableConvention</c> calling convention can read: that
    /// convention has no converter to any other, because one would be a synchronous pull behind an awaited
    /// interface or the reverse. A query touching a <see cref="ScannableTable"/>, a
    /// <see cref="QueryableTable"/> or a <see cref="FilterableTable"/> is still planned -- Calcite reads it
    /// and a converter carries the rows across -- but that part of it is not asynchronous and cannot be.
    ///
    /// <para>There is no cancellation parameter, and that is not an omission. A token enters an
    /// <see cref="IAsyncEnumerable{T}"/> at <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, which is
    /// where .NET puts it and what lets a plan carry no token of its own — an implementation declares
    /// <c>[EnumeratorCancellation]</c> on an iterator's token parameter and the language threads it.</para>
    ///
    /// <para>The other half of Calcite's table SPI is <see cref="QueryableTable"/>, whose counterpart here
    /// is <see cref="IClrAsyncQueryableTable"/>: a table states an element type of its own and hands back an
    /// expression rather than being called. A table implements whichever suits it, and
    /// <c>ClrAsyncEnumerableTableScan</c> reads both.</para>
    ///
    /// <para>The values in each row are Java's — <c>java.lang.Integer</c>, <c>java.lang.String</c>,
    /// <c>BigDecimal</c> — exactly as a <see cref="ScannableTable"/>'s are, because everything downstream of
    /// this is Calcite's and every boundary where a value crosses between the two runtimes is an adapter. A
    /// table handing back CLR primitives leaves two representations of one value in a plan, and Calcite's own
    /// comparators fail on them — at once rather than quietly, because what reads a field is
    /// <c>SqlFunctions.toInt</c> or a cast to the boxed type the row type declares.
    /// <c>ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys</c> holds that for the synchronous SPI, and
    /// the contract is the same one.</para>
    /// </remarks>
    public interface IClrAsyncScannableTable : Table
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
        /// <para>The elements are nullable, as Calcite declares them --
        /// <c>Enumerable&lt;@Nullable Object[]&gt;</c> -- because a field of a row is null wherever its
        /// column is. Saying <c>object[]</c> claimed otherwise.</para>
        /// </remarks>
        IAsyncEnumerable<object?[]> ScanAsync(DataContext root);

    }

}
