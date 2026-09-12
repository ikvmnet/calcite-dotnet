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
    /// The counterpart of <see cref="ScannableTable"/>, member for member, with the sequence swapped.
    ///
    /// <para>It exists because a <see cref="ScannableTable"/> written in .NET has to hand back a linq4j
    /// <c>Enumerable</c>, which means building one of Calcite's <c>Enumerator</c>s by hand, and then
    /// <c>ClrEnumerableTableScan</c> reads it straight back out through <c>FromJava</c>. The rows make a
    /// round trip through a runtime neither end of it belongs to. A table that implements this is read
    /// directly.</para>
    ///
    /// <para><b><see cref="Scan"/> is required and <see cref="ScanAsync"/> is optional</b>, which is the
    /// shape the nodes of this convention use and the shape .NET itself uses wherever a type does both.
    /// There was a second interface for the awaiting half for a while, and two interfaces could not say
    /// which member a table that implemented both actually wanted: the scan asked whether the table was of
    /// the <em>other</em> kind before asking whether it was of its own, in each of its two bodies, so a
    /// table implementing both sent the two into mutual recursion and overflowed the stack while the plan
    /// was being built. Measured. One interface makes that unrepresentable.</para>
    ///
    /// <para><b>A table chooses which halves it writes.</b> Rows that are already in hand: write
    /// <see cref="Scan"/> and take the default. Rows that arrive over a wire: write both, so that neither
    /// caller pays for the other. Rows that can <em>only</em> be awaited: write
    /// <see cref="ScanAsync"/> and write <see cref="Scan"/> as
    /// <see cref="ClrSequences.ToEnumerable{TSource}"/> over it, which is why that method is public.
    /// Leaving <see cref="ScanAsync"/> to the default in that last case is the one arrangement to avoid: the
    /// default would wrap a blocking wrap, and a caller who asked to await would get a thread blocked per
    /// row for nothing.</para>
    ///
    /// <para>The other half of Calcite's table SPI is <see cref="QueryableTable"/>, whose counterpart here
    /// is <see cref="IClrQueryableTable"/>: a table states an element type of its own and hands back an
    /// expression rather than being called. A table implements whichever suits it.</para>
    ///
    /// <para>Implementing Calcite's <see cref="ScannableTable"/> still works and is still the right thing
    /// for a table that has a linq4j sequence to give — an adapter over a Java source, say. This is for the
    /// ordinary case of a table whose rows are already .NET's.</para>
    ///
    /// <para>The values in each row are Java's — <c>java.lang.Integer</c>, <c>java.lang.String</c>,
    /// <c>BigDecimal</c> — exactly as a <see cref="ScannableTable"/>'s are. What is avoided is the hop the
    /// <em>sequence</em> makes, not the conversion each value needs: everything downstream is Calcite's, and
    /// every boundary where a value crosses between the two runtimes is an adapter. The rows go on
    /// unconverted, exactly as Calcite passes on a <see cref="ScannableTable"/>'s, and nothing checks them.
    /// Nothing needs to: what reads a field is <c>SqlFunctions.toInt</c> or a cast to the boxed type the row
    /// type declares, so a table that gets this wrong stops on its first row rather than going quietly wrong.
    /// <c>ShouldFailOverATableWhoseValuesAreNotTheTypeFactorys</c> holds that.</para>
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

        /// <summary>
        /// Returns this table's rows, as a sequence the reader awaits.
        /// </summary>
        /// <param name="root">The context the query is being run against, which is where a table reaches
        /// the schema, the query's parameters and its cancel flag.</param>
        /// <returns>The rows, one <c>object?[]</c> per row, produced as the sequence is enumerated.</returns>
        /// <remarks>
        /// What a plan compiled to an <see cref="IAsyncEnumerable{T}"/> calls, and the only leaf such a plan
        /// scans <em>itself</em>. A query touching a <see cref="ScannableTable"/>, a
        /// <see cref="QueryableTable"/> or a <see cref="FilterableTable"/> is still planned, Calcite reading
        /// it and a converter carrying the rows across, but that part of it is not asynchronous and cannot
        /// be.
        ///
        /// <para>By default <see cref="Scan"/> read across, which costs a state machine and no thread and
        /// suspends nowhere. Override it wherever the rows genuinely arrive asynchronously; the default is
        /// correct for a table whose rows are already in hand and is the wrong thing to leave in place for a
        /// table whose <see cref="Scan"/> blocks.</para>
        ///
        /// <para>There is no cancellation parameter, and that is not an omission. A token enters an
        /// <see cref="IAsyncEnumerable{T}"/> at <see cref="IAsyncEnumerable{T}.GetAsyncEnumerator"/>, which
        /// is where .NET puts it and what lets a plan carry no token of its own — an implementation declares
        /// <c>[EnumeratorCancellation]</c> on an iterator's token parameter and the language threads
        /// it.</para>
        /// </remarks>
        IAsyncEnumerable<object?[]> ScanAsync(DataContext root) => ClrSequences.ToAsyncEnumerable(Scan(root));

    }

}
