using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor.Tests;
using Apache.Calcite.Extensions.Schema;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The rows the asynchronous differential tests run over, shared by <see cref="SyncRowsTable"/> and
    /// <see cref="AsyncRowsTable"/>.
    /// </summary>
    /// <remarks>
    /// Awaiting reads are compared against synchronous reads rather than against Calcite, since the
    /// synchronous reads are compared against Calcite elsewhere. That comparison needs both sides to read the
    /// same rows, so there is one copy.
    /// </remarks>
    static class AsyncTestRows
    {

        /// <summary>
        /// Rows with partitions, ties and a null, so a window function has something to distinguish.
        /// </summary>
        public static readonly object?[][] Sales =
        [
            [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "A"],
            [java.lang.Integer.valueOf(2), "EAST", java.lang.Integer.valueOf(20), "B"],
            [java.lang.Integer.valueOf(3), "EAST", java.lang.Integer.valueOf(20), "C"],
            [java.lang.Integer.valueOf(4), "WEST", java.lang.Integer.valueOf(30), "D"],
            [java.lang.Integer.valueOf(5), "WEST", null, "E"],
            [java.lang.Integer.valueOf(6), "WEST", java.lang.Integer.valueOf(5), "F"],
        ];

        /// <summary>
        /// Rows sorted by their first field and declared so, which gives a merge join or a merge union the
        /// collation it needs to be chosen.
        /// </summary>
        public static readonly object?[][] Sorted =
        [
            [java.lang.Integer.valueOf(1), "A"],
            [java.lang.Integer.valueOf(2), "B"],
            [java.lang.Integer.valueOf(2), "C"],
            [java.lang.Integer.valueOf(4), "D"],
        ];

        /// <summary>
        /// Twelve distinct keys, a build-side size at which a hash join's unmatched rows come out in a different
        /// order from the lookup map's.
        /// </summary>
        /// <remarks>
        /// <c>hashEquiJoin_</c> ends a right or full join by copying the lookup's key set into a
        /// <c>java.util.HashSet</c> and iterating that. <c>HashSet(Collection)</c> sizes its table as
        /// <c>tableSizeFor(max((int) (n / 0.75f) + 1, 16))</c>, while a map grown by insertion holds the
        /// smallest power of two at or above 16 with <c>n &lt;= 0.75 * cap</c>. The two differ where
        /// <c>n = 0.75 * 2^k</c> (12, 24, 48): at twelve keys the map has 16 buckets and the copy 32.
        /// <c>SALES</c>, with six rows, puts both at 16.
        /// </remarks>
        public static readonly object?[][] Wide = BuildWide();

        static object?[][] BuildWide()
        {
            var rows = new object?[12][];
            for (var i = 0; i < rows.Length; i++)
                rows[i] = [string.Format("K{0:D2}", i + 1), java.lang.Integer.valueOf(i + 1)];

            return rows;
        }

        /// <summary>
        /// Values in a column of type ANY, whose Java class is <c>Object</c>.
        /// </summary>
        /// <remarks>
        /// A provider type the ADO.NET adapter has no <c>SqlTypeName</c> for arrives as ANY, so an aggregate
        /// over one accumulates a value whose type is known only at run time. The same rows as
        /// <c>ClrCursorConventionDifferentialTests.AnysTable</c>.
        /// </remarks>
        public static readonly object?[][] Anys =
        [
            [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "b"],
            [java.lang.Integer.valueOf(2), "EAST", java.lang.Double.valueOf(20.5), "a"],
            [java.lang.Integer.valueOf(3), "WEST", java.lang.Integer.valueOf(30), "d"],
            [java.lang.Integer.valueOf(4), "WEST", null, null],
            [java.lang.Integer.valueOf(5), "WEST", java.lang.Integer.valueOf(5), "c"],
        ];

        /// <summary>
        /// Collections in ANY columns, as a document store returns a path that holds a JSON array.
        /// </summary>
        /// <remarks>
        /// The same rows as <c>ClrCursorConventionDifferentialTests.DocsTable</c>.
        /// </remarks>
        public static readonly object?[][] Docs =
        [
            [java.lang.Integer.valueOf(1), List("red", "green"), List(java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2))],
            [java.lang.Integer.valueOf(2), List("blue"), List()],
            [java.lang.Integer.valueOf(3), null, List(java.lang.Integer.valueOf(3), java.lang.Double.valueOf(4.5))],
        ];

        /// <summary>
        /// Values in ANY columns as a document store returns them: a GUID, a timestamp and a number, each
        /// written as JSON writes it.
        /// </summary>
        /// <remarks>
        /// The same rows as <c>ClrCursorConventionDifferentialTests.CastsTable</c>, for casts out of ANY.
        /// </remarks>
        public static readonly object?[][] Casts =
        [
            [java.lang.Integer.valueOf(1), "11111111-1111-1111-1111-111111111111", "2026-01-01 00:00:00", java.lang.Long.valueOf(1767225600000L), "42"],
            [java.lang.Integer.valueOf(2), "22222222-2222-2222-2222-222222222222", "2025-06-15 12:30:45", java.lang.Long.valueOf(0L), "7"],
        ];

        /// <summary>
        /// Returns the CASTS row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>An INTEGER <c>ID</c> and four nullable ANY columns, <c>G</c>, <c>T</c>, <c>M</c> and <c>N</c>.</returns>
        public static RelDataType CastsRowType(RelDataTypeFactory typeFactory)
        {
            RelDataType Any() => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);

            return typeFactory.builder()
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("G", Any())
                .add("T", Any())
                .add("M", Any())
                .add("N", Any())
                .build();
        }

        /// <summary>
        /// Returns the WIDE row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>A VARCHAR <c>K</c> and an INTEGER <c>N</c>.</returns>
        public static RelDataType WideRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("K", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("N", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .build();
        }

        /// <summary>
        /// Returns the SALES row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>The <c>SALES</c> columns, of which <c>AMOUNT</c> is nullable.</returns>
        public static RelDataType SalesRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("REGION", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("AMOUNT", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                .add("LABEL", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .build();
        }

        /// <summary>
        /// Timestamps spread across three hours, two of them within the same hour, so a window table function
        /// puts rows in more than one bucket.
        /// </summary>
        /// <remarks>
        /// The same four rows as the <c>EVENTS</c> table of <c>ClrCursorConventionDifferentialTests</c>, for
        /// <c>TUMBLE</c>, <c>HOP</c> and <c>SESSION</c>.
        /// </remarks>
        public static readonly object?[][] Events =
        [
            [java.lang.Long.valueOf(EventsBase), java.lang.Integer.valueOf(1)],
            [java.lang.Long.valueOf(EventsBase + (EventsHour / 6)), java.lang.Integer.valueOf(2)],
            [java.lang.Long.valueOf(EventsBase + EventsHour), java.lang.Integer.valueOf(3)],
            [java.lang.Long.valueOf(EventsBase + (EventsHour * 2) + (EventsHour / 2)), java.lang.Integer.valueOf(4)],
        ];

        const long EventsHour = 3600000L;

        const long EventsBase = 1704067200000L;

        /// <summary>
        /// Returns the EVENTS row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>A TIMESTAMP <c>ROWTIME</c> and an INTEGER <c>ID</c>.</returns>
        public static RelDataType EventsRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("ROWTIME", typeFactory.createSqlType(SqlTypeName.TIMESTAMP))
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .build();
        }

        /// <summary>
        /// Returns the SORTED row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>An INTEGER <c>K</c> and a VARCHAR <c>V</c>.</returns>
        public static RelDataType SortedRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("K", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("V", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .build();
        }

        /// <summary>
        /// Returns the ANYS row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>An INTEGER <c>ID</c>, a VARCHAR <c>K</c>, and nullable ANY columns <c>V</c> and <c>S</c>.</returns>
        public static RelDataType AnysRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("K", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("V", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                .add("S", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                .build();
        }

        /// <summary>
        /// Returns the DOCS row type.
        /// </summary>
        /// <param name="typeFactory">The factory to build the type with.</param>
        /// <returns>An INTEGER <c>ID</c> and nullable ANY columns <c>TAGS</c> and <c>NUMS</c>.</returns>
        public static RelDataType DocsRowType(RelDataTypeFactory typeFactory)
        {
            RelDataType Any() => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);

            return typeFactory.builder()
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("TAGS", Any())
                .add("NUMS", Any())
                .build();
        }

        /// <summary>
        /// Returns a <c>java.util.List</c> of the items given, the form in which <c>SqlFunctions.flatProduct</c>
        /// reads a collection behind an ANY column.
        /// </summary>
        /// <param name="items">The list's elements, in order.</param>
        /// <returns>A new <c>java.util.ArrayList</c> holding the items.</returns>
        static java.util.List List(params object?[] items)
        {
            var list = new java.util.ArrayList();
            foreach (var item in items)
                list.add(item);

            return list;
        }

    }

    /// <summary>
    /// A Calcite <c>ScannableTable</c> over one of the shared row sets.
    /// </summary>
    /// <param name="rows">The table's rows.</param>
    /// <param name="rowType">Builds the table's row type from the type factory it is given.</param>
    /// <param name="sorted">Whether the table's statistic states that its rows are sorted by the first column.</param>
    sealed class SyncRowsTable(object?[][] rows, System.Func<RelDataTypeFactory, RelDataType> rowType, bool sorted) : AbstractTable, ScannableTable
    {

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory) => rowType(typeFactory);

        /// <inheritdoc />
        public override Statistic getStatistic()
        {
            return sorted
                ? Statistics.of(rows.Length, new java.util.ArrayList(), com.google.common.collect.ImmutableList.of(RelCollations.of(0)))
                : base.getStatistic();
        }

        /// <inheritdoc />
        public Enumerable scan(DataContext root)
        {
            var list = new java.util.ArrayList();
            foreach (var row in rows)
                list.add(row);

            return Linq4j.asEnumerable(list);
        }

    }

    /// <summary>
    /// An <see cref="IClrScannableTable"/> over one of the shared row sets whose rows arrive asynchronously.
    /// </summary>
    /// <param name="rows">The table's rows.</param>
    /// <param name="rowType">Builds the table's row type from the type factory it is given.</param>
    /// <param name="sorted">Whether the table's statistic states that its rows are sorted by the first column.</param>
    /// <remarks>
    /// The scan suspends on every row. A sequence that completed synchronously would exercise none of the
    /// resumption, ordering or disposal behaviour of an awaiting read, and an operator that dropped its
    /// continuation would still pass.
    /// </remarks>
    sealed class AsyncRowsTable(object?[][] rows, System.Func<RelDataTypeFactory, RelDataType> rowType, bool sorted) : AbstractTable, IClrScannableTable
    {

        /// <summary>
        /// Called with the running row count as each row is produced.
        /// </summary>
        /// <remarks>
        /// Lets a test cancel at a known row rather than after a delay; the rows are produced faster than any
        /// useful delay, so a timed cancellation can pass because the query finished first.
        /// </remarks>
        public System.Action<int>? OnRow { get; set; }

        /// <summary>
        /// Gets how many rows this table has produced, across every scan of it.
        /// </summary>
        public int Produced { get; private set; }

        /// <summary>
        /// Gets whether the last scan was enumerated with a token that could be cancelled.
        /// </summary>
        public bool SawCancellableToken { get; private set; }

        /// <summary>
        /// Gets whether the sequence was disposed and its disposal awaited, rather than abandoned.
        /// </summary>
        /// <remarks>
        /// Set at the end of the iterator's <c>finally</c>, after an await, so it is set only when the
        /// enumerator's disposal runs to completion.
        /// </remarks>
        public bool DisposedAsynchronously { get; private set; }

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory) => rowType(typeFactory);

        /// <inheritdoc />
        public override Statistic getStatistic()
        {
            return sorted
                ? Statistics.of(rows.Length, new java.util.ArrayList(), com.google.common.collect.ImmutableList.of(RelCollations.of(0)))
                : base.getStatistic();
        }

        /// <inheritdoc />
        public IAsyncEnumerable<object?[]> ScanAsync(DataContext root) => Rows();

        /// <inheritdoc />
        /// <remarks>
        /// This table's rows arrive only asynchronously, so a synchronous scan drains <see cref="ScanAsync"/>,
        /// blocking the calling thread for each row.
        /// </remarks>
        public IEnumerable<object?[]> Scan(DataContext root) => BlockingDrain.Of(ScanAsync(root));

        async IAsyncEnumerable<object?[]> Rows([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            SawCancellableToken = cancellationToken.CanBeCanceled;

            try
            {
                foreach (var row in rows)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // suspends, for the reason the class remarks give
                    await Task.Yield();

                    Produced++;
                    OnRow?.Invoke(Produced);

                    yield return row;
                }
            }
            finally
            {
                // an awaited disposal reaches the assignment below; an abandoned one does not
                await Task.Yield();

                DisposedAsynchronously = true;
            }
        }

    }


    /// <summary>
    /// Drains an asynchronous sequence on the calling thread.
    /// </summary>
    /// <remarks>
    /// This is what a table whose rows arrive only asynchronously writes for its <c>Scan</c>. It is written
    /// here rather than calling <c>ClrSequences</c>, which is internal and so unavailable to an adapter
    /// outside this repository.
    ///
    /// <para>The operators of this convention await without <c>ConfigureAwait(false)</c>, so a continuation
    /// is posted to whatever synchronization context is current when the sequence suspends, which happens
    /// inside the call to <c>MoveNextAsync</c>, before any wait begins. Blocking on that call under a context
    /// deadlocks, so the context is cleared before each call rather than only around the wait.</para>
    /// </remarks>
    static class BlockingDrain
    {

        public static IEnumerable<T> Of<T>(IAsyncEnumerable<T> source)
        {
            var e = source.GetAsyncEnumerator();

            try
            {
                while (Suppressed(() => e.MoveNextAsync().AsTask().GetAwaiter().GetResult()))
                    yield return e.Current;
            }
            finally
            {
                Suppressed(() =>
                {
                    e.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    return true;
                });
            }
        }

        static bool Suppressed(System.Func<bool> body)
        {
            var context = SynchronizationContext.Current;
            if (context == null)
                return body();

            SynchronizationContext.SetSynchronizationContext(null);

            try
            {
                return body();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(context);
            }
        }

    }

}
