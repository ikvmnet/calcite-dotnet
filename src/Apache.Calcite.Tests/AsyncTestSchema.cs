using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

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
    /// The rows the differential tests run over, held once and read by both a synchronous and an
    /// asynchronous table.
    /// </summary>
    /// <remarks>
    /// One copy on purpose. The asynchronous convention is compared against the synchronous one rather than
    /// against Calcite — the synchronous one is already checked against Calcite query by query, so
    /// transitivity gives the same oracle — and that comparison means nothing at all if the two sides are
    /// reading two sets of rows that have drifted apart.
    /// </remarks>
    static class AsyncTestRows
    {

        /// <summary>
        /// Partitions, ties, nulls and an order, so that a window has something to disagree over.
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
        /// Rows that arrive sorted by their first field, which is where a merge join and a merge union get
        /// the collation they are only ever chosen for.
        /// </summary>
        public static readonly object?[][] Sorted =
        [
            [java.lang.Integer.valueOf(1), "A"],
            [java.lang.Integer.valueOf(2), "B"],
            [java.lang.Integer.valueOf(2), "C"],
            [java.lang.Integer.valueOf(4), "D"],
        ];

        /// <summary>
        /// Twelve distinct keys, which is the one build-side size at which a hash join's leftovers can come
        /// out in the wrong order.
        /// </summary>
        /// <remarks>
        /// <c>hashEquiJoin_</c> ends a right or a full join by copying the lookup's key set into a
        /// <c>java.util.HashSet</c> and walking that. The copy does not have the map's iteration order:
        /// <c>HashSet(Collection)</c> sizes its table as <c>tableSizeFor(max((int) (n / 0.75f) + 1, 16))</c>,
        /// while a map grown by insertion holds the smallest power of two at or above 16 that still leaves
        /// <c>n &lt;= 0.75 * cap</c>. The two disagree exactly where <c>n = 0.75 * 2^k</c> — 12, 24, 48 — and
        /// at twelve keys the map is a table of 16 and the copy a table of 32. <c>SALES</c> has six, which
        /// puts both at 16 and says nothing.
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
        /// A provider type the ADO.NET adapter has no <c>SqlTypeName</c> for arrives as ANY, so an
        /// aggregate over one of these has to accumulate a value whose type is only known at run time. The
        /// same rows as <c>ClrEnumerableDifferentialTests.AnysTable</c>, which is what makes the
        /// synchronous convention an oracle for this one.
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
        /// The values a document store puts behind an ANY column — a GUID, a timestamp and a number, each
        /// written the way JSON writes it.
        /// </summary>
        /// <remarks>
        /// The same rows as <c>ClrEnumerableDifferentialTests.CastsTable</c>, whose remarks say what a cast
        /// out of ANY actually does. This convention reaches the same generator, and the point of running
        /// the queries here is that it keeps reaching it: the failure that raised the question was in
        /// <c>ClrAsyncEnumerableDefaults.CalcRows</c>.
        /// </remarks>
        public static readonly object?[][] Casts =
        [
            [java.lang.Integer.valueOf(1), "11111111-1111-1111-1111-111111111111", "2026-01-01 00:00:00", java.lang.Long.valueOf(1767225600000L), "42"],
            [java.lang.Integer.valueOf(2), "22222222-2222-2222-2222-222222222222", "2025-06-15 12:30:45", java.lang.Long.valueOf(0L), "7"],
        ];

        /// <summary>
        /// Returns the CASTS row type.
        /// </summary>
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
        /// Timestamps an hour apart and one inside an hour, which is what a window table function needs to
        /// have anything to put in two buckets.
        /// </summary>
        /// <remarks>
        /// The same four rows the synchronous differential tests' EVENTS table holds, so that the two
        /// harnesses put the same question to TUMBLE, HOP and SESSION.
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
        public static RelDataType AnysRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("K", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .add("V", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                .add("S", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true))
                .build();
        }

    }

    /// <summary>
    /// A table of the synchronous convention over one of the shared row sets.
    /// </summary>
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
    /// A table of the asynchronous convention over one of the shared row sets.
    /// </summary>
    /// <remarks>
    /// <b>It suspends on every row.</b> A fixture whose <see cref="IAsyncEnumerable{T}"/> completes
    /// synchronously exercises none of the resumption, ordering or disposal behaviour that distinguishes
    /// this convention from the other one — every test would pass over a sequence that is asynchronous in
    /// name only, and an operator that dropped its continuation would look correct.
    /// </remarks>
    sealed class AsyncRowsTable(object?[][] rows, System.Func<RelDataTypeFactory, RelDataType> rowType, bool sorted) : AbstractTable, IClrScannableTable
    {

        /// <summary>
        /// Called with the running row count as each row is produced.
        /// </summary>
        /// <remarks>
        /// So that a test can cancel at a known point in the input rather than after a wall-clock delay. A
        /// timer is the wrong instrument here: ten thousand rows that each yield finish in well under the
        /// shortest delay worth waiting for, and a test written that way passes because the query completed
        /// rather than because anything was cancelled.
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
        /// Gets whether the sequence was disposed with something to await, rather than abandoned.
        /// </summary>
        /// <remarks>
        /// Set from the iterator's <c>finally</c>, which runs when the enumerator is disposed. A reader that
        /// disposed the plan synchronously would never reach the awaited part of it.
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
        /// The awaiting-only table's half of the bargain. There is no pulled source to offer, so this blocks
        /// a thread per row, which is what a caller reading these rows synchronously is asking for. Leaving
        /// the interface default in place instead would be the mistake: it would wrap this, and a caller who
        /// asked to await would get the blocking read back with a state machine around it.
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

                    // genuinely suspends, for the reason the class remarks give
                    await Task.Yield();

                    Produced++;
                    OnRow?.Invoke(Produced);

                    yield return row;
                }
            }
            finally
            {
                // an awaited disposal reaches this; one dropped on the floor does not
                await Task.Yield();

                DisposedAsynchronously = true;
            }
        }

    }


    /// <summary>
    /// Drains an awaited sequence on the calling thread.
    /// </summary>
    /// <remarks>
    /// What a table whose rows only ever arrive asynchronously has to write for its <c>Scan</c>. It is here
    /// rather than reached out of the convention on purpose: <c>ClrSequences</c> is internal, an adapter
    /// outside this repository cannot call it, and a test table that did would be modelling something no
    /// real implementer can write.
    ///
    /// <para><b>It is not four lines, and the first version written here was.</b> That one blocked on
    /// <c>MoveNextAsync</c> directly and deadlocked
    /// <c>ShouldReadAnAsynchronousLeafSynchronouslyUnderASynchronizationContext</c> — thirty seconds and a
    /// hung thread. The operators of this convention await without <c>ConfigureAwait(false)</c>, so the
    /// continuation is promised to whatever context is current at the moment of suspension, which is inside
    /// <c>MoveNextAsync</c>'s synchronous phase and therefore before any wait begins. The context has to be
    /// nulled <em>before</em> the call, not around the wait. <c>ClrSequences.ToEnumerable</c> says the same
    /// thing and says it was measured; this is the second measurement.</para>
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
