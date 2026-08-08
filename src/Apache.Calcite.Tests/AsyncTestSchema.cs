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
        public static readonly object[][] Sales =
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
        public static readonly object[][] Sorted =
        [
            [java.lang.Integer.valueOf(1), "A"],
            [java.lang.Integer.valueOf(2), "B"],
            [java.lang.Integer.valueOf(2), "C"],
            [java.lang.Integer.valueOf(4), "D"],
        ];

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
        /// Returns the SORTED row type.
        /// </summary>
        public static RelDataType SortedRowType(RelDataTypeFactory typeFactory)
        {
            return typeFactory.builder()
                .add("K", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("V", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .build();
        }

    }

    /// <summary>
    /// A table of the synchronous convention over one of the shared row sets.
    /// </summary>
    sealed class SyncRowsTable(object[][] rows, System.Func<RelDataTypeFactory, RelDataType> rowType, bool sorted) : AbstractTable, ScannableTable
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
    sealed class AsyncRowsTable(object[][] rows, System.Func<RelDataTypeFactory, RelDataType> rowType, bool sorted) : AbstractTable, IClrAsyncScannableTable
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
        public IAsyncEnumerable<object[]> ScanAsync(DataContext root) => Rows();

        async IAsyncEnumerable<object[]> Rows([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            SawCancellableToken = cancellationToken.CanBeCanceled;

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

    }

}
