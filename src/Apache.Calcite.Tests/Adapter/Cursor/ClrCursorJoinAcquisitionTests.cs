using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

using JoinType = org.apache.calcite.linq4j.JoinType;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests that the joins of <c>ClrCursorDefaults</c> acquire, read and release their inputs in the order
    /// their linq4j originals do.
    /// </summary>
    /// <remarks>
    /// The order does not change the rows a query returns, so a differential test cannot see it. These call the
    /// operators directly and record every open, advance and disposal of each input, through both opens.
    /// </remarks>
    public class ClrCursorJoinAcquisitionTests
    {

        /// <summary>
        /// An input that records each open of it, and each advance and disposal of the cursors it opens.
        /// </summary>
        /// <param name="name">The name the input's events are recorded under.</param>
        /// <param name="log">Where the events are recorded.</param>
        /// <param name="rows">The rows each opened cursor returns.</param>
        /// <param name="fail">Whether each advance throws.</param>
        sealed class Input(string name, List<string> log, int[] rows, bool fail = false)
        {

            /// <summary>
            /// The last cursor opened, or null before the first open.
            /// </summary>
            public Rows? Opened;

            /// <summary>
            /// Opens the input synchronously.
            /// </summary>
            /// <returns>A cursor over the input's rows.</returns>
            public IClrCursor<int> Open()
            {
                log.Add(name + ".open");
                return Opened = new Rows(name, log, rows, fail);
            }

            /// <summary>
            /// Opens the input with await.
            /// </summary>
            /// <param name="cancellationToken">Unused.</param>
            /// <returns>A cursor over the input's rows.</returns>
            public ValueTask<IClrCursor<int>> OpenAsync(CancellationToken cancellationToken)
            {
                return new ValueTask<IClrCursor<int>>(Open());
            }

        }

        /// <summary>
        /// A cursor over a fixed list of rows that records its advances and its disposal.
        /// </summary>
        /// <param name="name">The name the events are recorded under.</param>
        /// <param name="log">Where the events are recorded.</param>
        /// <param name="rows">The rows the cursor returns, in order.</param>
        /// <param name="fail">Whether each advance throws.</param>
        sealed class Rows(string name, List<string> log, int[] rows, bool fail) : ClrCursor<int>
        {

            int index = -1;

            /// <summary>
            /// Whether the cursor has been disposed.
            /// </summary>
            public bool Disposed;

            /// <inheritdoc />
            public override int Current => rows[index];

            /// <inheritdoc />
            public override bool Read()
            {
                log.Add(name + ".read");

                if (fail)
                    throw new InvalidOperationException(name + " failed");

                if (index + 1 >= rows.Length)
                    return false;

                index++;
                return true;
            }

            /// <inheritdoc />
            public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
            {
                return new ValueTask<bool>(Read());
            }

            /// <inheritdoc />
            public override void Dispose()
            {
                Disposed = true;
                log.Add(name + ".dispose");
            }

        }

        static java.lang.Integer Key(int row) => java.lang.Integer.valueOf(row);

        static string Pair(int left, int right) => left + "/" + right;

        /// <summary>
        /// A hash join drains and disposes its build input before it opens its probe input, as
        /// <c>hashEquiJoin_</c> and <c>hashJoinWithPredicate_</c> build the lookup before
        /// <c>outer.enumerator()</c>.
        /// </summary>
        /// <param name="withPredicate">Whether the join has a predicate beyond its keys, which selects
        /// <c>hashJoinWithPredicate_</c>.</param>
        /// <returns>A task that completes when the test has run.</returns>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task HashJoinShouldDrainTheBuildInputBeforeOpeningTheProbeInput(bool withPredicate)
        {
            Func<int, int, bool>? predicate = withPredicate ? (l, r) => true : null;
            string[] expected = ["right.open", "right.read", "right.read", "right.read", "right.dispose", "left.open"];

            var log = new List<string>();
            var left = new Input("left", log, [1]);
            var right = new Input("right", log, [1, 2]);
            ClrCursorDefaults.HashJoin<int, int, java.lang.Integer, string>(left.Open, right.Open(), Key, Key, Pair, null, false, false, predicate);
            log.Should().Equal(expected);

            log = [];
            left = new Input("left", log, [1]);
            right = new Input("right", log, [1, 2]);
            await ClrCursorDefaults.HashJoinAsync<int, int, java.lang.Integer, string>(left.OpenAsync, right.OpenAsync(CancellationToken.None), Key, Key, Pair, null, false, false, predicate, CancellationToken.None);
            log.Should().Equal(expected);
        }

        /// <summary>
        /// A hash join whose probe input fails to open has already released its build input, because the
        /// drain finished first.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task HashJoinShouldHaveReleasedTheBuildInputWhenTheProbeInputFailsToOpen()
        {
            var log = new List<string>();
            var right = new Input("right", log, [1, 2]);
            var act = () => ClrCursorDefaults.HashJoin<int, int, java.lang.Integer, string>(() => throw new InvalidOperationException("left"), right.Open(), Key, Key, Pair, null, false, false, null);
            act.Should().Throw<InvalidOperationException>();
            right.Opened!.Disposed.Should().BeTrue();

            log = [];
            right = new Input("right", log, [1, 2]);
            var actAsync = async () => await ClrCursorDefaults.HashJoinAsync<int, int, java.lang.Integer, string>(_ => throw new InvalidOperationException("left"), right.OpenAsync(CancellationToken.None), Key, Key, Pair, null, false, false, null, CancellationToken.None);
            await actAsync.Should().ThrowAsync<InvalidOperationException>();
            right.Opened!.Disposed.Should().BeTrue();
        }

        /// <summary>
        /// A mark hash join drains and disposes its build input before it opens its probe input, as
        /// <c>leftMarkHashJoin</c> builds its hash table before <c>outer.enumerator()</c>.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task LeftMarkHashJoinShouldDrainTheBuildInputBeforeOpeningTheProbeInput()
        {
            string[] expected = ["right.open", "right.read", "right.read", "right.read", "right.dispose", "left.open"];

            var log = new List<string>();
            var left = new Input("left", log, [1]);
            var right = new Input("right", log, [1, 2]);
            ClrCursorDefaults.LeftMarkHashJoin<int, int, java.lang.Integer, java.lang.Integer, string>(
                left.Open, right.Open(), Key, Key, null, null, true, (l, m) => l + "/" + m, null, null, null, (l, r) => java.lang.Boolean.valueOf(l == r));
            log.Should().Equal(expected);

            log = [];
            left = new Input("left", log, [1]);
            right = new Input("right", log, [1, 2]);
            await ClrCursorDefaults.LeftMarkHashJoinAsync<int, int, java.lang.Integer, java.lang.Integer, string>(
                left.OpenAsync, right.OpenAsync(CancellationToken.None), Key, Key, null, null, true, (l, m) => l + "/" + m, null, null, null, (l, r) => java.lang.Boolean.valueOf(l == r), CancellationToken.None);
            log.Should().Equal(expected);
        }

        /// <summary>
        /// A nested loop join that generates nulls on the left reads and disposes its inner input before it
        /// opens its outer input, as <c>nestedLoopJoinAsList</c> calls <c>inner.toList()</c> before
        /// <c>outer.enumerator()</c>.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task RightNestedLoopJoinShouldReadTheInnerBeforeOpeningTheOuter()
        {
            string[] expected = ["right.open", "right.read", "right.read", "right.read", "right.dispose", "left.open", "left.read", "left.read", "left.dispose"];

            var log = new List<string>();
            var left = new Input("left", log, [1]);
            var right = new Input("right", log, [1, 2]);
            ClrCursorDefaults.NestedLoopJoin<int, int, string>(left.Open, right.Open, right.OpenAsync, (l, r) => Pair(l, r), (l, r) => l == r, JoinType.RIGHT);
            log.Should().Equal(expected);

            log = [];
            left = new Input("left", log, [1]);
            right = new Input("right", log, [1, 2]);
            await ClrCursorDefaults.NestedLoopJoinAsync<int, int, string>(left.OpenAsync, right.Open, right.OpenAsync, (l, r) => Pair(l, r), (l, r) => l == r, JoinType.RIGHT, CancellationToken.None);
            log.Should().Equal(expected);
        }

        /// <summary>
        /// A merge join opens its right input only at its first step of it, after the left input's first row,
        /// as <c>MergeJoinEnumerator</c>'s <c>getRightEnumerator</c> does.
        /// </summary>
        /// <param name="joinType">The join type, which selects either branch of <c>start()</c>.</param>
        /// <returns>A task that completes when the test has run.</returns>
        [Theory]
        [InlineData(nameof(JoinType.INNER))]
        [InlineData(nameof(JoinType.LEFT))]
        public async Task MergeJoinShouldOpenTheRightInputAfterTheLeftInputsFirstRow(string joinType)
        {
            var type = JoinType.valueOf(joinType);
            string[] expected = ["left.open", "left.read", "right.open", "right.read"];

            var log = new List<string>();
            var left = new Input("left", log, [1]);
            var right = new Input("right", log, [1]);
            ClrCursorDefaults.MergeJoin<int, int, java.lang.Integer, string>(left.Open(), right.Open, Key, Key, null, (l, r) => Pair(l, r), type, null, null);
            log.GetRange(0, expected.Length).Should().Equal(expected);

            log = [];
            left = new Input("left", log, [1]);
            right = new Input("right", log, [1]);
            await ClrCursorDefaults.MergeJoinAsync<int, int, java.lang.Integer, string>(left.OpenAsync(CancellationToken.None), right.OpenAsync, Key, Key, null, (l, r) => Pair(l, r), type, null, null, CancellationToken.None);
            log.GetRange(0, expected.Length).Should().Equal(expected);
        }

        /// <summary>
        /// A merge join whose left input is empty never opens its right input, because <c>start()</c> finishes
        /// before its first <c>rightMoveNext</c>; disposing the join disposes only the left input.
        /// </summary>
        /// <param name="joinType">The join type, which selects either branch of <c>start()</c>.</param>
        /// <returns>A task that completes when the test has run.</returns>
        [Theory]
        [InlineData(nameof(JoinType.INNER))]
        [InlineData(nameof(JoinType.LEFT))]
        public async Task MergeJoinShouldNotOpenTheRightInputWhenTheLeftInputIsEmpty(string joinType)
        {
            var type = JoinType.valueOf(joinType);
            string[] expected = ["left.open", "left.read", "left.dispose"];

            var log = new List<string>();
            var left = new Input("left", log, []);
            var right = new Input("right", log, [1]);
            var join = ClrCursorDefaults.MergeJoin<int, int, java.lang.Integer, string>(left.Open(), right.Open, Key, Key, null, (l, r) => Pair(l, r), type, null, null);
            join.Read().Should().BeFalse();
            join.Dispose();
            log.Should().Equal(expected);

            log = [];
            left = new Input("left", log, []);
            right = new Input("right", log, [1]);
            join = await ClrCursorDefaults.MergeJoinAsync<int, int, java.lang.Integer, string>(left.OpenAsync(CancellationToken.None), right.OpenAsync, Key, Key, null, (l, r) => Pair(l, r), type, null, null, CancellationToken.None);
            (await join.ReadAsync(CancellationToken.None)).Should().BeFalse();
            await join.DisposeAsync();
            log.Should().Equal(expected);
        }

        /// <summary>
        /// A merge join whose positioning throws leaves the inputs it opened undisposed, as an exception from
        /// <c>MergeJoinEnumerator</c>'s constructor does: a Calcite defect, reproduced.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task MergeJoinShouldNotDisposeItsInputsWhenThePositioningThrows()
        {
            var log = new List<string>();
            var left = new Input("left", log, [1]);
            var right = new Input("right", log, [1], fail: true);
            var act = () => ClrCursorDefaults.MergeJoin<int, int, java.lang.Integer, string>(left.Open(), right.Open, Key, Key, null, (l, r) => Pair(l, r), JoinType.INNER, null, null);
            act.Should().Throw<InvalidOperationException>();
            left.Opened!.Disposed.Should().BeFalse();
            right.Opened!.Disposed.Should().BeFalse();

            log = [];
            left = new Input("left", log, [1]);
            right = new Input("right", log, [1], fail: true);
            var actAsync = async () => await ClrCursorDefaults.MergeJoinAsync<int, int, java.lang.Integer, string>(left.OpenAsync(CancellationToken.None), right.OpenAsync, Key, Key, null, (l, r) => Pair(l, r), JoinType.INNER, null, null, CancellationToken.None);
            await actAsync.Should().ThrowAsync<InvalidOperationException>();
            left.Opened!.Disposed.Should().BeFalse();
            right.Opened!.Disposed.Should().BeFalse();
        }

    }

}
