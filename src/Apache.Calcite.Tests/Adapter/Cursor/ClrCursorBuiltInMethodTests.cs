using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests that <c>ClrCursorBuiltInMethod.CallAsync</c> passes the implementor's cancellation token parameter
    /// to each awaiting open, using a small plan of awaiting opens built by hand and then cancelled.
    /// </summary>
    public class ClrCursorBuiltInMethodTests
    {

        /// <summary>
        /// An endless source that counts the rows it produces and records whether it was given a token that
        /// can be cancelled.
        /// </summary>
        sealed class Endless
        {

            public int Produced { get; private set; }

            public bool SawCancellableToken { get; private set; }

            public async IAsyncEnumerable<object[]> Rows([EnumeratorCancellation] CancellationToken cancellationToken = default)
            {
                SawCancellableToken = cancellationToken.CanBeCanceled;

                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await Task.Yield();

                    Produced++;
                    yield return [java.lang.Integer.valueOf(Produced)];
                }
            }

        }

        /// <summary>
        /// Compiles a plan of awaiting opens over a source the caller supplies, taking the implementor's token
        /// parameter as the awaiting root does.
        /// </summary>
        /// <returns>A compiled delegate that takes the source and the open's token and opens the plan over them.</returns>
        static Func<IAsyncEnumerable<object[]>, CancellationToken, ValueTask<IClrCursor<object>>> Plan()
        {
            var implementor = new ClrCursorRelImplementor(
                new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.jdbc.JavaTypeFactoryImpl()),
                new java.util.HashMap());

            var source = Expression.Parameter(typeof(IAsyncEnumerable<object[]>), "source");

            // the leaf: a cursor over the sequence, enumerated under the open's token
            Expression plan = ClrCursorBuiltInMethod.CallAsync(implementor,
                ClrCursorBuiltInMethod.AsCursorAsync.MakeGenericMethod(typeof(object[])),
                source);

            // Slice0: a one column result is the value
            plan = ClrCursorBuiltInMethod.CallAsync(implementor,
                ClrCursorBuiltInMethod.Slice0Async.MakeGenericMethod(typeof(java.lang.Integer)),
                plan);

            // Select: the per-row delegate is synchronous, as every row-level delegate of this convention is
            var row = Expression.Parameter(typeof(java.lang.Integer), "row");
            plan = ClrCursorBuiltInMethod.CallAsync(implementor,
                ClrCursorBuiltInMethod.SelectAsync.MakeGenericMethod(typeof(java.lang.Integer), typeof(object)),
                plan,
                Expression.Lambda<Func<java.lang.Integer, object>>(Expression.Convert(row, typeof(object)), row));

            // Calc keeping every row, so that only cancellation can stop the cursor
            var kept = Expression.Parameter(typeof(object), "kept");
            plan = ClrCursorBuiltInMethod.CallAsync(implementor,
                ClrCursorBuiltInMethod.CalcAsync.MakeGenericMethod(typeof(object), typeof(object)),
                plan,
                Expression.Lambda<Func<object, bool>>(Expression.Constant(true), kept),
                Expression.Lambda<Func<object, object>>(kept, kept));

            return Expression.Lambda<Func<IAsyncEnumerable<object[]>, CancellationToken, ValueTask<IClrCursor<object>>>>(plan, source, implementor.CancellationToken).Compile();
        }

        /// <summary>
        /// The token given to the plan's open reaches the leaf through every awaiting open, so cancelling it
        /// stops the leaf.
        /// </summary>
        /// <returns>A task that completes when the test has run.</returns>
        [Fact]
        public async Task ShouldCarryTheOpensTokenToTheLeaf()
        {
            var leaf = new Endless();
            using var cancellation = new CancellationTokenSource();

            var read = 0;
            var cancelled = false;

            try
            {
                await using var cursor = await Plan()(leaf.Rows(), cancellation.Token);
                while (await cursor.ReadAsync(cancellation.Token))
                {
                    if (++read == 3)
                        cancellation.Cancel();
                }
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue("the leaf must observe the token the open was given");
            leaf.SawCancellableToken.Should().BeTrue("a token that cannot be cancelled is the default one, which means the parameter was not passed");
            leaf.Produced.Should().BeLessThan(10, "the leaf must stop producing once the caller has cancelled");
        }

        [Fact]
        public async Task ShouldRefuseToStartOnACancelledToken()
        {
            var leaf = new Endless();

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            var cancelled = false;

            try
            {
                await using var cursor = await Plan()(leaf.Rows(), cancellation.Token);
                await cursor.ReadAsync(cancellation.Token);
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue();
            leaf.Produced.Should().Be(0);
        }

        /// <summary>
        /// <c>CallAsync</c> refuses, when the call is built, an awaiting open given too few arguments and a
        /// method that is not an awaiting open.
        /// </summary>
        [Fact]
        public void ShouldRefuseAnOpenWithoutItsToken()
        {
            var implementor = new ClrCursorRelImplementor(
                new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.jdbc.JavaTypeFactoryImpl()),
                new java.util.HashMap());

            var source = Expression.Parameter(typeof(IAsyncEnumerable<object[]>), "source");

            var tooFew = () => ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.AsCursorAsync.MakeGenericMethod(typeof(object[])));
            tooFew.Should().Throw<InvalidOperationException>().WithMessage("*plus a token*");

            var notAwaiting = () => ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.AsCursor.MakeGenericMethod(typeof(object[])), source);
            notAwaiting.Should().Throw<InvalidOperationException>();
        }

    }

}
