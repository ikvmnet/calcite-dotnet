using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.DataCursor.Tests
{

    /// <summary>
    /// Holds <c>CallAsync</c> to passing the implementor's token parameter, by compiling a small plan of
    /// awaiting opens by hand and cancelling it.
    /// </summary>
    public class ClrDataCursorBuiltInMethodTests
    {

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
        /// A plan of three awaiting opens over a source the caller supplies, taking the open's token as
        /// the awaiting root does.
        /// </summary>
        static Func<IAsyncEnumerable<object[]>, CancellationToken, ValueTask<ClrDataCursor<object>>> Plan()
        {
            var implementor = new ClrDataCursorRelImplementor(
                new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.jdbc.JavaTypeFactoryImpl()),
                new java.util.HashMap());

            var source = Expression.Parameter(typeof(IAsyncEnumerable<object[]>), "source");

            // the leaf: a cursor over the sequence, enumerated under the open's token
            Expression plan = ClrDataCursorBuiltInMethod.CallAsync(implementor,
                ClrDataCursorBuiltInMethod.AsCursorAsync.MakeGenericMethod(typeof(object[])),
                source);

            // Slice0: a one column result is the value
            plan = ClrDataCursorBuiltInMethod.CallAsync(implementor,
                ClrDataCursorBuiltInMethod.Slice0Async.MakeGenericMethod(typeof(java.lang.Integer)),
                plan);

            // Select: a per row delegate, synchronous as every row level delegate of this convention is
            var row = Expression.Parameter(typeof(java.lang.Integer), "row");
            plan = ClrDataCursorBuiltInMethod.CallAsync(implementor,
                ClrDataCursorBuiltInMethod.SelectAsync.MakeGenericMethod(typeof(java.lang.Integer), typeof(object)),
                plan,
                Expression.Lambda<Func<java.lang.Integer, object>>(Expression.Convert(row, typeof(object)), row));

            // Calc keeping everything, so that what stops the cursor can only be the cancellation
            var kept = Expression.Parameter(typeof(object), "kept");
            plan = ClrDataCursorBuiltInMethod.CallAsync(implementor,
                ClrDataCursorBuiltInMethod.CalcAsync.MakeGenericMethod(typeof(object), typeof(object)),
                plan,
                Expression.Lambda<Func<object, bool>>(Expression.Constant(true), kept),
                Expression.Lambda<Func<object, object>>(kept, kept));

            return Expression.Lambda<Func<IAsyncEnumerable<object[]>, CancellationToken, ValueTask<ClrDataCursor<object>>>>(plan, source, implementor.CancellationToken).Compile();
        }

        /// <summary>
        /// Every awaiting open in the plan was passed the root's token parameter, and it is the open's
        /// token that the leaf sees.
        /// </summary>
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
        /// An open built without the token, or with something other than the implementor's parameter, is
        /// refused where it is built.
        /// </summary>
        [Fact]
        public void ShouldRefuseAnOpenWithoutItsToken()
        {
            var implementor = new ClrDataCursorRelImplementor(
                new org.apache.calcite.rex.RexBuilder(new org.apache.calcite.jdbc.JavaTypeFactoryImpl()),
                new java.util.HashMap());

            var source = Expression.Parameter(typeof(IAsyncEnumerable<object[]>), "source");

            var tooFew = () => ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.AsCursorAsync.MakeGenericMethod(typeof(object[])));
            tooFew.Should().Throw<InvalidOperationException>().WithMessage("*plus a token*");

            var notAwaiting = () => ClrDataCursorBuiltInMethod.CallAsync(implementor, ClrDataCursorBuiltInMethod.AsCursor.MakeGenericMethod(typeof(object[])), source);
            notAwaiting.Should().Throw<InvalidOperationException>();
        }

    }

}
