using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Requires that a cancellation token reaches the leaf of a composed plan without the plan carrying one.
    /// </summary>
    /// <remarks>
    /// The convention's whole cancellation design rests on one fact, and it is not obvious enough to assume:
    /// a plan is an expression tree, an expression tree cannot hold the token a caller has not supplied yet,
    /// and so every operator takes its token as an <c>[EnumeratorCancellation]</c> parameter that the tree
    /// passes as <c>default</c>. What makes that work is that the compiler's iterator substitutes the token
    /// given to <c>GetAsyncEnumerator</c> for a parameter that arrived as <c>default</c>.
    ///
    /// <para>If that substitution did not happen, or did not survive composition through
    /// <c>WithCancellation</c>, every one of these operators would ignore cancellation and nothing else in
    /// the convention would say so — a plan would simply run to completion after the caller had given up.
    /// So it is measured here, over a tree built the way a node builds one, rather than over a hand-written
    /// <c>await foreach</c> that would prove nothing about the plan.</para>
    /// </remarks>
    [TestClass]
    public class ClrAsyncEnumerableCancellationTests
    {

        /// <summary>
        /// A leaf that records the token it was enumerated with and never ends.
        /// </summary>
        /// <remarks>
        /// Endless on purpose: a source that finishes would let a test pass because the sequence ran out
        /// rather than because anything was cancelled.
        /// </remarks>
        sealed class Endless
        {

            /// <summary>
            /// Gets how many rows were produced before the sequence stopped.
            /// </summary>
            public int Produced { get; private set; }

            /// <summary>
            /// Gets whether the token the leaf saw was the caller's rather than the default one.
            /// </summary>
            public bool SawCancellableToken { get; private set; }

            /// <summary>
            /// Yields rows until cancelled.
            /// </summary>
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
        /// Builds <c>Where(Select(Slice0(source)))</c> the way a node builds a call, and compiles it.
        /// </summary>
        static Func<IAsyncEnumerable<object[]>, IAsyncEnumerable<object>> Plan()
        {
            var source = Expression.Parameter(typeof(IAsyncEnumerable<object[]>), "source");

            // Slice0: a one column result is the value
            Expression plan = ClrAsyncBuiltInMethod.Call(
                ClrAsyncBuiltInMethod.Slice0.MakeGenericMethod(typeof(java.lang.Integer)),
                source);

            // Select: a per row delegate, synchronous as every row level delegate of this convention is
            var row = Expression.Parameter(typeof(java.lang.Integer), "row");
            plan = ClrAsyncBuiltInMethod.Call(
                ClrAsyncBuiltInMethod.Select.MakeGenericMethod(typeof(java.lang.Integer), typeof(object)),
                plan,
                Expression.Lambda<Func<java.lang.Integer, object>>(Expression.Convert(row, typeof(object)), row));

            // Where: keeps everything, so that what stops the sequence can only be the cancellation
            var kept = Expression.Parameter(typeof(object), "kept");
            plan = ClrAsyncBuiltInMethod.Call(
                ClrAsyncBuiltInMethod.Where.MakeGenericMethod(typeof(object)),
                plan,
                Expression.Lambda<Func<object, bool>>(Expression.Constant(true), kept));

            return Expression.Lambda<Func<IAsyncEnumerable<object[]>, IAsyncEnumerable<object>>>(plan, source).Compile();
        }

        /// <summary>
        /// The token given at enumeration reaches the leaf, through two composed operators, of a plan that
        /// passes <c>default</c> at every call.
        /// </summary>
        [TestMethod]
        public async Task ShouldCarryTheCallersTokenToTheLeaf()
        {
            var leaf = new Endless();
            var rows = Plan()(leaf.Rows());

            using var cancellation = new CancellationTokenSource();

            var read = 0;
            var cancelled = false;

            try
            {
                await foreach (var row in rows.WithCancellation(cancellation.Token))
                {
                    if (++read == 3)
                        cancellation.Cancel();
                }
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue("the leaf must observe a token the plan never held");
            leaf.SawCancellableToken.Should().BeTrue("a token that cannot be cancelled is the default one, which means the substitution did not happen");
            leaf.Produced.Should().BeLessThan(10, "the leaf must stop producing once the caller has cancelled");
        }

        /// <summary>
        /// A token already cancelled stops the plan before the leaf produces anything.
        /// </summary>
        [TestMethod]
        public async Task ShouldRefuseToStartOnACancelledToken()
        {
            var leaf = new Endless();
            var rows = Plan()(leaf.Rows());

            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            var cancelled = false;

            try
            {
                await foreach (var row in rows.WithCancellation(cancellation.Token))
                    break;
            }
            catch (OperationCanceledException)
            {
                cancelled = true;
            }

            cancelled.Should().BeTrue();
            leaf.Produced.Should().Be(0);
        }

    }

}
