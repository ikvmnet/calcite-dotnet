using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.linq4j.function;

using JoinType = org.apache.calcite.linq4j.JoinType;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// What the operators do with the sequences they are given, against what <c>EnumerableDefaults</c> does.
    /// </summary>
    /// <remarks>
    /// None of this is a property of any row a query returns, so the differential suite is no oracle for it:
    /// how many rows an operator draws, whether it opens its input at all, when it drains it, and what it does
    /// with a join type or a null the planner never produces. Each test is written against Calcite's body.
    /// </remarks>
    [TestClass]
    public class ClrEnumerableDefaultsContractTests
    {

        /// <summary>
        /// A satisfied fetch has drawn one row more than it returned.
        /// </summary>
        /// <remarks>
        /// <c>take</c> is <c>takeWhile</c> over <c>n &lt; count</c>, and the enumerator has to draw a row
        /// before it can test it. LINQ's own <c>Take</c> stops on the count and draws exactly n, which is why
        /// delegating to it was a divergence.
        /// </remarks>
        [TestMethod]
        public void ShouldDrawOneRowMoreThanItTakes()
        {
            var drawn = 0;

            IEnumerable<int> Source()
            {
                for (var i = 0; i < 5; i++)
                {
                    drawn++;
                    yield return i;
                }
            }

            ClrEnumerableDefaults.Take(Source(), 2).ToList().Should().Equal(0, 1);
            drawn.Should().Be(3);
        }

        /// <summary>
        /// A fetch of no rows still opens its input and still draws a row from it.
        /// </summary>
        /// <remarks>
        /// Opening is not free. It is what runs a lazy collection spool's write-back and a repeat union's
        /// clean-up, so returning an empty sequence without touching the input skips work Calcite does.
        /// </remarks>
        [TestMethod]
        public void ShouldOpenItsInputForAFetchOfNoRows()
        {
            var drawn = 0;

            IEnumerable<int> Source()
            {
                for (var i = 0; i < 5; i++)
                {
                    drawn++;
                    yield return i;
                }
            }

            ClrEnumerableDefaults.Take(Source(), 0).ToList().Should().BeEmpty();
            drawn.Should().Be(1);
        }

        /// <summary>
        /// The asynchronous fetch draws the same rows as the synchronous one.
        /// </summary>
        [TestMethod]
        public async Task ShouldDrawOneRowMoreThanItTakesAsync()
        {
            var drawn = 0;

            async IAsyncEnumerable<int> Source()
            {
                for (var i = 0; i < 5; i++)
                {
                    drawn++;
                    await Task.Yield();
                    yield return i;
                }
            }

            var rows = new List<int>();
            await foreach (var row in ClrAsyncEnumerableDefaults.Take(Source(), 2))
                rows.Add(row);

            rows.Should().Equal(0, 1);
            drawn.Should().Be(3);
        }

        /// <summary>
        /// A semi join over an empty outer never touches its inner.
        /// </summary>
        /// <remarks>
        /// CALCITE-2909. Calcite holds the inner lookup behind a <c>Suppliers.memoize</c> and only asks for it
        /// while testing the first outer row, so an outer that has no rows never builds it. Building the
        /// lookup first reads a whole input for an answer that was already known.
        /// </remarks>
        [TestMethod]
        public void ShouldNotEnumerateTheInnerOfASemiJoinOverAnEmptyOuter()
        {
            var enumerations = 0;

            IEnumerable<int> Inner()
            {
                enumerations++;
                yield return 1;
            }

            ClrEnumerableDefaults.SemiJoin(System.Array.Empty<int>(), Inner(), x => x, x => x, null, false, null).ToList().Should().BeEmpty();
            enumerations.Should().Be(0);
        }

        /// <summary>
        /// A semi join over a non-empty outer builds its lookup once and no more.
        /// </summary>
        [TestMethod]
        public void ShouldEnumerateTheInnerOfASemiJoinOnce()
        {
            var enumerations = 0;

            IEnumerable<int> Inner()
            {
                enumerations++;
                yield return 1;
                yield return 2;
            }

            ClrEnumerableDefaults.SemiJoin(new[] { 1, 2, 3 }, Inner(), x => x, x => x, null, false, null).ToList().Should().Equal(1, 2);
            enumerations.Should().Be(1);
        }

        /// <summary>
        /// A correlated join refuses RIGHT and FULL where it is built, not where it is enumerated.
        /// </summary>
        /// <remarks>
        /// <c>correlateJoin</c> throws before it constructs the enumerable at all. Deferring the refusal into
        /// an iterator would let a plan be assembled and only fail once rows were pulled; falling through and
        /// inner-joining, which is what happened here, would let it not fail at all.
        /// </remarks>
        [TestMethod]
        public void ShouldRefuseARightOrFullCorrelateWhereItIsBuilt()
        {
            foreach (var joinType in new[] { JoinType.RIGHT, JoinType.FULL })
            {
                var act = () => ClrEnumerableDefaults.CorrelateJoin<int, int, int>(new[] { 1 }, _ => [1], (a, b) => a + b, joinType);

                act.Should().Throw<ArgumentException>().WithMessage("*" + joinType.name() + "*");
            }
        }

        /// <summary>
        /// A correlated function that answers null is read as a sequence of no rows.
        /// </summary>
        /// <remarks>
        /// <c>Linq4j.emptyEnumerable()</c>, which Calcite substitutes rather than dereferencing. A LEFT join
        /// therefore emits the outer row against null instead of throwing.
        /// </remarks>
        [TestMethod]
        public void ShouldReadANullCorrelatedSequenceAsEmpty()
        {
            ClrEnumerableDefaults.CorrelateJoin<int, string, string>(new[] { 1, 2 }, _ => null!, (a, b) => $"{a}:{b ?? "null"}", JoinType.LEFT)
                .ToList()
                .Should()
                .Equal("1:null", "2:null");
        }

        /// <summary>
        /// A grouped aggregate folds its whole input where it is called, before a row is pulled from it.
        /// </summary>
        /// <remarks>
        /// <c>groupBy_</c> drains the input into the map and only then returns a <c>LookupResultEnumerable</c>
        /// over a map that is already finished, so a second pass replays rather than folding again.
        /// </remarks>
        [TestMethod]
        public void ShouldFoldAGroupedAggregateWhereItIsCalled()
        {
            var drawn = 0;

            IEnumerable<int> Source()
            {
                foreach (var i in new[] { 1, 1, 2 })
                {
                    drawn++;
                    yield return i;
                }
            }

            var rows = ClrEnumerableDefaults.GroupBy<int, int, int>(Source(), x => x, new Count(), new Add(), new Result(), null);

            drawn.Should().Be(3, "the fold runs where groupBy is called, not where its result is enumerated");

            rows.ToList().Should().Equal(2, 1);

            // a second pass replays the map rather than folding again, which is what returning an enumerable
            // over finished state buys and what a yield in the method itself would have lost
            rows.ToList().Should().Equal(2, 1);
            drawn.Should().Be(3);
        }

        /// <summary>
        /// A row array holding a CLR-boxed value is not equal to one holding the Java-boxed same value, so a
        /// set operator keeps both.
        /// </summary>
        /// <remarks>
        /// The failure mode behind the SPI contract on <c>IClrScannableTable</c>, proved here rather than
        /// left as a possibility. <c>Functions.arrayComparer</c> compares element by element with
        /// <c>equals</c>, and a <see cref="int"/> boxed the CLR way and a <c>java.lang.Integer</c> are not
        /// equal in either direction. Their hashes do agree — IKVM maps <c>hashCode</c> onto
        /// <see cref="object.GetHashCode"/>, and <see cref="int"/>'s is the value, which is
        /// <c>Integer.hashCode</c> — so the two land in the same bucket, the iteration order of the set is
        /// undisturbed, and the only thing that breaks is the deduplication. That is why it would not show up
        /// as rows in the wrong order.
        ///
        /// <para><see cref="JavaValues.From"/> does not save this and is not meant to: a row is an
        /// <c>object[]</c>, which is not a value type, so it passes through whole and its elements are never
        /// looked at. Calcite requires the same of a <c>ScannableTable</c> and converts nothing either.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldNotDeduplicateARowBoxedTheClrWayAgainstOneBoxedTheJavaWay()
        {
            var comparer = org.apache.calcite.linq4j.function.Functions.arrayComparer();

            object[] clr = [1, "A"];
            object[] java = [global::java.lang.Integer.valueOf(1), "A"];

            comparer.equal(clr, java).Should().BeFalse("this is what the SPI contract exists to prevent");
            comparer.hashCode(clr).Should().Be(comparer.hashCode(java), "the hashes agree, so only dedup breaks");

            ClrEnumerableDefaults.Distinct([clr, java], comparer).Should().HaveCount(2);

            // and the same two rows, both boxed the way the contract requires, are one row
            object[] other = [global::java.lang.Integer.valueOf(1), "A"];
            ClrEnumerableDefaults.Distinct([java, other], comparer).Should().HaveCount(1);
        }

        /// <summary>
        /// An accumulator of one counter.
        /// </summary>
        sealed class Count : Function0
        {

            public object apply() => new int[1];

        }

        /// <summary>
        /// Adds a row to the counter.
        /// </summary>
        sealed class Add : Function2
        {

            public object apply(object accumulator, object row)
            {
                ((int[])accumulator)[0]++;
                return accumulator;
            }

        }

        /// <summary>
        /// Answers the counter.
        /// </summary>
        sealed class Result : Function2
        {

            public object apply(object key, object accumulator) => java.lang.Integer.valueOf(((int[])accumulator)[0]);

        }

    }

}
