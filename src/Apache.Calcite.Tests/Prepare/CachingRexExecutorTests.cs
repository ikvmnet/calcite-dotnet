using System.Collections.Generic;

using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.jdbc;
using org.apache.calcite.rex;
using org.apache.calcite.runtime;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Extensions.Prepare.Tests
{

    /// <summary>
    /// Tests of <see cref="CachingRexExecutor"/>, alone and as the executor <see cref="ClrPrepare"/> plans with.
    /// </summary>
    /// <remarks>
    /// Compilations are counted through <c>Hook.EXPRESSION_REDUCER</c>, which <c>RexExecutable.reduce</c> runs
    /// once for each class <c>RexExecutorImpl</c> compiles and executes, with the generated source.
    /// </remarks>
    public class CachingRexExecutorTests
    {

        /// <summary>
        /// Records the generated source of every reduction <c>Hook.EXPRESSION_REDUCER</c> announces on the
        /// current thread while it is open.
        /// </summary>
        /// <remarks>
        /// IKVM does not project a Java default method onto a CLR class that implements the interface, so
        /// <c>andThen</c> is implemented here.
        /// </remarks>
        sealed class Reductions : java.util.function.Consumer, System.IDisposable
        {

            /// <summary>
            /// Two handlers run in order, as <c>Consumer.andThen</c> returns in Java.
            /// </summary>
            /// <param name="first">The handler run first.</param>
            /// <param name="then">The handler run second.</param>
            sealed class Composed(java.util.function.Consumer first, java.util.function.Consumer then) : java.util.function.Consumer
            {

                /// <inheritdoc />
                public void accept(object value)
                {
                    first.accept(value);
                    then.accept(value);
                }

                /// <inheritdoc />
                public java.util.function.Consumer andThen(java.util.function.Consumer after)
                {
                    return new Composed(this, after);
                }

            }

            readonly List<string> _sources = [];
            readonly Hook.Closeable _handle;

            /// <summary>
            /// Initializes a new instance and starts recording.
            /// </summary>
            public Reductions()
            {
                _handle = Hook.EXPRESSION_REDUCER.addThread(this);
            }

            /// <summary>
            /// Gets the generated source of each reduction so far, in order.
            /// </summary>
            public IReadOnlyList<string> Sources => _sources;

            /// <inheritdoc />
            public void accept(object value)
            {
                _sources.Add((string)((org.apache.calcite.util.Pair)value).getKey());
            }

            /// <inheritdoc />
            public java.util.function.Consumer andThen(java.util.function.Consumer after)
            {
                return new Composed(this, after);
            }

            /// <summary>
            /// Stops recording.
            /// </summary>
            public void Dispose()
            {
                _handle.close();
            }

        }

        static readonly JavaTypeFactoryImpl TypeFactory = new();

        static readonly RexBuilder RexBuilder = new(TypeFactory);

        /// <summary>
        /// Returns a new executor over a <c>RexExecutorImpl</c> with an empty data context.
        /// </summary>
        /// <returns>The executor.</returns>
        static CachingRexExecutor NewExecutor()
        {
            return new CachingRexExecutor(new RexExecutorImpl(DataContexts.EMPTY));
        }

        /// <summary>
        /// Reduces a list of expressions and returns what the executor answered.
        /// </summary>
        /// <param name="executor">The executor.</param>
        /// <param name="constExps">The constant expressions.</param>
        /// <returns>The reduced expressions, one per input.</returns>
        static java.util.List Reduce(RexExecutor executor, params RexNode[] constExps)
        {
            var reducedValues = new java.util.ArrayList();
            executor.reduce(RexBuilder, java.util.Arrays.asList(constExps), reducedValues);
            return reducedValues;
        }

        /// <summary>
        /// Returns <c>a + b</c> over two integer literals, as a new instance on every call.
        /// </summary>
        /// <param name="a">The left operand.</param>
        /// <param name="b">The right operand.</param>
        /// <returns>The call.</returns>
        static RexNode Plus(int a, int b)
        {
            return RexBuilder.makeCall(SqlStdOperatorTable.PLUS, RexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(a)), RexBuilder.makeExactLiteral(java.math.BigDecimal.valueOf(b)));
        }

        /// <summary>
        /// A list equal to one reduced before is answered without compiling again, with the same values.
        /// </summary>
        [Fact]
        public void A_repeated_list_should_be_compiled_once()
        {
            var executor = NewExecutor();
            using var reductions = new Reductions();

            var first = Reduce(executor, Plus(1, 2), Plus(3, 4));
            var second = Reduce(executor, Plus(1, 2), Plus(3, 4));
            var third = Reduce(executor, Plus(1, 2), Plus(3, 4));

            reductions.Sources.Should().HaveCount(1);
            first.ToString().Should().Be("[3, 7]");
            second.ToString().Should().Be("[3, 7]");
            third.ToString().Should().Be("[3, 7]");
        }

        /// <summary>
        /// A list that differs in one expression, or in the order of its expressions, is reduced on its own.
        /// </summary>
        [Fact]
        public void A_different_list_should_be_compiled_again()
        {
            var executor = NewExecutor();
            using var reductions = new Reductions();

            Reduce(executor, Plus(1, 2), Plus(3, 4)).ToString().Should().Be("[3, 7]");
            Reduce(executor, Plus(3, 4), Plus(1, 2)).ToString().Should().Be("[7, 3]");
            Reduce(executor, Plus(1, 2)).ToString().Should().Be("[3]");

            reductions.Sources.Should().HaveCount(3);
        }

        /// <summary>
        /// Two calls with the same digest and different types are different keys: a call's digest leaves out
        /// its own type, and the reduced literal carries it.
        /// </summary>
        [Fact]
        public void A_list_of_the_same_digest_and_another_type_should_be_compiled_again()
        {
            var executor = NewExecutor();
            using var reductions = new Reductions();

            var integer = Plus(1, 2);
            var bigint = RexBuilder.makeCall(TypeFactory.createSqlType(SqlTypeName.BIGINT), SqlStdOperatorTable.PLUS, ((RexCall)integer).getOperands());
            integer.ToString().Should().Be(bigint.ToString());

            var integerValue = (RexNode)Reduce(executor, integer).get(0);
            var bigintValue = (RexNode)Reduce(executor, bigint).get(0);

            reductions.Sources.Should().HaveCount(2);
            integerValue.getType().getSqlTypeName().name().Should().Be(nameof(SqlTypeName.INTEGER));
            bigintValue.getType().getSqlTypeName().name().Should().Be(nameof(SqlTypeName.BIGINT));
        }

        /// <summary>
        /// A list one of whose expressions fails to evaluate is returned unreduced, as <c>RexExecutorImpl</c>
        /// returns it, on every call: the caller's own instances, not those of the call that was remembered.
        /// </summary>
        [Fact]
        public void A_failed_reduction_should_return_the_callers_expressions_without_compiling_again()
        {
            var executor = NewExecutor();
            using var reductions = new Reductions();

            var one = RexBuilder.makeExactLiteral(java.math.BigDecimal.ONE);
            var zero = RexBuilder.makeExactLiteral(java.math.BigDecimal.ZERO);
            var first = RexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, one, zero);
            var second = RexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, one, zero);

            var firstSum = Plus(1, 2);
            var secondSum = Plus(1, 2);

            var firstReduced = Reduce(executor, first, firstSum);
            var secondReduced = Reduce(executor, second, secondSum);

            reductions.Sources.Should().HaveCount(1);
            firstReduced.get(0).Should().BeSameAs(first);
            firstReduced.get(1).Should().BeSameAs(firstSum);
            secondReduced.get(0).Should().BeSameAs(second);
            secondReduced.get(1).Should().BeSameAs(secondSum);
        }

        /// <summary>
        /// Planning a statement whose constants the reduction rules meet on several filters compiles them once.
        /// </summary>
        /// <remarks>
        /// <c>ReduceExpressionsRule.pushPredicateIntoCase</c> turns <c>CASE ... END = 'T'</c> into a <c>CASE</c>
        /// whose branches compare each branch's literal with <c>'T'</c>, and those comparisons are the constants
        /// reduced. The planner derives several filters of that condition over different inputs as it explores the
        /// joins, and each reduces the same comparisons, though each is pruned once it is reduced.
        /// </remarks>
        [Fact]
        public void Planning_should_compile_constants_met_on_several_filters_once()
        {
            const string sql = """
                SELECT *
                FROM (
                    SELECT s.ID, CASE WHEN s.AMOUNT > 10 THEN 'T' WHEN t.AMOUNT > 5 THEN 'U' END AS D
                    FROM SALES s
                    JOIN SALES t ON s.ID = t.ID
                    JOIN SALES u ON t.ID = u.ID)
                WHERE D = 'T'
                """;

            var sources = ClrPrepareFixture.WithContext(sql, (context, _) =>
            {
                using var reductions = new Reductions();
                new ClrPrepareImpl().PrepareSql(context, IClrPrepare.Query.Of(sql), typeof(object[]), -1);
                return reductions.Sources;
            });

            sources.Should().HaveCount(1);
        }

    }

}
