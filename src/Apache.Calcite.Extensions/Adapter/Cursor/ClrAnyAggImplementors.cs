using System;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.core;
using org.apache.calcite.runtime;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Aggregate implementors for MIN, MAX, ANY_VALUE, SUM and $SUM0 over a value of type ANY.
    /// </summary>
    /// <remarks>
    /// An addition, not a port: Calcite cannot implement these over ANY. <c>MinMaxImplementor</c> resolves
    /// <c>SqlFunctions.lesser</c> against the accumulator's static type, which for ANY is <c>Object</c> and has
    /// no overload, and <c>SumImplementor</c> writes a binary <c>+</c> over two <c>Object</c>s, which Janino
    /// rejects. Calcite therefore cannot serve as the oracle for these in the differential tests.
    ///
    /// <para>The implementors use the same runtime Calcite uses for scalar operators over ANY
    /// (<c>RexImpTable.BinaryImplementor</c> answers <c>ltAny</c>, <c>gtAny</c> and <c>plusAny</c>), so an
    /// aggregate orders and adds its values the way <c>&lt;</c> and <c>+</c> over ANY do, mixed numeric types
    /// included. <c>plusAny</c> converts both operands to <c>java.math.BigDecimal</c>, so SUM over ANY is a
    /// <c>BigDecimal</c> whatever the column's values are.</para>
    ///
    /// <para>AVG needs no implementor: <c>AGGREGATE_REDUCE_FUNCTIONS</c> rewrites it to <c>$SUM0</c> over
    /// <c>COUNT</c>, and the remaining division is a <c>RexCall</c> that <c>BinaryImplementor</c>'s ANY path
    /// handles.</para>
    /// </remarks>
    static class ClrAnyAggImplementors
    {

        /// <summary>
        /// Returns the implementor for a call whose result type is ANY, or null where Calcite's own is used.
        /// </summary>
        /// <param name="call">The aggregate call.</param>
        /// <returns>The replacement implementor, or null.</returns>
        /// <remarks>
        /// Tests the call's result type rather than its argument's, because <c>StrictAggImplementor</c> takes the
        /// accumulator type from <c>info.returnType()</c>. MIN, MAX and SUM return the type they read, so the two
        /// agree for these.
        ///
        /// <para>COUNT never reads the value, and <c>COLLECT</c>, <c>MODE</c>, <c>ARG_MIN</c>, <c>ARG_MAX</c>,
        /// <c>LISTAGG</c> and <c>JSON_ARRAYAGG</c> already work over <c>Object</c>, so none of them is replaced.
        /// <c>BIT_AND</c> and <c>BIT_OR</c> are not supported over ANY: <c>SqlFunctions</c> has neither an
        /// <c>Object</c> overload nor an <c>*Any</c> counterpart for them.</para>
        /// </remarks>
        internal static AggImplementor? For(AggregateCall call)
        {
            if (call.getType().getSqlTypeName() != SqlTypeName.ANY)
                return null;

            // by name, because a Java enum's ordinals are not stable across versions
            return call.getAggregation().getKind().name() switch
            {
                nameof(SqlKind.MIN) => new ClrAnyMinMaxImplementor(true),
                nameof(SqlKind.MAX) => new ClrAnyMinMaxImplementor(false),

                // Calcite implements ANY_VALUE with MinMaxImplementor, which takes its MAX branch for any kind
                // other than MIN
                nameof(SqlKind.ANY_VALUE) => new ClrAnyMinMaxImplementor(false),

                nameof(SqlKind.SUM) or nameof(SqlKind.SUM0) => new ClrAnySumImplementor(),
                _ => null,
            };
        }

        /// <summary>
        /// Implements MIN and MAX over a value whose type is known only at run time.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable.MinMaxImplementor</c> with the comparison replaced. <c>SqlFunctions.lesser</c> uses
        /// <c>Comparable.compareTo</c>, which throws for two values of different classes; <c>ltAny</c> and
        /// <c>gtAny</c> compare values of one class directly, compare two numbers as <c>BigDecimal</c>, and
        /// reject anything else with Calcite's own message.
        ///
        /// <para>Those two do not accept null, so the empty accumulator is tested here. The accumulator is null
        /// only before the first row: the reset inherited from <c>StrictAggImplementor</c> starts it at null, and
        /// <c>implementAdd</c> skips null arguments.</para>
        /// </remarks>
        sealed class ClrAnyMinMaxImplementor(bool min) : StrictAggImplementor
        {

            /// <inheritdoc />
            protected override void implementNotNullAdd(AggContext info, AggAddContext add)
            {
                var acc = (J.Expression)add.accumulator().get(0);

                // declared once because it is read three times below and may be an arbitrary expression
                var arg = add.currentBlock().append("arg", (J.Expression)add.arguments().get(0));

                accAdvance(add, acc,
                    J.Expressions.condition(
                        J.Expressions.equal(acc, J.Expressions.constant(null)),
                        arg,
                        J.Expressions.condition(
                            J.Expressions.call(min ? LtAny : GtAny, arg, acc),
                            arg,
                            acc)));
            }

        }

        /// <summary>
        /// Implements SUM and $SUM0 over a value whose type is known only at run time.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable.SumImplementor</c> with its addition replaced by <c>SqlFunctions.plusAny</c>, which
        /// converts both operands to <c>java.math.BigDecimal</c> and rejects anything that is not a number.
        ///
        /// <para>The reset matches <c>SumImplementor</c>'s: the accumulator starts at a Java <c>Integer</c> zero,
        /// which <c>plusAny</c> accepts where it would reject a CLR box. An empty set is handled by
        /// <c>StrictAggImplementor</c>: SUM answers null and $SUM0 answers the zero.</para>
        /// </remarks>
        sealed class ClrAnySumImplementor : StrictAggImplementor
        {

            /// <inheritdoc />
            protected override void implementNotNullReset(AggContext info, AggResetContext reset)
            {
                reset.currentBlock().add(
                    J.Expressions.statement(
                        J.Expressions.assign((J.Expression)reset.accumulator().get(0), Zero)));
            }

            /// <inheritdoc />
            protected override void implementNotNullAdd(AggContext info, AggAddContext add)
            {
                var acc = (J.Expression)add.accumulator().get(0);
                var arg = (J.Expression)add.arguments().get(0);

                accAdvance(add, acc, J.Expressions.call(PlusAny, acc, arg));
            }

            /// <summary>
            /// The constant zero, as a <c>java.lang.Integer</c>.
            /// </summary>
            static readonly J.ConstantExpression Zero = J.Expressions.constant(java.lang.Integer.valueOf(0));

        }

        /// <summary>
        /// <c>SqlFunctions.ltAny(Object, Object)</c>.
        /// </summary>
        static readonly java.lang.reflect.Method LtAny = AnyOfTwo("ltAny");

        /// <summary>
        /// <c>SqlFunctions.gtAny(Object, Object)</c>.
        /// </summary>
        static readonly java.lang.reflect.Method GtAny = AnyOfTwo("gtAny");

        /// <summary>
        /// <c>SqlFunctions.plusAny(Object, Object)</c>.
        /// </summary>
        static readonly java.lang.reflect.Method PlusAny = AnyOfTwo("plusAny");

        /// <summary>
        /// Returns the <c>SqlFunctions</c> method of the given name that takes two <c>Object</c>s.
        /// </summary>
        /// <param name="name">The method name.</param>
        /// <returns>The method.</returns>
        /// <exception cref="NotSupportedException">The method does not exist.</exception>
        /// <remarks>
        /// The method is resolved here rather than named in the tree, because
        /// <c>Expressions.call(Type, String, ...)</c> resolves the overload through <c>Types.lookupMethod</c>
        /// against the arguments' static types, and that fails when those types are <c>Object</c>.
        /// </remarks>
        static java.lang.reflect.Method AnyOfTwo(string name)
        {
            return ((java.lang.Class)typeof(SqlFunctions)).getMethod(name, [(java.lang.Class)typeof(java.lang.Object), (java.lang.Class)typeof(java.lang.Object)])
                ?? throw new NotSupportedException($"Calcite's runtime has no SqlFunctions.{name}(Object, Object).");
        }

    }

}
