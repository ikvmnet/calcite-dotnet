using System;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.core;
using org.apache.calcite.runtime;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The aggregate implementors this project adds for a column of type ANY.
    /// </summary>
    /// <remarks>
    /// Not a port. Calcite cannot aggregate over ANY either, and the two ways it fails are both measured in
    /// <c>ClrEnumerableDifferentialTests</c>: <c>MinMaxImplementor</c> names <c>SqlFunctions.lesser</c> and
    /// asks <c>Types.lookupMethod</c> to resolve it against the accumulator's static type, which for ANY is
    /// <c>Object</c> and has no overload; <c>SumImplementor</c> writes a binary <c>+</c>, which Janino refuses
    /// over two <c>Object</c>s. Both fail before a row is read, so <c>EnumerableConvention</c> forms a plan for
    /// MIN, MAX, SUM and AVG over an ANY column and then cannot implement it. There is nothing upstream to
    /// reproduce, which is why these are an addition rather than a defect, and why the differential tests
    /// cannot use Calcite as the oracle for them. <see cref="ClrAggImpState"/> is what carries one to the
    /// node that writes with it.
    ///
    /// <para>What the addition reaches for is Calcite's own, though. A value of type ANY carries its type at
    /// run time and nowhere else, so the operation has to be chosen per value, and <c>SqlFunctions</c> already
    /// does that for the scalar case: <c>RexImpTable.BinaryImplementor</c> answers <c>ltAny</c>,
    /// <c>plusAny</c>, <c>divideAny</c> and the rest whenever an operand is ANY. These implementors call that
    /// same runtime from the accumulator, so a minimum over ANY orders its values the way <c>&lt;</c> over ANY
    /// orders them and a sum adds them the way <c>+</c> over ANY adds them — mixed numeric types included,
    /// which is the case a schema of ANY columns is usually there for. It also fixes the answer's type:
    /// <c>plusAny</c> converts both operands to <c>java.math.BigDecimal</c>, so SUM and AVG over ANY are
    /// BigDecimal however the column's values were boxed.</para>
    ///
    /// <para>AVG needs no implementor of its own. <c>RexImpTable</c> has none for it in any type — a program
    /// runs <c>AGGREGATE_REDUCE_FUNCTIONS</c> first and the call becomes <c>$SUM0</c> over <c>COUNT</c> — so
    /// once <c>$SUM0</c> accumulates, the division left behind is a <c>RexCall</c> and
    /// <c>BinaryImplementor</c>'s ANY path already answers it.</para>
    /// </remarks>
    static class ClrAnyAggImplementors
    {

        /// <summary>
        /// Returns the implementor to write a call of type ANY with, or null where there is none.
        /// </summary>
        /// <param name="call"></param>
        /// <returns></returns>
        /// <remarks>
        /// The call's own type rather than its argument's, because that is what breaks Calcite's implementors:
        /// <c>StrictAggImplementor.getNotNullState</c> takes the accumulator's type from
        /// <c>info.returnType()</c>, and an ANY return type is what makes it <c>Object</c>. MIN, MAX and SUM
        /// all return the type they read, so for these three the two are the same type anyway.
        ///
        /// <para>COUNT is not here and needs nothing: it never looks at the value. Nor are the aggregates
        /// whose implementors already work over an <c>Object</c> — <c>COLLECT</c>, <c>MODE</c>,
        /// <c>ARG_MIN</c>, <c>ARG_MAX</c>, <c>LISTAGG</c> and <c>JSON_ARRAYAGG</c> all run over ANY untouched,
        /// in Calcite as well. What is left out and could be added is <c>BIT_AND</c> and <c>BIT_OR</c>, which
        /// have no <c>Object</c> overload in <c>SqlFunctions</c> and no <c>*Any</c> counterpart to reach for
        /// either.</para>
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

                // ANY_VALUE is MinMaxImplementor upstream too, and takes its MAX branch: the implementor asks
                // whether the kind is MIN and this is not it
                nameof(SqlKind.ANY_VALUE) => new ClrAnyMinMaxImplementor(false),

                nameof(SqlKind.SUM) or nameof(SqlKind.SUM0) => new ClrAnySumImplementor(),
                _ => null,
            };
        }

        /// <summary>
        /// Implements MIN and MAX over a value whose type is only known at run time.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable.MinMaxImplementor</c> with its comparison changed. That one calls
        /// <c>SqlFunctions.lesser</c>, which compares with <c>Comparable.compareTo</c> and so requires the two
        /// values to be of one class — true of every column whose type Calcite can name, and not of an ANY
        /// column, where <c>compareTo</c> on an <c>Integer</c> and a <c>Double</c> throws. So the comparison
        /// here is <c>ltAny</c> and <c>gtAny</c>, which is what <c>BinaryImplementor</c> answers for a scalar
        /// <c>&lt;</c> over ANY: same class compares directly, two numbers compare as BigDecimal, and anything
        /// else is refused with Calcite's own message.
        ///
        /// <para>Those two take no null, where <c>lesser</c> does, so the empty accumulator is tested here
        /// instead. It is only ever null when no row has been folded in yet:
        /// <c>StrictAggImplementor.implementAdd</c> guards the whole block on the arguments being non-null, so
        /// a null the column holds never reaches this. The reset is Calcite's own and needs no override —
        /// <c>Primitive.of(Object)</c> is null, so the accumulator starts null.</para>
        /// </remarks>
        sealed class ClrAnyMinMaxImplementor(bool min) : StrictAggImplementor
        {

            /// <inheritdoc />
            protected override void implementNotNullAdd(AggContext info, AggAddContext add)
            {
                var acc = (J.Expression)add.accumulator().get(0);

                // named once, because it is read three times below and is an arbitrary expression rather than
                // a field: MinMaxImplementor reads its argument once and needs no such declaration
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
        /// Implements SUM and $SUM0 over a value whose type is only known at run time.
        /// </summary>
        /// <remarks>
        /// <c>RexImpTable.SumImplementor</c>, whose <c>Expressions.add</c> becomes a call to
        /// <c>SqlFunctions.plusAny</c> — the addition Calcite already performs for a scalar <c>+</c> whose
        /// operand is ANY, which converts both sides to <c>java.math.BigDecimal</c> and refuses anything that
        /// is not a number.
        ///
        /// <para>The reset is <c>SumImplementor</c>'s, copied: the accumulator starts at the <c>int</c>
        /// constant zero, which the translator boxes the Java way on its way into an <c>Object</c> slot, so
        /// <c>plusAny</c> is handed a <c>java.lang.Integer</c> rather than a CLR box it would refuse. An empty
        /// set is still <c>StrictAggImplementor</c>'s business: SUM is nullable and answers null, $SUM0 is not
        /// and answers the zero.</para>
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
            /// <c>Expressions.constant(0)</c>, which is an <c>int</c> rather than a box.
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
        /// Returns the <c>SqlFunctions</c> method of a name taking two <c>Object</c>s.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        /// <remarks>
        /// Named as a method rather than written into the tree as a string. That is half of the fix for MIN
        /// and MAX: <c>Expressions.call(Type, String, ...)</c> resolves the overload through
        /// <c>Types.lookupMethod</c> against the arguments' static types, which is exactly the resolution that
        /// fails when those types are <c>Object</c>.
        /// </remarks>
        static java.lang.reflect.Method AnyOfTwo(string name)
        {
            return ((java.lang.Class)typeof(SqlFunctions)).getMethod(name, [(java.lang.Class)typeof(java.lang.Object), (java.lang.Class)typeof(java.lang.Object)])
                ?? throw new NotSupportedException($"Calcite's runtime has no SqlFunctions.{name}(Object, Object).");
        }

    }

}
