using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Tree;

using FluentAssertions;

using java.lang;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.runtime;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tests.Tree
{

    [TestClass]
    public class ExpressionTranslatorTests
    {

        /// <summary>
        /// Translates an expression, compiles it, and runs it.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <returns></returns>
        static T Run<T>(J.Expression expression)
        {
            var translated = new ExpressionTranslator().Translate(expression);

            return Expression.Lambda<Func<T>>(JavaCast.To(translated, typeof(T))).Compile()();
        }

        /// <summary>
        /// Translates a block as a function body, compiles it, and runs it.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="block"></param>
        /// <returns></returns>
        static T RunBody<T>(J.BlockStatement block)
        {
            var translated = new ExpressionTranslator().TranslateBody(block, typeof(T));

            return Expression.Lambda<Func<T>>(translated).Compile()();
        }

        [TestMethod]
        public void ShouldTranslateConstant()
        {
            // linq4j holds the value of an int constant as an Integer, which the CLR will not accept for an int
            Run<int>(J.Expressions.constant(Integer.valueOf(42))).Should().Be(42);
            Run<string>(J.Expressions.constant("abc")).Should().Be("abc");
            Run<bool>(J.Expressions.constant(java.lang.Boolean.TRUE)).Should().BeTrue();
            Run<double>(J.Expressions.constant(java.lang.Double.valueOf(1.5))).Should().Be(1.5);
        }

        [TestMethod]
        public void ShouldTranslateNullConstant()
        {
            Run<object>(J.Expressions.constant(null)).Should().BeNull();
        }

        [TestMethod]
        public void ShouldPromoteNarrowOperandsToInt()
        {
            // Java promotes byte and short to int before it adds them, and the result is an int
            var e = J.Expressions.add(
                J.Expressions.constant(java.lang.Byte.valueOf((byte)1)),
                J.Expressions.constant(java.lang.Short.valueOf((short)2)));

            Run<int>(e).Should().Be(3);
        }

        [TestMethod]
        public void ShouldPromoteToTheWiderOperand()
        {
            var e = J.Expressions.add(
                J.Expressions.constant(Integer.valueOf(1)),
                J.Expressions.constant(java.lang.Double.valueOf(0.5)));

            Run<double>(e).Should().Be(1.5);
        }

        [TestMethod]
        public void ShouldBoxWhenConvertingToABoxClass()
        {
            // Expression.Convert cannot do this: java.lang.Integer is a class, not a boxed CLR int
            var e = J.Expressions.convert_(J.Expressions.constant(Integer.valueOf(7)), (Class)typeof(Integer));

            Run<Integer>(e).Should().Be(Integer.valueOf(7));
        }

        [TestMethod]
        public void ShouldUnboxWhenConvertingToAPrimitive()
        {
            var e = J.Expressions.convert_(
                J.Expressions.constant(Integer.valueOf(7), (Class)typeof(java.lang.Object)),
                Integer.TYPE);

            Run<int>(e).Should().Be(7);
        }

        [TestMethod]
        public void ShouldUnboxThroughTheBoxClassRatherThanUnboxAny()
        {
            // the value at runtime is a java.lang.Integer instance; a straight Convert would emit unbox.any and
            // demand a boxed CLR int, which is never what is there
            var e = J.Expressions.convert_(
                J.Expressions.constant(Integer.valueOf(3), (Class)typeof(java.lang.Object)),
                Integer.TYPE);

            Run<int>(e).Should().Be(3);
        }

        [TestMethod]
        public void ShouldSignExtendAByteBeingWidened()
        {
            // Java's byte is signed and the CLR byte it is stored in is not, so widening without going by way
            // of an sbyte turns -1 into 255
            var e = J.Expressions.convert_(
                J.Expressions.constant(java.lang.Byte.valueOf(unchecked((byte)-1))),
                Integer.TYPE);

            Run<int>(e).Should().Be(-1);
        }

        [TestMethod]
        public void ShouldSignExtendAByteBeingPromoted()
        {
            var e = J.Expressions.add(
                J.Expressions.constant(java.lang.Byte.valueOf(unchecked((byte)-1))),
                J.Expressions.constant(Integer.valueOf(0)));

            Run<int>(e).Should().Be(-1);
        }

        [TestMethod]
        public void ShouldTranslateEveryPrimitiveRoundTrip()
        {
            RoundTrip(java.lang.Boolean.TRUE, java.lang.Boolean.TYPE, (Class)typeof(java.lang.Boolean));
            RoundTrip(java.lang.Byte.valueOf((byte)1), java.lang.Byte.TYPE, (Class)typeof(java.lang.Byte));
            RoundTrip(Character.valueOf('x'), Character.TYPE, (Class)typeof(Character));
            RoundTrip(Short.valueOf((short)2), Short.TYPE, (Class)typeof(Short));
            RoundTrip(Integer.valueOf(3), Integer.TYPE, (Class)typeof(Integer));
            RoundTrip(Long.valueOf(4L), Long.TYPE, (Class)typeof(Long));
            RoundTrip(Float.valueOf(5f), Float.TYPE, (Class)typeof(Float));
            RoundTrip(java.lang.Double.valueOf(6d), java.lang.Double.TYPE, (Class)typeof(java.lang.Double));
        }

        /// <summary>
        /// Boxes a primitive constant and unboxes it again, which is the pair of conversions the CLR has neither of.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="primitive"></param>
        /// <param name="box"></param>
        static void RoundTrip(java.lang.Object value, Class primitive, Class box)
        {
            var e = J.Expressions.convert_(J.Expressions.convert_(J.Expressions.constant(value), box), primitive);

            var translated = new ExpressionTranslator().Translate(e);
            var result = Expression.Lambda<Func<object>>(Expression.Convert(translated, typeof(object))).Compile()();

            result.Should().NotBeNull();
            JavaCast.Unwrap(value, TypeResolver.FromClass(primitive)).Should().Be(result);
        }

        [TestMethod]
        public void ShouldTranslateCallToStaticMethod()
        {
            var e = J.Expressions.call(
                (Class)typeof(Utilities),
                "compare",
                J.Expressions.constant(Integer.valueOf(1)),
                J.Expressions.constant(Integer.valueOf(2)));

            Run<int>(e).Should().Be(-1);
        }

        [TestMethod]
        public void ShouldTranslateCallToMethodOfRemappedClass()
        {
            // String.toUpperCase is static on a helper and takes the receiver first, so what linq4j calls the
            // target has to move into argument zero
            var e = J.Expressions.call(
                J.Expressions.constant("abc"),
                ((Class)typeof(java.lang.String)).getDeclaredMethod("toUpperCase", []));

            Run<string>(e).Should().Be("ABC");
        }

        [TestMethod]
        public void ShouldTranslateTernary()
        {
            var e = J.Expressions.condition(
                J.Expressions.constant(java.lang.Boolean.TRUE),
                J.Expressions.constant(Integer.valueOf(1)),
                J.Expressions.constant(Integer.valueOf(2)));

            Run<int>(e).Should().Be(1);
        }

        [TestMethod]
        public void ShouldTranslateArrayCreationAndAccess()
        {
            var builder = new J.BlockBuilder();
            var array = builder.append("values", J.Expressions.newArrayBounds((Class)typeof(java.lang.Object), 1, J.Expressions.constant(Integer.valueOf(2))));
            builder.add(J.Expressions.statement(J.Expressions.assign(J.Expressions.arrayIndex(array, J.Expressions.constant(Integer.valueOf(0))), J.Expressions.constant("a"))));
            builder.add(J.Expressions.statement(J.Expressions.assign(J.Expressions.arrayIndex(array, J.Expressions.constant(Integer.valueOf(1))), J.Expressions.constant("b"))));
            builder.add(J.Expressions.return_(null, array));

            RunBody<object[]>(builder.toBlock()).Should().BeEquivalentTo(["a", "b"]);
        }

        [TestMethod]
        public void ShouldTranslateBlockWithDeclarationsAndReturn()
        {
            var builder = new J.BlockBuilder();
            var a = builder.append("a", J.Expressions.constant(Integer.valueOf(20)));
            var b = builder.append("b", J.Expressions.constant(Integer.valueOf(22)));
            builder.add(J.Expressions.return_(null, J.Expressions.add(a, b)));

            RunBody<int>(builder.toBlock()).Should().Be(42);
        }

        [TestMethod]
        public void ShouldTranslateEarlyReturn()
        {
            // the shape PhysType.generateComparator emits: compare, return if non-zero, fall through to zero
            var c = J.Expressions.parameter(Integer.TYPE, "c");
            var builder = new J.BlockBuilder();
            builder.add(J.Expressions.declare(0, c, J.Expressions.constant(Integer.valueOf(5))));
            builder.add(J.Expressions.ifThen(
                J.Expressions.notEqual(c, J.Expressions.constant(Integer.valueOf(0))),
                J.Expressions.return_(null, c)));
            builder.add(J.Expressions.return_(null, J.Expressions.constant(Integer.valueOf(0))));

            RunBody<int>(builder.toBlock()).Should().Be(5);
        }

        [TestMethod]
        public void ShouldTranslateForLoop()
        {
            var sum = J.Expressions.parameter(Integer.TYPE, "sum");
            var i = J.Expressions.parameter(Integer.TYPE, "i");

            var body = new J.BlockBuilder();
            body.add(J.Expressions.statement(J.Expressions.addAssign(sum, i)));

            var builder = new J.BlockBuilder();
            builder.add(J.Expressions.declare(0, sum, J.Expressions.constant(Integer.valueOf(0))));
            builder.add(
                J.Expressions.for_(
                    java.util.Collections.singletonList(J.Expressions.declare(0, i, J.Expressions.constant(Integer.valueOf(0)))),
                    J.Expressions.lessThan(i, J.Expressions.constant(Integer.valueOf(5))),
                    J.Expressions.postIncrementAssign(i),
                    body.toBlock()));
            builder.add(J.Expressions.return_(null, sum));

            RunBody<int>(builder.toBlock()).Should().Be(10);
        }

        [TestMethod]
        public void ShouldTranslateLambda()
        {
            var v = J.Expressions.parameter(Integer.TYPE, "v");
            var e = J.Expressions.lambda(
                J.Expressions.block(J.Expressions.return_(null, J.Expressions.multiply(v, J.Expressions.constant(Integer.valueOf(2))))),
                v);

            // a lambda linq4j declared as a Function1 is one, so the delegate is asked for back
            var translated = SamAdapters.Unwrap(new ExpressionTranslator().Translate(e))!;

            translated.Should().NotBeNull();
            ((Func<int, int>)translated.Compile())(21).Should().Be(42);
        }

        [TestMethod]
        public void ShouldTranslateAnonymousClassAsLambda()
        {
            // what PhysType returns for a comparator, and what an expression tree cannot declare
            var v0 = J.Expressions.parameter(Integer.TYPE, "v0");
            var v1 = J.Expressions.parameter(Integer.TYPE, "v1");

            var body = new J.BlockBuilder();
            body.add(J.Expressions.return_(null, J.Expressions.subtract(v0, v1)));

            var e = J.Expressions.new_(
                (Class)typeof(java.util.Comparator),
                java.util.Collections.emptyList(),
                java.util.Collections.singletonList(
                    J.Expressions.methodDecl(
                        java.lang.reflect.Modifier.PUBLIC,
                        Integer.TYPE,
                        "compare",
                        java.util.Arrays.asList([v0, v1]),
                        body.toBlock())));

            var translated = new ExpressionTranslator().Translate(e);

            // the lambda is wrapped back into the interface the class was declared against, because the same
            // operator takes a comparator that never was an anonymous class
            translated.Type.Should().Be(typeof(java.util.Comparator));

            var comparator = Expression.Lambda<Func<java.util.Comparator>>(translated).Compile()();

            // the adapter casts each argument to the type the declaration gave its parameters, exactly as the
            // erased compare(Object, Object) of the class it stands for would have
            comparator.compare(5, 3).Should().Be(2);
        }

        [TestMethod]
        public void ShouldBindAnExternalParameter()
        {
            var row = J.Expressions.parameter((Class)typeof(object[]), "row");
            var target = Expression.Parameter(typeof(object[]), "row");

            var translator = new ExpressionTranslator();
            translator.Bind(row, target);

            var translated = translator.Translate(J.Expressions.arrayIndex(row, J.Expressions.constant(Integer.valueOf(1))));

            Expression.Lambda<Func<object[], object>>(translated, target).Compile()(["a", "b"]).Should().Be("b");
        }

    }

}
