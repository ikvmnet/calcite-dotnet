using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using java.lang;

using org.apache.calcite.runtime;
using org.apache.calcite.runtime.rtti;

using Xunit;

using JavaTypeFactoryImpl = org.apache.calcite.jdbc.JavaTypeFactoryImpl;
using J = org.apache.calcite.linq4j.tree;
using RelDataTypeFactory = org.apache.calcite.rel.type.RelDataTypeFactory;
using SqlTypeName = org.apache.calcite.sql.type.SqlTypeName;

namespace Apache.Calcite.Extensions.Linq4j.Tree.Tests
{

    public class LixToClrTranslatorTests
    {

        /// <summary>
        /// Translates an expression, converts it to <typeparamref name="T"/>, compiles it, and runs it.
        /// </summary>
        /// <typeparam name="T">The type the result is converted to.</typeparam>
        /// <param name="expression">A linq4j expression with no free parameters.</param>
        /// <returns>The expression's value.</returns>
        static T Run<T>(J.Expression expression)
        {
            var translated = new LixToClrTranslator().Translate(expression);

            return Expression.Lambda<Func<T>>(ClrEnumUtils.Convert(translated, typeof(T))).Compile()();
        }

        /// <summary>
        /// Translates a block as the body of a function returning <typeparamref name="T"/>, compiles it, and
        /// runs it.
        /// </summary>
        /// <typeparam name="T">The function's return type.</typeparam>
        /// <param name="block">A linq4j block that ends in a return of <typeparamref name="T"/>.</param>
        /// <returns>The value the block returns.</returns>
        static T RunBody<T>(J.BlockStatement block)
        {
            var translated = new LixToClrTranslator().TranslateBody(block, typeof(T));

            return Expression.Lambda<Func<T>>(translated).Compile()();
        }

        [Fact]
        public void ShouldTranslateConstant()
        {
            // linq4j holds the value of an int constant as an Integer, which the CLR will not accept for an int
            Run<int>(J.Expressions.constant(Integer.valueOf(42))).Should().Be(42);
            Run<string>(J.Expressions.constant("abc")).Should().Be("abc");
            Run<bool>(J.Expressions.constant(java.lang.Boolean.TRUE)).Should().BeTrue();
            Run<double>(J.Expressions.constant(java.lang.Double.valueOf(1.5))).Should().Be(1.5);
        }

        [Fact]
        public void ShouldTranslateNullConstant()
        {
            Run<object>(J.Expressions.constant(null)).Should().BeNull();
        }

        [Fact]
        public void ShouldPromoteNarrowOperandsToInt()
        {
            // Java promotes byte and short to int before adding them, so the result is an int
            var e = J.Expressions.add(
                J.Expressions.constant(java.lang.Byte.valueOf((byte)1)),
                J.Expressions.constant(java.lang.Short.valueOf((short)2)));

            Run<int>(e).Should().Be(3);
        }

        [Fact]
        public void ShouldPromoteToTheWiderOperand()
        {
            var e = J.Expressions.add(
                J.Expressions.constant(Integer.valueOf(1)),
                J.Expressions.constant(java.lang.Double.valueOf(0.5)));

            Run<double>(e).Should().Be(1.5);
        }

        [Fact]
        public void ShouldBoxWhenConvertingToABoxClass()
        {
            // Expression.Convert cannot do this: java.lang.Integer is a class, not a boxed CLR int
            var e = J.Expressions.convert_(J.Expressions.constant(Integer.valueOf(7)), (Class)typeof(Integer));

            Run<Integer>(e).Should().Be(Integer.valueOf(7));
        }

        [Fact]
        public void ShouldUnboxWhenConvertingToAPrimitive()
        {
            var e = J.Expressions.convert_(
                J.Expressions.constant(Integer.valueOf(7), (Class)typeof(java.lang.Object)),
                Integer.TYPE);

            Run<int>(e).Should().Be(7);
        }

        [Fact]
        public void ShouldUnboxThroughTheBoxClassRatherThanUnboxAny()
        {
            // the value at run time is a java.lang.Integer; Expression.Convert would emit unbox.any, which
            // requires a boxed CLR int
            var e = J.Expressions.convert_(
                J.Expressions.constant(Integer.valueOf(3), (Class)typeof(java.lang.Object)),
                Integer.TYPE);

            Run<int>(e).Should().Be(3);
        }

        [Fact]
        public void ShouldSignExtendAByteBeingWidened()
        {
            // Java's byte is signed and the CLR byte IKVM maps it to is not, so widening has to go by way of
            // sbyte or -1 becomes 255
            var e = J.Expressions.convert_(
                J.Expressions.constant(java.lang.Byte.valueOf(unchecked((byte)-1))),
                Integer.TYPE);

            Run<int>(e).Should().Be(-1);
        }

        [Fact]
        public void ShouldSignExtendAByteBeingPromoted()
        {
            var e = J.Expressions.add(
                J.Expressions.constant(java.lang.Byte.valueOf(unchecked((byte)-1))),
                J.Expressions.constant(Integer.valueOf(0)));

            Run<int>(e).Should().Be(-1);
        }

        [Fact]
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
        /// Converts a primitive constant to its Java box class and back, neither of which is a CLR conversion.
        /// </summary>
        /// <param name="value">The constant, as a Java box.</param>
        /// <param name="primitive">The Java primitive class the value is converted back to.</param>
        /// <param name="box">The Java box class the value is converted to first.</param>
        static void RoundTrip(java.lang.Object value, Class primitive, Class box)
        {
            var e = J.Expressions.convert_(J.Expressions.convert_(J.Expressions.constant(value), box), primitive);

            var translated = new LixToClrTranslator().Translate(e);
            var result = Expression.Lambda<Func<object>>(Expression.Convert(translated, typeof(object))).Compile()();

            result.Should().NotBeNull();
            JavaValues.Unwrap(value, ClrTypes.FromClass(primitive)).Should().Be(result);
        }

        [Fact]
        public void ShouldTranslateCallToStaticMethod()
        {
            var e = J.Expressions.call(
                (Class)typeof(Utilities),
                "compare",
                J.Expressions.constant(Integer.valueOf(1)),
                J.Expressions.constant(Integer.valueOf(2)));

            Run<int>(e).Should().Be(-1);
        }

        [Fact]
        public void ShouldTranslateCallToMethodOfRemappedClass()
        {
            // IKVM implements String.toUpperCase as a static helper taking the receiver first, so linq4j's
            // target becomes the first argument
            var e = J.Expressions.call(
                J.Expressions.constant("abc"),
                ((Class)typeof(java.lang.String)).getDeclaredMethod("toUpperCase", []));

            Run<string>(e).Should().Be("ABC");
        }

        /// <summary>
        /// A Java method with no CLR method of its name: IKVM exposes <c>String.length()</c> as a property, so
        /// <see cref="ClrTypes.TryResolve"/> finds nothing and the call goes through a delegate over the Java
        /// method.
        /// </summary>
        [Fact]
        public void ShouldTranslateCallToMethodWithNoClrMethod()
        {
            var method = ((Class)typeof(java.lang.String)).getDeclaredMethod("length", []);
            ClrTypes.TryResolve(method).Should().BeNull();

            Run<int>(J.Expressions.call(J.Expressions.constant("abc"), method)).Should().Be(3);
        }

        /// <summary>
        /// A method of <c>java.lang.Object</c>, which IKVM maps onto <see cref="object"/>, called through a
        /// delegate; its primitive result comes back as an <see cref="int"/>, not a <c>java.lang.Integer</c>.
        /// </summary>
        [Fact]
        public void ShouldTranslateCallToMethodOfObject()
        {
            var method = ((Class)typeof(java.lang.Object)).getDeclaredMethod("hashCode", []);
            ClrTypes.TryResolve(method).Should().BeNull();

            var value = new java.util.ArrayList();
            value.add("a");

            Run<int>(J.Expressions.call(J.Expressions.constant(value), method)).Should().Be(value.hashCode());
        }

        /// <summary>
        /// A call through a delegate with an argument, which has to follow the receiver.
        /// </summary>
        [Fact]
        public void ShouldTranslateCallToMethodWithNoClrMethodAndAnArgument()
        {
            var method = ((Class)typeof(java.lang.String)).getDeclaredMethod("charAt", [Integer.TYPE]);
            ClrTypes.TryResolve(method).Should().BeNull();

            Run<char>(J.Expressions.call(J.Expressions.constant("abc"), method, J.Expressions.constant(Integer.valueOf(1)))).Should().Be('b');
        }

        [Fact]
        public void ShouldTranslateTernary()
        {
            var e = J.Expressions.condition(
                J.Expressions.constant(java.lang.Boolean.TRUE),
                J.Expressions.constant(Integer.valueOf(1)),
                J.Expressions.constant(Integer.valueOf(2)));

            Run<int>(e).Should().Be(1);
        }

        [Fact]
        public void ShouldTranslateArrayCreationAndAccess()
        {
            var builder = new J.BlockBuilder();
            var array = builder.append("values", J.Expressions.newArrayBounds((Class)typeof(java.lang.Object), 1, J.Expressions.constant(Integer.valueOf(2))));
            builder.add(J.Expressions.statement(J.Expressions.assign(J.Expressions.arrayIndex(array, J.Expressions.constant(Integer.valueOf(0))), J.Expressions.constant("a"))));
            builder.add(J.Expressions.statement(J.Expressions.assign(J.Expressions.arrayIndex(array, J.Expressions.constant(Integer.valueOf(1))), J.Expressions.constant("b"))));
            builder.add(J.Expressions.return_(null, array));

            RunBody<object[]>(builder.toBlock()).Should().BeEquivalentTo(["a", "b"]);
        }

        [Fact]
        public void ShouldTranslateBlockWithDeclarationsAndReturn()
        {
            var builder = new J.BlockBuilder();
            var a = builder.append("a", J.Expressions.constant(Integer.valueOf(20)));
            var b = builder.append("b", J.Expressions.constant(Integer.valueOf(22)));
            builder.add(J.Expressions.return_(null, J.Expressions.add(a, b)));

            RunBody<int>(builder.toBlock()).Should().Be(42);
        }

        [Fact]
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

        [Fact]
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

        [Fact]
        public void ShouldTranslateLambda()
        {
            var v = J.Expressions.parameter(Integer.TYPE, "v");
            var e = J.Expressions.lambda(
                J.Expressions.block(J.Expressions.return_(null, J.Expressions.multiply(v, J.Expressions.constant(Integer.valueOf(2))))),
                v);

            // a lambda linq4j declares as a Function1 is wrapped as one, so the delegate is unwrapped here
            var translated = AnonymousClasses.Unwrap(new LixToClrTranslator().Translate(e))!;

            translated.Should().NotBeNull();
            ((Func<int, int>)translated.Compile())(21).Should().Be(42);
        }

        [Fact]
        public void ShouldTranslateAnonymousClassAsLambda()
        {
            // the anonymous class PhysType returns for a comparator, which an expression tree cannot declare
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

            var translated = new LixToClrTranslator().Translate(e);

            // the lambda is wrapped in the interface the class implements, because the operators that take a
            // comparator take the interface
            translated.Type.Should().Be(typeof(java.util.Comparator));

            var comparator = Expression.Lambda<Func<java.util.Comparator>>(translated).Compile()();

            // the adapter converts each argument to its declared parameter type, as the erased
            // compare(Object, Object) of the Java class would
            comparator.compare(5, 3).Should().Be(2);
        }

        [Fact]
        public void ShouldBindAnExternalParameter()
        {
            var row = J.Expressions.parameter((Class)typeof(object[]), "row");
            var target = Expression.Parameter(typeof(object[]), "row");

            var translator = new LixToClrTranslator();
            translator.Bind(row, target);

            var translated = translator.Translate(J.Expressions.arrayIndex(row, J.Expressions.constant(Integer.valueOf(1))));

            Expression.Lambda<Func<object[], object>>(translated, target).Compile()(["a", "b"]).Should().Be("b");
        }

        /// <summary>
        /// A Java varargs call passes its trailing arguments individually and the Java compiler collects them
        /// into an array; an expression tree has no such step, so the translator does it.
        /// </summary>
        /// <remarks>
        /// These use the generator that produces such a call. <c>CAST(x AS VARIANT)</c> compiles the payload's
        /// type into the plan through <c>RuntimeTypeInformation.createExpression</c>, and the constructor it
        /// uses for a parameterised type is varargs.
        /// </remarks>
        [Fact]
        public void ShouldTranslateVarArgsConstructorGivenOneArgument()
        {
            var factory = new JavaTypeFactoryImpl();
            var type = factory.createArrayType(factory.createSqlType(SqlTypeName.INTEGER), -1);

            // new GenericSqlTypeRtti(ARRAY, new BasicSqlTypeRtti(INTEGER)): two arguments against two
            // parameters, the second a RuntimeTypeInformation[] the single argument has to be wrapped in
            var rtti = Run<GenericSqlTypeRtti>(RuntimeTypeInformation.createExpression(type));

            rtti.getArgumentCount().Should().Be(1);
            rtti.getTypeArgument(0).getTypeName().Should().BeSameAs(RuntimeTypeInformation.RuntimeSqlTypeName.INTEGER);
        }

        [Fact]
        public void ShouldTranslateVarArgsConstructorGivenSeveralArguments()
        {
            var factory = new JavaTypeFactoryImpl();
            var type = factory.createMapType(factory.createSqlType(SqlTypeName.VARCHAR), factory.createSqlType(SqlTypeName.INTEGER));

            // new GenericSqlTypeRtti(MAP, key, value): three arguments against two parameters, so no
            // constructor matches by arity
            var rtti = Run<GenericSqlTypeRtti>(RuntimeTypeInformation.createExpression(type));

            rtti.getArgumentCount().Should().Be(2);
            rtti.getTypeArgument(0).getTypeName().Should().BeSameAs(RuntimeTypeInformation.RuntimeSqlTypeName.VARCHAR);
            rtti.getTypeArgument(1).getTypeName().Should().BeSameAs(RuntimeTypeInformation.RuntimeSqlTypeName.INTEGER);
        }

        /// <summary>
        /// The trailing arguments are converted to the array's element type, as a row's RTTI needs: its
        /// arguments are <c>AbstractMap.SimpleEntry</c> and the array is of <c>Map.Entry</c>.
        /// </summary>
        [Fact]
        public void ShouldConvertTheElementsOfAVarArgsArray()
        {
            var factory = new JavaTypeFactoryImpl();
            var type = new RelDataTypeFactory.Builder(factory).add("A", SqlTypeName.INTEGER).add("B", SqlTypeName.VARCHAR).build();

            var rtti = Run<RowSqlTypeRtti>(RuntimeTypeInformation.createExpression(type));

            rtti.size().Should().Be(2);
        }

    }

}
