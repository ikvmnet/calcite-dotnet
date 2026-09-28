using System;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using IKVM.Runtime;

using java.lang;

using org.apache.calcite.util;

using Xunit;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    public class JavaDelegatesTests
    {

        static java.lang.reflect.Method Method(Type declaring, string name, params Class[] parameters)
        {
            return ((Class)declaring).getMethod(name, parameters);
        }

        [Fact]
        public void ShouldCallInstanceMethod()
        {
            var m = Method(typeof(java.util.ArrayList), "size");
            var d = (MH<object, int>)JavaDelegates.FromMethod(m);

            var list = new java.util.ArrayList();
            list.add("a");
            list.add("b");

            d(list).Should().Be(2);
        }

        [Fact]
        public void ShouldCallInstanceMethodWithArgument()
        {
            var m = Method(typeof(StringBuilder), "append", (Class)typeof(java.lang.String));
            var d = (MH<object, object, object>)JavaDelegates.FromMethod(m);

            var sb = new StringBuilder("a");
            d(sb, "b").Should().BeSameAs(sb);
            sb.toString().Should().Be("ab");
        }

        [Fact]
        public void ShouldCallStaticMethod()
        {
            var m = Method(typeof(Integer), "parseInt", (Class)typeof(java.lang.String));
            var d = (MH<object, int>)JavaDelegates.FromMethod(m);

            d("42").Should().Be(42);
        }

        [Fact]
        public void ShouldCallVoidMethod()
        {
            var m = Method(typeof(java.util.ArrayList), "clear");
            var d = (MHV<object>)JavaDelegates.FromMethod(m);

            var list = new java.util.ArrayList();
            list.add("a");
            d(list);

            list.size().Should().Be(0);
        }

        [Fact]
        public void ShouldCallConstructor()
        {
            var c = ((Class)typeof(java.util.ArrayList)).getConstructor([]);
            var d = (MH<object>)JavaDelegates.FromMethod(c);

            d().Should().BeOfType<java.util.ArrayList>();
        }

        /// <summary>
        /// IKVM maps <c>java.lang.Object</c> onto <see cref="object"/>, which has no CLR method of the Java
        /// method's name, so only a delegate over the Java method can call it.
        /// </summary>
        [Fact]
        public void ShouldCallMethodOnRemappedType()
        {
            var m = Method(typeof(java.lang.Object), "hashCode");
            var d = (MH<object, int>)JavaDelegates.FromMethod(m);

            var o = new object();
            d(o).Should().Be(java.lang.System.identityHashCode(o));
        }

        /// <summary>
        /// IKVM maps <c>java.lang.String</c> onto <see cref="string"/> and implements <c>toUpperCase</c> as a
        /// static helper taking the receiver first; the delegate still takes the receiver as its first argument.
        /// </summary>
        [Fact]
        public void ShouldCallMethodMovedToAHelperClass()
        {
            var m = Method(typeof(java.lang.String), "toUpperCase");
            var d = (MH<object, object>)JavaDelegates.FromMethod(m);

            d("abc").Should().Be("ABC");
        }

        /// <summary>
        /// <c>java.lang.Comparable</c> is an IKVM ghost interface that the CLR does not see on a string, so a
        /// cast to it throws; the delegate calls the method without casting.
        /// </summary>
        [Fact]
        public void ShouldCallMethodOnGhostInterface()
        {
            var m = Method(typeof(java.lang.Comparable), "compareTo", (Class)typeof(java.lang.Object));
            var d = (MH<object, object, int>)JavaDelegates.FromMethod(m);

            d("a", "b").Should().BeNegative();
        }

        /// <summary>
        /// A method from Calcite's <c>BuiltInMethod</c> table; its primitive parameter stays a CLR
        /// <see cref="int"/> in the delegate's signature rather than being erased to <see cref="object"/>.
        /// </summary>
        [Fact]
        public void ShouldCallACalciteBuiltInMethod()
        {
            var d = (MH<object, int, object>)JavaDelegates.FromMethod(BuiltInMethod.LIST_GET.method);

            var list = new java.util.ArrayList();
            list.add("a");
            list.add("b");

            d(list, 1).Should().Be("b");
        }

        /// <summary>
        /// Java access checks apply: <c>java.lang.Runtime</c>'s private constructor is refused until the caller
        /// calls <c>setAccessible(true)</c> on it.
        /// </summary>
        [Fact]
        public void ShouldRefuseAnInaccessibleMemberUntilMarkedAccessible()
        {
            var c = ((Class)typeof(java.lang.Runtime)).getDeclaredConstructor([]);

            var act = () => JavaDelegates.FromMethod(c);
            act.Should().Throw<java.lang.IllegalAccessException>();

            c.setAccessible(true);
            var d = (MH<object>)JavaDelegates.FromMethod(c);

            d().Should().BeOfType<java.lang.Runtime>();
        }

        [Fact]
        public void ShouldRefuseANullMethod()
        {
            var act = () => JavaDelegates.FromMethod(null!);
            act.Should().Throw<ArgumentNullException>();
        }

    }

}
