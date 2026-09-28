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
        /// The case a name search cannot answer and this exists for: java.lang.Object is remapped onto
        /// System.Object, and its Java methods have no CLR method of that name on that type at all.
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
        /// The other half of it: String is remapped onto System.String, and toUpperCase lands on
        /// java.lang.StringHelper taking the receiver first. The delegate hides that the receiver moved.
        /// </summary>
        [Fact]
        public void ShouldCallMethodMovedToAHelperClass()
        {
            var m = Method(typeof(java.lang.String), "toUpperCase");
            var d = (MH<object, object>)JavaDelegates.FromMethod(m);

            d("abc").Should().Be("ABC");
        }

        /// <summary>
        /// A ghost interface declares nothing the CLR type system can see, and a cast to one throws. The
        /// handle does not cast.
        /// </summary>
        [Fact]
        public void ShouldCallMethodOnGhostInterface()
        {
            var m = Method(typeof(java.lang.Comparable), "compareTo", (Class)typeof(java.lang.Object));
            var d = (MH<object, object, int>)JavaDelegates.FromMethod(m);

            d("a", "b").Should().BeNegative();
        }

        /// <summary>
        /// A method of Calcite's own, reached the way the convention reaches one — and its primitive
        /// parameter kept primitive, which is what keeps a java.lang.Integer from crossing where a CLR int
        /// belongs.
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
        /// Access is checked as Lookup.unreflect checks it, and marking the member accessible is the caller's
        /// to do. java.lang.Runtime's constructor is private.
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
