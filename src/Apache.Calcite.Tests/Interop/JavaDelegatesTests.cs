using System;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using IKVM.Runtime;

using java.lang;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.util;

namespace Apache.Calcite.Tests.Interop
{

    [TestClass]
    public class JavaDelegatesTests
    {

        static java.lang.reflect.Method Method(Type declaring, string name, params Class[] parameters)
        {
            return ((Class)declaring).getMethod(name, parameters);
        }

        [TestMethod]
        public void ShouldCallInstanceMethod()
        {
            var m = Method(typeof(java.util.ArrayList), "size");
            var d = (Func<object, int>)JavaDelegates.FromMethod(typeof(Func<object, int>), m);

            var list = new java.util.ArrayList();
            list.add("a");
            list.add("b");

            d(list).Should().Be(2);
        }

        [TestMethod]
        public void ShouldCallInstanceMethodWithArgument()
        {
            var m = Method(typeof(StringBuilder), "append", (Class)typeof(java.lang.String));
            var d = (Func<object, object, object>)JavaDelegates.FromMethod(typeof(Func<object, object, object>), m);

            var sb = new StringBuilder("a");
            d(sb, "b").Should().BeSameAs(sb);
            sb.toString().Should().Be("ab");
        }

        [TestMethod]
        public void ShouldCallStaticMethod()
        {
            var m = Method(typeof(Integer), "parseInt", (Class)typeof(java.lang.String));
            var d = (Func<object, int>)JavaDelegates.FromMethod(typeof(Func<object, int>), m);

            d("42").Should().Be(42);
        }

        [TestMethod]
        public void ShouldCallVoidMethod()
        {
            var m = Method(typeof(java.util.ArrayList), "clear");
            var d = (Action<object>)JavaDelegates.FromMethod(typeof(Action<object>), m);

            var list = new java.util.ArrayList();
            list.add("a");
            d(list);

            list.size().Should().Be(0);
        }

        [TestMethod]
        public void ShouldCallConstructor()
        {
            var c = ((Class)typeof(java.util.ArrayList)).getConstructor([]);
            var d = (Func<object>)JavaDelegates.FromMethod(typeof(Func<object>), c);

            d().Should().BeOfType<java.util.ArrayList>();
        }

        /// <summary>
        /// The case a name search cannot answer and this exists for: java.lang.Object is remapped onto
        /// System.Object, and its Java methods have no CLR method of that name on that type at all.
        /// </summary>
        [TestMethod]
        public void ShouldCallMethodOnRemappedType()
        {
            var m = Method(typeof(java.lang.Object), "hashCode");
            var d = (Func<object, int>)JavaDelegates.FromMethod(typeof(Func<object, int>), m);

            var o = new object();
            d(o).Should().Be(java.lang.System.identityHashCode(o));
        }

        /// <summary>
        /// The other half of it: String is remapped onto System.String, and toUpperCase lands on
        /// java.lang.StringHelper taking the receiver first. The delegate hides that the receiver moved.
        /// </summary>
        [TestMethod]
        public void ShouldCallMethodMovedToAHelperClass()
        {
            var m = Method(typeof(java.lang.String), "toUpperCase");
            var d = (Func<string, string>)JavaDelegates.FromMethod(typeof(Func<string, string>), m);

            d("abc").Should().Be("ABC");
        }

        /// <summary>
        /// A ghost interface declares nothing the CLR type system can see, and a cast to one throws. The
        /// handle does not cast.
        /// </summary>
        [TestMethod]
        public void ShouldCallMethodOnGhostInterface()
        {
            var m = Method(typeof(java.lang.Comparable), "compareTo", (Class)typeof(java.lang.Object));
            var d = (Func<object, object, int>)JavaDelegates.FromMethod(typeof(Func<object, object, int>), m);

            d("a", "b").Should().BeNegative();
        }

        /// <summary>
        /// A delegate typed in the runtime's own terms rather than in objects, which is what a translated
        /// tree wants: no value crosses a boxing boundary on the way in or out.
        /// </summary>
        [TestMethod]
        public void ShouldCallThroughATypedDelegate()
        {
            var m = Method(typeof(Integer), "parseInt", (Class)typeof(java.lang.String));
            var d = (Func<string, int>)JavaDelegates.FromMethod(typeof(Func<string, int>), m);

            d("42").Should().Be(42);
        }

        /// <summary>
        /// The canonical delegate is what IKVM hands back, so asking for it directly is the one signature
        /// that costs nothing at all — no second delegate wrapping the first.
        /// </summary>
        [TestMethod]
        public void ShouldReturnTheCanonicalDelegateUnwrapped()
        {
            var m = Method(typeof(Integer), "parseInt", (Class)typeof(java.lang.String));
            var d = (MH<string, int>)JavaDelegates.FromMethod(typeof(MH<string, int>), m);

            d("42").Should().Be(42);
            d.Target.Should().BeAssignableTo<java.lang.invoke.MethodHandle>();
        }

        /// <summary>
        /// A method of Calcite's own, reached the way the convention reaches one.
        /// </summary>
        [TestMethod]
        public void ShouldCallACalciteBuiltInMethod()
        {
            var d = (Func<object, int, object>)JavaDelegates.FromMethod(typeof(Func<object, int, object>), BuiltInMethod.LIST_GET.method);

            var list = new java.util.ArrayList();
            list.add("a");
            list.add("b");

            d(list, 1).Should().Be("b");
        }

        /// <summary>
        /// A primitive crossing as an object is a java.lang.Integer rather than a boxed CLR int, and the
        /// adaptation the handle performs is Java's own. This is the boundary the whole port turns on.
        /// </summary>
        [TestMethod]
        public void ShouldBoxAPrimitiveReturnTheJavaWay()
        {
            var m = Method(typeof(Integer), "parseInt", (Class)typeof(java.lang.String));
            var d = (Func<object, object>)JavaDelegates.FromMethod(typeof(Func<object, object>), m);

            d("42").Should().BeOfType<Integer>();
        }

        /// <summary>
        /// Access is checked as Lookup.unreflect checks it, and marking the member accessible is the caller's
        /// to do. java.lang.Runtime's constructor is private.
        /// </summary>
        [TestMethod]
        public void ShouldRefuseAnInaccessibleMemberUntilMarkedAccessible()
        {
            var c = ((Class)typeof(Runtime)).getDeclaredConstructor([]);

            var act = () => JavaDelegates.FromMethod(typeof(Func<object>), c);
            act.Should().Throw<java.lang.IllegalAccessException>();

            c.setAccessible(true);
            var d = (Func<object>)JavaDelegates.FromMethod(typeof(Func<object>), c);

            d().Should().BeOfType<Runtime>();
        }

        [TestMethod]
        public void ShouldRefuseATypeThatIsNotADelegate()
        {
            var m = Method(typeof(java.util.ArrayList), "size");

            var act = () => JavaDelegates.FromMethod(typeof(string), m);
            act.Should().Throw<ArgumentException>();
        }

        [TestMethod]
        public void ShouldRefuseAnIncompatibleSignature()
        {
            var m = Method(typeof(Integer), "parseInt", (Class)typeof(java.lang.String));

            // parseInt takes one argument; this delegate supplies three
            var act = () => JavaDelegates.FromMethod(typeof(Func<object, object, object, int>), m);
            act.Should().Throw<java.lang.invoke.WrongMethodTypeException>();
        }

        [TestMethod]
        public void ShouldRefuseNullArguments()
        {
            var m = Method(typeof(java.util.ArrayList), "size");

            var nullType = () => JavaDelegates.FromMethod(null!, m);
            nullType.Should().Throw<ArgumentNullException>();

            var nullMethod = () => JavaDelegates.FromMethod(typeof(Func<object, int>), null!);
            nullMethod.Should().Throw<ArgumentNullException>();
        }

    }

}
