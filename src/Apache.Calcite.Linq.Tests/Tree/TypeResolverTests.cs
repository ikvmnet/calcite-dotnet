using System;
using System.Collections.Generic;

using Apache.Calcite.Linq.Tree;

using FluentAssertions;

using java.lang;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tests.Tree
{

    [TestClass]
    public class TypeResolverTests
    {

        [TestMethod]
        public void ShouldResolveEveryJavaPrimitive()
        {
            TypeResolver.FromClass(java.lang.Boolean.TYPE).Should().Be(typeof(bool));
            // IKVM stores Java's signed byte in a CLR byte, which is not signed. Widening one has to go by way
            // of an sbyte, which is what JavaCast does.
            TypeResolver.FromClass(java.lang.Byte.TYPE).Should().Be(typeof(byte));
            TypeResolver.FromClass(Character.TYPE).Should().Be(typeof(char));
            TypeResolver.FromClass(Short.TYPE).Should().Be(typeof(short));
            TypeResolver.FromClass(Integer.TYPE).Should().Be(typeof(int));
            TypeResolver.FromClass(Long.TYPE).Should().Be(typeof(long));
            TypeResolver.FromClass(Float.TYPE).Should().Be(typeof(float));
            TypeResolver.FromClass(java.lang.Double.TYPE).Should().Be(typeof(double));
        }

        [TestMethod]
        public void ShouldResolveBoxClassesAsDistinctFromPrimitives()
        {
            // the whole port depends on these being different types: a nullable column holds an Integer, and
            // an Integer is an object rather than a boxed int
            TypeResolver.FromClass((Class)typeof(Integer)).Should().Be(typeof(Integer));
            TypeResolver.FromClass((Class)typeof(Integer)).Should().NotBe(typeof(int));
        }

        [TestMethod]
        public void ShouldResolveRemappedClasses()
        {
            TypeResolver.FromClass((Class)typeof(java.lang.String)).Should().Be(typeof(string));

            // IKVM keeps a java.lang.Object of its own, which is not System.Object and is not assignable from
            // a string, but every signature it compiles uses System.Object. A tree naming Object means that one.
            TypeResolver.FromClass((Class)typeof(java.lang.Object)).Should().Be(typeof(object));
            typeof(java.util.List).GetMethod("get", [typeof(int)])!.ReturnType.Should().Be(typeof(object));
        }

        [TestMethod]
        public void ShouldResolveArrays()
        {
            TypeResolver.FromClass((Class)typeof(object[])).Should().Be(typeof(object[]));
            TypeResolver.FromClass((Class)typeof(int[])).Should().Be(typeof(int[]));
        }

        [TestMethod]
        public void ShouldResolveParameterizedType()
        {
            var type = Types.of((Class)typeof(java.util.List), (Class)typeof(java.lang.String));

            TypeResolver.Resolve(type).Should().Be(typeof(java.util.List));
        }

        [TestMethod]
        public void ShouldRefuseWhatItCannotName()
        {
            var act = () => TypeResolver.Resolve(new UnknownType());

            act.Should().Throw<NotSupportedException>();
        }

        /// <summary>
        /// A Java reflection type that is none of the shapes the resolver knows.
        /// </summary>
        sealed class UnknownType : java.lang.reflect.Type
        {

            public string getTypeName() => "unknown";

        }

    }

}
