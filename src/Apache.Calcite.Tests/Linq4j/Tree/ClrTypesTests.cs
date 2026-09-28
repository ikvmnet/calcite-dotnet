using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using java.lang;

using org.apache.calcite.linq4j.tree;

using Xunit;

namespace Apache.Calcite.Extensions.Linq4j.Tree.Tests
{

    public class ClrTypesTests
    {

        [Fact]
        public void ShouldResolveEveryJavaPrimitive()
        {
            ClrTypes.FromClass(java.lang.Boolean.TYPE).Should().Be(typeof(bool));
            // IKVM maps Java's signed byte to the unsigned CLR byte; ClrEnumUtils widens one by way of sbyte
            ClrTypes.FromClass(java.lang.Byte.TYPE).Should().Be(typeof(byte));
            ClrTypes.FromClass(Character.TYPE).Should().Be(typeof(char));
            ClrTypes.FromClass(Short.TYPE).Should().Be(typeof(short));
            ClrTypes.FromClass(Integer.TYPE).Should().Be(typeof(int));
            ClrTypes.FromClass(Long.TYPE).Should().Be(typeof(long));
            ClrTypes.FromClass(Float.TYPE).Should().Be(typeof(float));
            ClrTypes.FromClass(java.lang.Double.TYPE).Should().Be(typeof(double));
        }

        [Fact]
        public void ShouldResolveBoxClassesAsDistinctFromPrimitives()
        {
            // a nullable column holds a java.lang.Integer, which is a reference type and not a boxed int
            ClrTypes.FromClass((Class)typeof(Integer)).Should().Be(typeof(Integer));
            ClrTypes.FromClass((Class)typeof(Integer)).Should().NotBe(typeof(int));
        }

        [Fact]
        public void ShouldResolveRemappedClasses()
        {
            ClrTypes.FromClass((Class)typeof(java.lang.String)).Should().Be(typeof(string));

            // IKVM has a java.lang.Object type of its own that is not System.Object, but every signature it
            // compiles uses System.Object, so a tree naming Object means System.Object
            ClrTypes.FromClass((Class)typeof(java.lang.Object)).Should().Be(typeof(object));
            typeof(java.util.List).GetMethod("get", [typeof(int)])!.ReturnType.Should().Be(typeof(object));
        }

        [Fact]
        public void ShouldResolveArrays()
        {
            ClrTypes.FromClass((Class)typeof(object[])).Should().Be(typeof(object[]));
            ClrTypes.FromClass((Class)typeof(int[])).Should().Be(typeof(int[]));
        }

        [Fact]
        public void ShouldResolveParameterizedType()
        {
            var type = Types.of((Class)typeof(java.util.List), (Class)typeof(java.lang.String));

            ClrTypes.Resolve(type).Should().Be(typeof(java.util.List));
        }

        [Fact]
        public void ShouldRefuseWhatItCannotName()
        {
            var act = () => ClrTypes.Resolve(new UnknownType());

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
