using System;

using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The four members Calcite writes into a generated record class, which are on <c>SyntheticRecord</c> here
    /// instead.
    /// </summary>
    [TestClass]
    public class SyntheticRecordTests
    {

        /// <summary>
        /// Returns the CLR type of a synthetic record over the given Java field types.
        /// </summary>
        /// <param name="types"></param>
        /// <returns></returns>
        static Type Record(params java.lang.Class[] types)
        {
            var list = new java.util.ArrayList();
            foreach (var type in types)
                list.add(type);

            return ClrTypes.Resolve((java.lang.reflect.Type)new JavaTypeFactoryImpl().createSyntheticType(list));
        }

        /// <summary>
        /// <c>compareTo</c> returns on the first field that differs, and one of a reference type is among them:
        /// Calcite's generated body compares it through <c>Utilities.compare(Comparable, Comparable)</c> and
        /// leaves a field out only where no overload takes it at all.
        /// </summary>
        /// <remarks>
        /// The ghost interface, from the other side. IKVM erases a <c>java.lang.Comparable</c> parameter to
        /// <see cref="IComparable"/>, so the overload was looked for under a type no signature has, never
        /// found, and every reference field dropped out of the comparison — a record of a string and an int
        /// ordered on the int alone.
        /// </remarks>
        [TestMethod]
        public void ShouldCompareAReferenceField()
        {
            var type = Record((java.lang.Class)typeof(java.lang.String), java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, ["A", 1])!;
            var b = Activator.CreateInstance(type, ["B", 1])!;

            ((IComparable)a).CompareTo(b).Should().BeNegative();
            ((IComparable)b).CompareTo(a).Should().BePositive();
            ((IComparable)a).CompareTo(Activator.CreateInstance(type, ["A", 1])!).Should().Be(0);
        }

        /// <summary>
        /// A field of a primitive type is compared through the overload that takes it.
        /// </summary>
        [TestMethod]
        public void ShouldCompareAPrimitiveField()
        {
            var type = Record(java.lang.Integer.TYPE, java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, [1, 2])!;
            var b = Activator.CreateInstance(type, [1, 3])!;

            ((IComparable)a).CompareTo(b).Should().BeNegative();
            ((IComparable)b).CompareTo(a).Should().BePositive();
        }

        /// <summary>
        /// A field of a type no overload takes is left out of the comparison rather than failing it, because a
        /// record is not always used as a sorting key.
        /// </summary>
        [TestMethod]
        public void ShouldSkipAFieldNothingCompares()
        {
            var type = Record((java.lang.Class)typeof(java.lang.Object), java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, [new object(), 1])!;
            var b = Activator.CreateInstance(type, [new object(), 1])!;

            ((IComparable)a).CompareTo(b).Should().Be(0);
        }

    }

}
