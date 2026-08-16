using System;
using System.Runtime.CompilerServices;

using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The six members <c>EnumerableRelImplementor.classDecl</c> writes into a generated record class.
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
        /// A record of one field per type, with the two constructors and nothing that is not a field.
        /// </summary>
        [TestMethod]
        public void ShouldEmitAFieldPerRecordField()
        {
            var type = Record((java.lang.Class)typeof(java.lang.String), java.lang.Integer.TYPE);

            type.GetFields().Should().HaveCount(2);
            type.GetField("f0")!.FieldType.Should().Be(typeof(string));
            type.GetField("f1")!.FieldType.Should().Be(typeof(int));
            type.GetConstructor([]).Should().NotBeNull();
            type.GetConstructor([typeof(string), typeof(int)]).Should().NotBeNull();
        }

        /// <summary>
        /// <c>equals</c> compares every field, a primitive with <c>==</c> and a reference through Guava's
        /// <c>Objects.equal</c>, and <c>hashCode</c> accumulates <c>Utilities.hash</c> over them.
        /// </summary>
        [TestMethod]
        public void ShouldEqualAndHashByEveryField()
        {
            var type = Record((java.lang.Class)typeof(java.lang.String), java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, ["A", 1])!;

            a.Equals(a).Should().BeTrue();
            a.Equals(Activator.CreateInstance(type, ["A", 1])).Should().BeTrue();
            a.Equals(Activator.CreateInstance(type, ["B", 1])).Should().BeFalse();
            a.Equals(Activator.CreateInstance(type, ["A", 2])).Should().BeFalse();
            a.Equals(null).Should().BeFalse();
            a.Equals("A").Should().BeFalse();

            a.GetHashCode().Should().Be(Activator.CreateInstance(type, ["A", 1])!.GetHashCode());
        }

        /// <summary>
        /// A null in a reference field is equal to another null and not to a value.
        /// </summary>
        [TestMethod]
        public void ShouldEqualOnANullReferenceField()
        {
            var type = Record((java.lang.Class)typeof(java.lang.String), java.lang.Integer.TYPE);

            var n = Activator.CreateInstance(type, [null, 1])!;

            n.Equals(Activator.CreateInstance(type, [null, 1])).Should().BeTrue();
            n.Equals(Activator.CreateInstance(type, ["A", 1])).Should().BeFalse();
            n.GetHashCode().Should().Be(Activator.CreateInstance(type, [null, 1])!.GetHashCode());
        }

        /// <summary>
        /// <c>compareTo</c> returns on the first field that differs, and one of a reference type is among
        /// them: Calcite's body compares it through <c>Utilities.compare</c> and leaves a field out only
        /// where no overload takes it.
        /// </summary>
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
        /// A field the record type declares nullable is compared with <c>Utilities.compareNullsLast</c>, which
        /// orders a null after everything rather than throwing on it. Every reference field of a record built
        /// from a list of types is nullable, because <c>createSyntheticType</c> asks <c>!Primitive.is</c>.
        /// </summary>
        [TestMethod]
        public void ShouldCompareANullReferenceFieldLast()
        {
            var type = Record((java.lang.Class)typeof(java.lang.String), java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, ["A", 1])!;
            var n = Activator.CreateInstance(type, [null, 1])!;

            ((IComparable)a).CompareTo(n).Should().BeNegative();
            ((IComparable)n).CompareTo(a).Should().BePositive();
            ((IComparable)n).CompareTo(Activator.CreateInstance(type, [null, 1])!).Should().Be(0);
        }

        /// <summary>
        /// A primitive field is not nullable and is compared with the overload that takes it.
        /// </summary>
        [TestMethod]
        public void ShouldCompareAPrimitiveField()
        {
            var type = Record(java.lang.Integer.TYPE, java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, [1, 2])!;
            var b = Activator.CreateInstance(type, [1, 3])!;

            ((IComparable)a).CompareTo(b).Should().BeNegative();
            ((IComparable)b).CompareTo(a).Should().BePositive();
            ((IComparable)a).CompareTo(Activator.CreateInstance(type, [1, 2])!).Should().Be(0);
        }

        /// <summary>
        /// A field of a type no overload of <c>compare</c> takes is left out rather than failing the
        /// comparison, because a record is not always used as a sorting key.
        /// </summary>
        [TestMethod]
        public void ShouldSkipAFieldNothingCompares()
        {
            var type = Record((java.lang.Class)typeof(java.lang.Object), java.lang.Integer.TYPE);

            var a = Activator.CreateInstance(type, [new object(), 1])!;
            var b = Activator.CreateInstance(type, [new object(), 2])!;

            ((IComparable)a).CompareTo(b).Should().BeNegative();
            ((IComparable)a).CompareTo(Activator.CreateInstance(type, [new object(), 1])!).Should().Be(0);
        }

        /// <summary>
        /// <c>toString</c> renders every field, and a null the way Java's string concatenation does.
        /// </summary>
        [TestMethod]
        public void ShouldPrintEveryField()
        {
            var type = Record((java.lang.Class)typeof(java.lang.String), java.lang.Integer.TYPE);

            Activator.CreateInstance(type, ["A", 1])!.ToString().Should().Be("{f0=A, f1=1}");
            Activator.CreateInstance(type, [null, 1])!.ToString().Should().Be("{f0=null, f1=1}");
        }

        /// <summary>
        /// The emitted type is collected when the type factory that described it is, which is when the
        /// connection closes.
        /// </summary>
        /// <remarks>
        /// The record type is reachable only from <c>JavaTypeFactoryImpl.syntheticTypes</c>, so this holds
        /// exactly as long as the emitter keys on it weakly and gives it an assembly of its own —
        /// <c>RunAndCollect</c> collects an assembly rather than a type, so a type in a module shared with a
        /// live one is never released, and a strongly keyed map roots the record type itself.
        /// </remarks>
        [TestMethod]
        public void ShouldCollectWithTheTypeFactory()
        {
            var (type, factory) = Emitted();

            for (int i = 0; i < 5; i++)
            {
                GC.Collect(2, GCCollectionMode.Forced, true, true);
                GC.WaitForPendingFinalizers();
            }

            factory.IsAlive.Should().BeFalse();
            type.IsAlive.Should().BeFalse();
        }

        /// <summary>
        /// Emits a record from a factory of its own, and keeps neither.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        static (WeakReference Type, WeakReference Factory) Emitted()
        {
            var factory = new JavaTypeFactoryImpl();
            var list = new java.util.ArrayList();
            list.add((java.lang.Class)typeof(java.lang.String));
            list.add(java.lang.Integer.TYPE);

            var clr = ClrTypes.Resolve((java.lang.reflect.Type)factory.createSyntheticType(list));
            Activator.CreateInstance(clr, ["A", 1]);

            return (new WeakReference(clr), new WeakReference(factory));
        }

    }

}
