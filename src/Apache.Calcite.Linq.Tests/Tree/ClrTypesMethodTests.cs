using System;
using System.Collections.Generic;
using System.Data.Common;

using Apache.Calcite.Linq.Tree;

using FluentAssertions;

using java.lang;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;
using org.apache.calcite.util;

namespace Apache.Calcite.Linq.Tests.Tree
{

    [TestClass]
    public class ClrTypesMethodTests
    {

        /// <summary>
        /// A method reached the way the AdoNet adapter reaches one, so the round trip back is measured
        /// against a call that is known to work.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int Twice(int value) => value * 2;

        [TestMethod]
        public void ShouldResolveInstanceMethodOnInterface()
        {
            // ExtendedEnumerable.where(Predicate1), which every filter in the port calls
            var m = ClrTypes.Resolve(BuiltInMethod.WHERE.method);

            m.Name.Should().Be("where");
            m.DeclaringType.Should().Be(typeof(ExtendedEnumerable));
            m.GetParameters().Should().ContainSingle()
                .Which.ParameterType.Should().Be(typeof(org.apache.calcite.linq4j.function.Predicate1));
        }

        [TestMethod]
        public void ShouldResolveStaticMethodAmongOverloads()
        {
            // Utilities.compare is overloaded for every primitive and for Comparable, so this fails if the
            // parameter types are not being matched
            var m = ClrTypes.Resolve(((Class)typeof(Utilities)).getDeclaredMethod("compare", [Integer.TYPE, Integer.TYPE]));

            m.Name.Should().Be("compare");
            m.GetParameters().Should().HaveCount(2);
            m.GetParameters()[0].ParameterType.Should().Be(typeof(int));
            m.GetParameters()[1].ParameterType.Should().Be(typeof(int));
        }

        [TestMethod]
        public void ShouldResolveBoxingMethod()
        {
            // the translator emits this in place of a convert from int to Integer, which is not a CLR conversion
            var m = ClrTypes.Resolve(((Class)typeof(Integer)).getDeclaredMethod("valueOf", [Integer.TYPE]));

            m.IsStatic.Should().BeTrue();
            m.ReturnType.Should().Be(typeof(Integer));
            m.Invoke(null, [7]).Should().Be(Integer.valueOf(7));
        }

        [TestMethod]
        public void ShouldResolveMethodOfRemappedClass()
        {
            // java.lang.String is System.String, which has no toUpperCase, so the method is static on a helper
            // and takes the receiver first. A translated call has to pass the target as argument zero.
            var m = ClrTypes.Resolve(((Class)typeof(java.lang.String)).getDeclaredMethod("toUpperCase", []));

            m.IsStatic.Should().BeTrue();
            m.GetParameters().Should().ContainSingle()
                .Which.ParameterType.Should().Be(typeof(string));
            m.Invoke(null, ["abc"]).Should().Be("ABC");
        }

        [TestMethod]
        public void ShouldResolveMethodInheritedFromClrInterface()
        {
            // IKVM leaves java.lang.Comparable empty and extends System.IComparable, and an interface does not
            // report what it inherits, so this only resolves if the base interfaces are walked
            var m = ClrTypes.Resolve(((Class)typeof(java.lang.Comparable)).getDeclaredMethod("compareTo", [typeof(object)]));

            m.Name.Should().Be("CompareTo");
            m.DeclaringType.Should().Be(typeof(IComparable));
        }

        [TestMethod]
        public void ShouldResolveClrMethodDeclaredToJava()
        {
            var m = ClrTypes.Resolve(((Class)typeof(ClrTypesMethodTests)).getDeclaredMethod(nameof(Twice), [typeof(int)]));

            m.Should().BeSameAs(typeof(ClrTypesMethodTests).GetMethod(nameof(Twice)));
            m.Invoke(null, [21]).Should().Be(42);
        }

        [TestMethod]
        public void ShouldResolveClrMethodWithReferenceParameters()
        {
            var m = ClrTypes.Resolve(((Class)typeof(ClrTypesMethodTests)).getDeclaredMethod(nameof(Describe), [typeof(DbConnection), typeof(string)]));

            m.GetParameters()[0].ParameterType.Should().Be(typeof(DbConnection));
            m.GetParameters()[1].ParameterType.Should().Be(typeof(string));
        }

        /// <summary>
        /// Second .NET method, present only so a reference-typed signature is covered as well as a primitive one.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="label"></param>
        /// <returns></returns>
        public static string Describe(DbConnection connection, string label) => label;

        /// <summary>
        /// Resolves every method Calcite names in <c>BuiltInMethod</c>.
        /// </summary>
        /// <remarks>
        /// The whole port rests on this direction of the interop working, and it either works for a signature
        /// shape or it does not. Sweeping the table says which shapes are the exceptions instead of finding
        /// them one at a time, node by node, much later.
        /// </remarks>
        [TestMethod]
        public void ShouldResolveEveryBuiltInMethod()
        {
            var failures = new List<string>();
            var resolved = 0;

            foreach (BuiltInMethod value in BuiltInMethod.values())
            {
                var method = value.method;
                if (method == null)
                    continue;

                try
                {
                    ClrTypes.Resolve(method);
                    resolved++;
                }
                catch (System.Exception e)
                {
                    failures.Add($"{value.name()}: {e.Message}");
                }
            }

            resolved.Should().BeGreaterThan(0);
            Assert.IsTrue(failures.Count == 0, $"{resolved} resolved, {failures.Count} failed:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");
        }

    }

}
