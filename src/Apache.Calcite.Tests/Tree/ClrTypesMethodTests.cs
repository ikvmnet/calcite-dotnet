using System;
using System.Collections.Generic;
using System.Data.Common;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using java.lang;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;
using org.apache.calcite.util;
using Apache.Calcite.Extensions;

namespace Apache.Calcite.Tests.Tree
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

        /// <summary>
        /// The three methods of Calcite's whole table that have no CLR method of their name and signature.
        /// </summary>
        /// <remarks>
        /// Each was reachable by a search that reconstructed what IKVM had done — java.lang.String is
        /// System.String and its Java methods went to a static helper; java.lang.Comparable is left empty and
        /// extends System.IComparable, whose method is spelled CompareTo. Both reconstructions happened to
        /// land, and neither was an answer from IKVM. They go to a delegate over the method handle instead,
        /// which is one.
        /// </remarks>
        [TestMethod]
        public void ShouldNotResolveWhatOnlyASearchWouldFind()
        {
            ClrTypes.TryResolve(((Class)typeof(java.lang.String)).getDeclaredMethod("toUpperCase", [])).Should().BeNull();
            ClrTypes.TryResolve(((Class)typeof(java.lang.Object)).getDeclaredMethod("toString", [])).Should().BeNull();
            ClrTypes.TryResolve(((Class)typeof(java.lang.Comparable)).getDeclaredMethod("compareTo", [typeof(object)])).Should().BeNull();
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
        /// Reaches every method Calcite names in <c>BuiltInMethod</c>, by one route or the other.
        /// </summary>
        /// <remarks>
        /// The whole port rests on this direction of the interop working, and it either works for a signature
        /// shape or it does not. Sweeping the table says which shapes are the exceptions instead of finding
        /// them one at a time, node by node, much later.
        ///
        /// <para>Reaching one is either resolving it to a CLR method or building a delegate over it, which is
        /// what a translated call does. The count of each is asserted rather than only the total: a method
        /// moving from the first route to the second is a call becoming an invoke, and that should not happen
        /// unnoticed.</para>
        /// </remarks>
        [TestMethod]
        public void ShouldReachEveryBuiltInMethod()
        {
            var failures = new List<string>();
            var invoked = new List<string>();
            var called = 0;

            foreach (BuiltInMethod value in BuiltInMethod.values())
            {
                var method = value.method;
                if (method == null)
                    continue;

                try
                {
                    if (ClrTypes.TryResolve(method) != null)
                        called++;
                    else if (JavaDelegates.FromMethod(method) != null)
                        invoked.Add($"{value.name()}: {method}");
                }
                catch (System.Exception e)
                {
                    failures.Add($"{value.name()}: {e.Message}");
                }
            }

            Assert.IsTrue(failures.Count == 0, $"{called} called, {invoked.Count} invoked, {failures.Count} failed:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");

            called.Should().Be(594);
            invoked.Should().BeEquivalentTo([
                "STRING_TO_UPPER: public java.lang.String java.lang.String.toUpperCase()",
                "OBJECT_TO_STRING: public java.lang.String java.lang.Object.toString()",
                "COMPARE_TO: public abstract int java.lang.Comparable.compareTo(java.lang.Object)"]);
        }

    }

}
