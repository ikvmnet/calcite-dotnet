using System;
using System.Collections.Generic;
using System.Data.Common;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using java.lang;

using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;
using org.apache.calcite.util;

using Xunit;

namespace Apache.Calcite.Extensions.Linq4j.Tree.Tests
{

    public class ClrTypesMethodTests
    {

        /// <summary>
        /// A CLR method with a primitive signature, for resolving from its Java reflection form back to the
        /// CLR method.
        /// </summary>
        /// <param name="value">The value to double.</param>
        /// <returns>Twice <paramref name="value"/>.</returns>
        public static int Twice(int value) => value * 2;

        [Fact]
        public void ShouldResolveInstanceMethodOnInterface()
        {
            // ExtendedEnumerable.where(Predicate1)
            var m = ClrTypes.Resolve(BuiltInMethod.WHERE.method);

            m.Name.Should().Be("where");
            m.DeclaringType.Should().Be(typeof(ExtendedEnumerable));
            m.GetParameters().Should().ContainSingle()
                .Which.ParameterType.Should().Be(typeof(org.apache.calcite.linq4j.function.Predicate1));
        }

        [Fact]
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

        [Fact]
        public void ShouldResolveBoxingMethod()
        {
            // boxing an int as an Integer is a call to valueOf, not a CLR conversion
            var m = ClrTypes.Resolve(((Class)typeof(Integer)).getDeclaredMethod("valueOf", [Integer.TYPE]));

            m.IsStatic.Should().BeTrue();
            m.ReturnType.Should().Be(typeof(Integer));
            m.Invoke(null, [7]).Should().Be(Integer.valueOf(7));
        }

        /// <summary>
        /// The three methods in Calcite's <c>BuiltInMethod</c> table that have no CLR method of their name and
        /// signature are not resolved.
        /// </summary>
        /// <remarks>
        /// IKVM implements <c>java.lang.String</c>'s and <c>java.lang.Object</c>'s Java methods outside the
        /// remapped CLR type, and <c>java.lang.Comparable</c> is a ghost interface whose CLR counterpart spells
        /// its method <c>CompareTo</c>. These are called through a delegate over the Java method instead.
        /// </remarks>
        [Fact]
        public void ShouldNotResolveWhatOnlyASearchWouldFind()
        {
            ClrTypes.TryResolve(((Class)typeof(java.lang.String)).getDeclaredMethod("toUpperCase", [])).Should().BeNull();
            ClrTypes.TryResolve(((Class)typeof(java.lang.Object)).getDeclaredMethod("toString", [])).Should().BeNull();
            ClrTypes.TryResolve(((Class)typeof(java.lang.Comparable)).getDeclaredMethod("compareTo", [typeof(object)])).Should().BeNull();
        }

        [Fact]
        public void ShouldResolveClrMethodDeclaredToJava()
        {
            var m = ClrTypes.Resolve(((Class)typeof(ClrTypesMethodTests)).getDeclaredMethod(nameof(Twice), [typeof(int)]));

            m.Should().BeSameAs(typeof(ClrTypesMethodTests).GetMethod(nameof(Twice)));
            m.Invoke(null, [21]).Should().Be(42);
        }

        [Fact]
        public void ShouldResolveClrMethodWithReferenceParameters()
        {
            var m = ClrTypes.Resolve(((Class)typeof(ClrTypesMethodTests)).getDeclaredMethod(nameof(Describe), [typeof(DbConnection), typeof(string)]));

            m.GetParameters()[0].ParameterType.Should().Be(typeof(DbConnection));
            m.GetParameters()[1].ParameterType.Should().Be(typeof(string));
        }

        /// <summary>
        /// A CLR method with reference-typed parameters, for resolving alongside <see cref="Twice"/>.
        /// </summary>
        /// <param name="connection">Unused.</param>
        /// <param name="label">The value returned.</param>
        /// <returns><paramref name="label"/>.</returns>
        public static string Describe(DbConnection connection, string label) => label;

        /// <summary>
        /// Reaches every method in Calcite's <c>BuiltInMethod</c> table, either by resolving it to a CLR method or
        /// by building a delegate over it, as a translated call does.
        /// </summary>
        /// <remarks>
        /// The number reached each way is asserted, so a method that moves from a direct call to a delegate
        /// invocation fails the test. The resolved count is a census of the loaded Calcite's table and changes
        /// when Calcite adds a method.
        /// </remarks>
        [Fact]
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

            Assert.True(failures.Count == 0, $"{called} called, {invoked.Count} invoked, {failures.Count} failed:{Environment.NewLine}{string.Join(Environment.NewLine, failures)}");

            // the size of Calcite's table, which grows with Calcite; the failure list above is what checks
            // that each method resolves
            called.Should().Be(609);
            invoked.Should().BeEquivalentTo([
                "STRING_TO_UPPER: public java.lang.String java.lang.String.toUpperCase()",
                "OBJECT_TO_STRING: public java.lang.String java.lang.Object.toString()",
                "COMPARE_TO: public abstract int java.lang.Comparable.compareTo(java.lang.Object)"]);
        }

    }

}
