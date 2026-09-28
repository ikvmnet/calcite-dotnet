using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using org.apache.calcite.linq4j.function;

using Xunit;

namespace Apache.Calcite.Extensions.Linq4j.Tree.Tests
{

    /// <summary>
    /// Checks that <see cref="AnonymousClasses"/> wraps a lambda declared as linq4j's <c>Predicate1</c> or
    /// <c>Predicate2</c> in an implementation of that interface.
    /// </summary>
    /// <remarks>
    /// The translator leaves a lambda as a delegate when <see cref="AnonymousClasses.Handles"/> does not know its
    /// interface. The conversion to the interface still compiles, because both are reference types, and throws
    /// <see cref="InvalidCastException"/> only when the plan runs, so the table is checked directly.
    /// <c>Predicate1</c> and <c>Predicate2</c> each declare an <c>apply</c> returning a primitive
    /// <c>boolean</c>, so an adapter for <c>Function1</c> or <c>Function2</c> does not implement them.
    /// </remarks>
    public class AnonymousClassesTests
    {

        /// <summary>
        /// <see cref="AnonymousClasses"/> handles both of linq4j's predicate interfaces, so a lambda declared
        /// against either is wrapped rather than left a delegate.
        /// </summary>
        [Fact]
        public void ShouldHandleLinq4jPredicates()
        {
            AnonymousClasses.Handles(typeof(Predicate1)).Should().BeTrue();
            AnonymousClasses.Handles(typeof(Predicate2)).Should().BeTrue();
        }

        /// <summary>
        /// A wrapped one-argument predicate answers, through the interface, what its lambda answers.
        /// </summary>
        [Fact]
        public void ShouldWrapPredicate1()
        {
            Expression<Func<string, bool>> lambda = s => s.Length > 2;

            var wrapped = AnonymousClasses.Wrap(typeof(Predicate1), lambda);
            wrapped.Type.Should().Be(typeof(Predicate1));

            var predicate = (Predicate1)Expression.Lambda<Func<Predicate1>>(wrapped).Compile()();
            predicate.apply("abc").Should().BeTrue();
            predicate.apply("ab").Should().BeFalse();
        }

        /// <summary>
        /// A two-argument predicate whose arguments have different types, as a row and a key do.
        /// </summary>
        [Fact]
        public void ShouldWrapPredicate2()
        {
            Expression<Func<object[], int, bool>> lambda = (row, key) => (int)row[0] == key;

            var wrapped = AnonymousClasses.Wrap(typeof(Predicate2), lambda);
            wrapped.Type.Should().Be(typeof(Predicate2));

            var predicate = (Predicate2)Expression.Lambda<Func<Predicate2>>(wrapped).Compile()();
            predicate.apply(new object[] { 1 }, 1).Should().BeTrue();
            predicate.apply(new object[] { 1 }, 2).Should().BeFalse();
        }

    }

}
