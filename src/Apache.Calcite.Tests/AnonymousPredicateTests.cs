using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Requires that a lambda declared as one of linq4j's predicates is wrapped to be one.
    /// </summary>
    /// <remarks>
    /// A block of Calcite's making passes a lambda to an operator of its own, whose parameter is the
    /// functional interface the lambda was declared against. <see cref="AnonymousClasses"/> is what turns the
    /// delegate into that interface, and an interface missing from its table is not refused: the lambda is
    /// left a delegate and the conversion to the interface is emitted anyway, which the expression compiler
    /// accepts — a delegate and an interface are both references — and which throws
    /// <see cref="InvalidCastException"/> the first time the compiled plan runs.
    ///
    /// <para>That is how <c>Predicate1</c> and <c>Predicate2</c> were missed. They are not
    /// <c>Function1</c> and <c>Function2</c> of <c>Boolean</c>: each declares its own <c>apply</c> returning
    /// a primitive, so a function adapter put in their place does not implement them either. The failure has
    /// no compile-time symptom and does not appear until a plan that mixes this convention with Calcite's own
    /// reaches such an operator, so the table is checked here directly.</para>
    /// </remarks>
    [TestClass]
    public class AnonymousPredicateTests
    {

        /// <summary>
        /// Every functional interface a linq4j tree may declare a lambda against has to be one
        /// <see cref="AnonymousClasses"/> handles, or the conversion is emitted unwrapped.
        /// </summary>
        [TestMethod]
        public void ShouldHandleLinq4jPredicates()
        {
            AnonymousClasses.Handles(typeof(Predicate1)).Should().BeTrue();
            AnonymousClasses.Handles(typeof(Predicate2)).Should().BeTrue();
        }

        /// <summary>
        /// A one-argument predicate answers what its delegate answers, through the interface.
        /// </summary>
        [TestMethod]
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
        /// A two-argument predicate is closed over both of the types it takes, neither of them its result:
        /// the pair the join in a mixed plan hands it is a row and a key, and they are not the same type.
        /// </summary>
        [TestMethod]
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
