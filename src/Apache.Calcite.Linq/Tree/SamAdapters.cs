using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// Wraps the lambda an anonymous class became in something implementing the interface it was declared
    /// against.
    /// </summary>
    /// <remarks>
    /// An expression tree cannot declare a class, so an anonymous one with a single method becomes that
    /// method as a lambda. What consumes it still expects the interface, because the same value can reach the
    /// same operator without having been an anonymous class at all — <c>PhysType</c> returns a comparator from
    /// a plain method call when there is only one collation.
    /// </remarks>
    public static class SamAdapters
    {

        /// <summary>
        /// The adapter for each interface a lambda may be declared against.
        /// </summary>
        static readonly Dictionary<Type, Type> Adapters = new()
        {
            [typeof(java.util.Comparator)] = typeof(ClrComparator<>),
            [typeof(org.apache.calcite.linq4j.function.Function0)] = typeof(DelegateFunction0<>),
            [typeof(org.apache.calcite.linq4j.function.Function1)] = typeof(DelegateFunction1Of<,>),
            [typeof(org.apache.calcite.linq4j.function.Function2)] = typeof(DelegateFunction2<,,>),
        };

        /// <summary>
        /// Returns whether a lambda declared against this type has to be wrapped to be used as one.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static bool Handles(Type type) => Adapters.ContainsKey(type);

        /// <summary>
        /// Returns an expression yielding an implementation of <paramref name="type"/> that calls
        /// <paramref name="lambda"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="lambda"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static Expression Wrap(Type type, LambdaExpression lambda)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(lambda);

            if (Adapters.TryGetValue(type, out var adapter) == false)
                throw new NotSupportedException($"There is no adapter for a lambda declared as '{type}'.");

            // a comparator is named by what it compares; the rest by what they take and return
            var arguments = adapter == typeof(ClrComparator<>)
                ? [lambda.Parameters[0].Type]
                : Arguments(lambda);

            var closed = adapter.MakeGenericType(arguments);
            var constructor = closed.GetConstructor([lambda.Type])
                ?? closed.GetConstructors()[0];

            // typed as the interface rather than the adapter, because that is what the lambda was declared as
            return Expression.Convert(Expression.New(constructor, lambda), type);
        }

        /// <summary>
        /// Returns the parameter types of a lambda followed by its result type.
        /// </summary>
        /// <param name="lambda"></param>
        /// <returns></returns>
        static Type[] Arguments(LambdaExpression lambda)
        {
            var arguments = new Type[lambda.Parameters.Count + 1];
            for (int i = 0; i < lambda.Parameters.Count; i++)
                arguments[i] = lambda.Parameters[i].Type;

            arguments[^1] = lambda.ReturnType;

            return arguments;
        }

    }

}
