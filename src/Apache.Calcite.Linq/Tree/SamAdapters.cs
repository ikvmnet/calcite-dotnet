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
        /// The adapter for each interface an anonymous class is declared against.
        /// </summary>
        static readonly Dictionary<Type, Type> Adapters = new()
        {
            [typeof(java.util.Comparator)] = typeof(ClrComparator<>),
        };

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
                throw new NotSupportedException($"There is no adapter for an anonymous '{type}'.");

            var parameters = lambda.Parameters;
            if (parameters.Count == 0)
                throw new NotSupportedException($"An anonymous '{type}' has no parameter to take its element type from.");

            var closed = adapter.MakeGenericType(parameters[0].Type);
            var constructor = closed.GetConstructor([lambda.Type])
                ?? throw new NotSupportedException($"'{closed}' takes no '{lambda.Type}'.");

            // typed as the interface rather than the adapter, because that is what the anonymous class it
            // stands for was typed as
            return Expression.Convert(Expression.New(constructor, lambda), type);
        }

    }

}
