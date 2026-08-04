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
            [typeof(java.util.function.Predicate)] = typeof(DelegatePredicate<>),
            [typeof(org.apache.calcite.runtime.Enumerables.Emitter)] = typeof(DelegateEmitter),
            [typeof(org.apache.calcite.linq4j.AbstractEnumerable)] = typeof(DelegateEnumerable),
            [typeof(org.apache.calcite.linq4j.function.Function0)] = typeof(DelegateFunction0<>),
            [typeof(org.apache.calcite.linq4j.function.Function1)] = typeof(DelegateFunction1Of<,>),
            [typeof(org.apache.calcite.linq4j.function.Function2)] = typeof(DelegateFunction2<,,>),
        };

        /// <summary>
        /// The interfaces named by what they take rather than by what they take and return.
        /// </summary>
        /// <remarks>
        /// A comparator compares two of one thing and answers an int; a predicate tests one thing and answers
        /// a boolean. Neither has a result worth naming the adapter by.
        /// </remarks>
        static readonly HashSet<Type> ByArgument = [typeof(ClrComparator<>), typeof(DelegatePredicate<>)];

        /// <summary>
        /// The interfaces an anonymous class implements with more than one method, and the order its methods
        /// are handed to the adapter in.
        /// </summary>
        /// <remarks>
        /// An anonymous class of one method is a lambda. One of several is a thing: the methods share state,
        /// so what stands in for it has to be an object holding a delegate each.
        /// </remarks>
        static readonly Dictionary<Type, (Type Adapter, string[] Methods)> Classes = new()
        {
            [typeof(org.apache.calcite.linq4j.Enumerator)] = (typeof(DelegateEnumerator), ["current", "moveNext", "reset", "close"]),
        };

        /// <summary>
        /// Returns the methods an anonymous class of this type has to supply, or null where one of its
        /// methods is the whole of it.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static string[]? MethodsOf(Type type)
        {
            return Classes.TryGetValue(type, out var entry) ? entry.Methods : null;
        }

        /// <summary>
        /// Returns an expression yielding an implementation of <paramref name="type"/> that calls one lambda
        /// per method.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="methods"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static Expression WrapClass(Type type, IReadOnlyDictionary<string, LambdaExpression> methods)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(methods);

            if (Classes.TryGetValue(type, out var entry) == false)
                throw new NotSupportedException($"There is no adapter for an anonymous '{type}'.");

            var arguments = new Expression[entry.Methods.Length];
            for (int i = 0; i < arguments.Length; i++)
                arguments[i] = methods.TryGetValue(entry.Methods[i], out var lambda)
                    ? lambda
                    : throw new NotSupportedException($"An anonymous '{type}' does not declare '{entry.Methods[i]}'.");

            return Expression.Convert(Expression.New(entry.Adapter.GetConstructors()[0], arguments), type);
        }

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

            // an adapter whose every parameter type is fixed has nothing to close over
            var closed = adapter.IsGenericTypeDefinition == false
                ? adapter
                : adapter.MakeGenericType(ByArgument.Contains(adapter) ? [lambda.Parameters[0].Type] : Arguments(lambda));
            var constructor = closed.GetConstructor([lambda.Type])
                ?? closed.GetConstructors()[0];

            // typed as the interface rather than the adapter, because that is what the lambda was declared as
            return Expression.Convert(Expression.New(constructor, lambda), type);
        }

        /// <summary>
        /// Returns the lambda inside an adapter, or null where the expression is not one.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <remarks>
        /// A lambda a linq4j tree declared against one of its functional interfaces is wrapped to be that
        /// interface, because that is what the tree says it is. An operator of this convention takes the
        /// delegate, so it asks for it back.
        /// </remarks>
        public static LambdaExpression? Unwrap(Expression expression)
        {
            if (expression is LambdaExpression lambda)
                return lambda;

            if (expression is UnaryExpression { NodeType: ExpressionType.Convert, Operand: NewExpression created }
                && created.Arguments.Count == 1
                && created.Arguments[0] is LambdaExpression inner)
                return inner;

            return null;
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
