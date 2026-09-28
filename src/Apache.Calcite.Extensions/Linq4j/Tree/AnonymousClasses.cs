using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Function;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Wraps the lambdas that translated linq4j anonymous classes become in adapters implementing the
    /// interface each class was declared against.
    /// </summary>
    /// <remarks>
    /// An expression tree cannot declare a class, so an anonymous class of one method is translated to that
    /// method as a lambda, and one of several methods to a lambda per method. Code that consumes the value
    /// still expects the interface, since the same value can also arrive from an ordinary call (for a
    /// single-field collation <c>PhysType</c> returns a comparator from a method call), so the lambdas are
    /// wrapped in the adapters of <c>Linq4j.Function</c>.
    /// </remarks>
    static class AnonymousClasses
    {

        /// <summary>
        /// The adapter for each single-method interface or class a lambda may be declared against.
        /// </summary>
        static readonly Dictionary<Type, Type> Adapters = new()
        {
            [typeof(java.util.Comparator)] = typeof(DelegateComparator<>),
            [typeof(java.util.function.Predicate)] = typeof(DelegatePredicate<>),
            [typeof(org.apache.calcite.runtime.Enumerables.Emitter)] = typeof(DelegateEmitter),
            [typeof(org.apache.calcite.linq4j.AbstractEnumerable)] = typeof(DelegateEnumerable),
            [typeof(org.apache.calcite.linq4j.function.Function0)] = typeof(DelegateFunction0<>),
            [typeof(org.apache.calcite.linq4j.function.Function1)] = typeof(DelegateFunction1Of<,>),
            [typeof(org.apache.calcite.linq4j.function.Function2)] = typeof(DelegateFunction2<,,>),
            [typeof(org.apache.calcite.linq4j.function.Predicate1)] = typeof(DelegatePredicate1<>),
            [typeof(org.apache.calcite.linq4j.function.Predicate2)] = typeof(DelegatePredicate2<,>),
        };

        /// <summary>
        /// The generic adapters closed over the lambda's first parameter type only.
        /// </summary>
        /// <remarks>
        /// A comparator takes two values of one type and a predicate one value; neither result type is a type
        /// argument.
        /// </remarks>
        static readonly HashSet<Type> ByArgument = [typeof(DelegateComparator<>), typeof(DelegatePredicate<>)];

        /// <summary>
        /// The generic adapters closed over every parameter type of the lambda and not its result type.
        /// </summary>
        /// <remarks>
        /// A linq4j predicate's <c>apply</c> returns a primitive <see cref="bool"/>, so its result is not a
        /// type argument. Every other generic adapter is closed over the parameter types and the result type.
        /// </remarks>
        static readonly HashSet<Type> ByArguments = [typeof(DelegatePredicate1<>), typeof(DelegatePredicate2<,>)];

        /// <summary>
        /// The multi-method types an anonymous class may implement, each with its adapter and the order in
        /// which the adapter's constructor takes the methods' lambdas.
        /// </summary>
        static readonly Dictionary<Type, (Type Adapter, string[] Methods)> Classes = new()
        {
            [typeof(org.apache.calcite.linq4j.Enumerator)] = (typeof(DelegateEnumerator), ["current", "moveNext", "reset", "close"]),
        };

        /// <summary>
        /// Returns the methods an anonymous class of <paramref name="type"/> must declare, or
        /// <see langword="null"/> if the type is not a multi-method type with an adapter.
        /// </summary>
        /// <param name="type">The type an anonymous class implements or extends.</param>
        /// <returns>The Java names of the methods, in the order the adapter's constructor takes their lambdas, or <see langword="null"/>.</returns>
        public static string[]? MethodsOf(Type type)
        {
            return Classes.TryGetValue(type, out var entry) ? entry.Methods : null;
        }

        /// <summary>
        /// Returns an expression creating an implementation of <paramref name="type"/> that calls one lambda
        /// per method.
        /// </summary>
        /// <param name="type">The multi-method type.</param>
        /// <param name="methods">The lambda for each method, by Java method name.</param>
        /// <exception cref="NotSupportedException"><paramref name="type"/> has no adapter, or a method is
        /// missing from <paramref name="methods"/>.</exception>
        /// <returns>An expression of type <paramref name="type"/> constructing the adapter over the lambdas.</returns>
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
        /// Returns whether there is an adapter for a lambda declared against <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The interface or class a lambda was declared against.</param>
        /// <returns><see langword="true"/> if <see cref="Wrap"/> can wrap a lambda declared against <paramref name="type"/>.</returns>
        public static bool Handles(Type type) => Adapters.ContainsKey(type);

        /// <summary>
        /// Returns an expression yielding an implementation of <paramref name="type"/> that calls
        /// <paramref name="lambda"/>.
        /// </summary>
        /// <param name="type">The interface or class the lambda was declared against.</param>
        /// <param name="lambda">The lambda.</param>
        /// <returns>An expression of type <paramref name="type"/>.</returns>
        /// <exception cref="NotSupportedException"><paramref name="type"/> has no adapter.</exception>
        public static Expression Wrap(Type type, LambdaExpression lambda)
        {
            ArgumentNullException.ThrowIfNull(type);
            ArgumentNullException.ThrowIfNull(lambda);

            if (Adapters.TryGetValue(type, out var adapter) == false)
                throw new NotSupportedException($"There is no adapter for a lambda declared as '{type}'.");

            // a non-generic adapter's types are fixed by the interface
            var closed = adapter.IsGenericTypeDefinition == false
                ? adapter
                : adapter.MakeGenericType(
                    ByArgument.Contains(adapter) ? [lambda.Parameters[0].Type] :
                    ByArguments.Contains(adapter) ? Parameters(lambda) :
                    Arguments(lambda));
            var constructor = closed.GetConstructor([lambda.Type])
                ?? closed.GetConstructors()[0];

            // typed as the declared interface rather than the adapter
            return Expression.Convert(Expression.New(constructor, lambda), type);
        }

        /// <summary>
        /// Returns <paramref name="expression"/> if it is a lambda, the lambda inside it if it is a
        /// <see cref="Wrap"/> result, and <see langword="null"/> otherwise.
        /// </summary>
        /// <remarks>
        /// The operators of the cursor convention take delegates, so they recover the lambda from a wrapped
        /// value.
        /// </remarks>
        /// <param name="expression">An expression that may be a lambda or a wrapped lambda.</param>
        /// <returns>The lambda, or <see langword="null"/>.</returns>
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
        /// Returns the parameter types of a lambda.
        /// </summary>
        /// <param name="lambda">The lambda.</param>
        /// <returns>The type of each parameter, in order.</returns>
        static Type[] Parameters(LambdaExpression lambda)
        {
            var parameters = new Type[lambda.Parameters.Count];
            for (int i = 0; i < parameters.Length; i++)
                parameters[i] = lambda.Parameters[i].Type;

            return parameters;
        }

        /// <summary>
        /// Returns the parameter types of a lambda followed by its result type.
        /// </summary>
        /// <param name="lambda">The lambda.</param>
        /// <returns>The type of each parameter, in order, and then the return type: the type arguments of a <c>Func</c> over the lambda.</returns>
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
