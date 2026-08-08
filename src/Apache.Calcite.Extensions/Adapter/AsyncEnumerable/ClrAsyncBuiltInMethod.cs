using System;
using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrAsyncEnumerableConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// <see cref="Apache.Calcite.Extensions.Adapter.Enumerable.ClrBuiltInMethod"/>, name for name, over the
    /// asynchronous operator set. This is the whole of what a node of this convention names differently from
    /// its synchronous counterpart: a node reads a member here where the other reads one there, and the rest
    /// of its <c>Implement</c> is the same code.
    ///
    /// <para>Only the operators the nodes written so far need are here. A member is added when the node that
    /// calls it is written, so that a name in this class always has an operator and a caller.</para>
    /// </remarks>
    static class ClrAsyncBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Slice0"/>.
        /// </summary>
        public static readonly MethodInfo Slice0 = Of(nameof(ClrAsyncEnumerableDefaults.Slice0));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Calc"/>.
        /// </summary>
        public static readonly MethodInfo Calc = Of(nameof(ClrAsyncEnumerableDefaults.Calc));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Where"/>.
        /// </summary>
        public static readonly MethodInfo Where = Of(nameof(ClrAsyncEnumerableDefaults.Where));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Select{TSource, TResult}"/>.
        /// </summary>
        public static readonly MethodInfo Select = Of(nameof(ClrAsyncEnumerableDefaults.Select));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.OrderBy"/>.
        /// </summary>
        public static readonly MethodInfo OrderBy = Of(nameof(ClrAsyncEnumerableDefaults.OrderBy));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Skip"/>.
        /// </summary>
        public static readonly MethodInfo Skip = Of(nameof(ClrAsyncEnumerableDefaults.Skip));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Take"/>.
        /// </summary>
        public static readonly MethodInfo Take = Of(nameof(ClrAsyncEnumerableDefaults.Take));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.OrderByWithFetchAndOffset"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffset = Of(nameof(ClrAsyncEnumerableDefaults.OrderByWithFetchAndOffset));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.AsAsyncEnumerable"/>.
        /// </summary>
        public static readonly MethodInfo AsAsyncEnumerable = Of(nameof(ClrAsyncEnumerableDefaults.AsAsyncEnumerable));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo Singleton = Of(nameof(ClrAsyncEnumerableDefaults.Singleton));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Empty"/>.
        /// </summary>
        public static readonly MethodInfo Empty = Of(nameof(ClrAsyncEnumerableDefaults.Empty));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.FromJavaList"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaList = Of(nameof(ClrAsyncEnumerableDefaults.FromJavaList));

        /// <summary>
        /// Builds the call to an operator of this class.
        /// </summary>
        /// <param name="method">The operator, with its type arguments already applied.</param>
        /// <param name="arguments">The arguments, less the cancellation token.</param>
        /// <returns></returns>
        /// <remarks>
        /// A node calls this where its synchronous counterpart calls <c>Expression.Call(null, …)</c>, and the
        /// difference is the whole reason it exists: every operator here takes a trailing
        /// <see cref="System.Threading.CancellationToken"/>, and an expression tree does not apply a default
        /// argument — <c>Expression.Call</c> requires one expression per parameter. So the token is appended
        /// here rather than written out at thirty call sites.
        ///
        /// <para>The value appended is <c>default</c>, and that is not a token being thrown away. It is what
        /// <c>[EnumeratorCancellation]</c> reads: the compiler's iterator uses the token given to
        /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}.GetAsyncEnumerator"/> in place of a
        /// parameter that arrived as <c>default</c>. Passing <c>default</c> from the plan is what lets the
        /// caller's token reach every operator without the plan carrying one.</para>
        /// </remarks>
        public static System.Linq.Expressions.MethodCallExpression Call(MethodInfo method, params System.Linq.Expressions.Expression[] arguments)
        {
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(arguments);

            var parameters = method.GetParameters();
            if (parameters.Length != arguments.Length + 1)
                throw new InvalidOperationException($"{method.Name} takes {parameters.Length} arguments and was given {arguments.Length} plus a token.");
            if (parameters[^1].ParameterType != typeof(System.Threading.CancellationToken))
                throw new InvalidOperationException($"{method.Name} does not end in a {nameof(System.Threading.CancellationToken)}.");

            var all = new System.Linq.Expressions.Expression[arguments.Length + 1];
            arguments.CopyTo(all, 0);
            all[^1] = System.Linq.Expressions.Expression.Default(typeof(System.Threading.CancellationToken));

            return System.Linq.Expressions.Expression.Call(null, method, all);
        }

        /// <summary>
        /// Returns the named operator.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static MethodInfo Of(string name)
        {
            return typeof(ClrAsyncEnumerableDefaults).GetMethod(name, BindingFlags.Public | BindingFlags.Static)
                ?? throw new InvalidOperationException($"'{name}' is missing from {nameof(ClrAsyncEnumerableDefaults)}.");
        }

    }

}
