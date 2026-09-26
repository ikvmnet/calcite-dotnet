using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrDataCursorConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, named the same way, so a node reads as its Calcite
    /// original does. A generic one is the open definition, and a node closes it over the row type it is
    /// working with.
    ///
    /// <para><b>One table, two sets of names.</b> An unsuffixed member is an open that acquires
    /// synchronously, which a node's <c>Implement</c> names; the <c>Async</c>-suffixed member of the same
    /// name is the open that awaits its acquisition, which its <c>ImplementAsync</c> names. Both are in
    /// <see cref="ClrDataCursorDefaults"/> and both produce the same cursor.</para>
    ///
    /// <para><b>Every awaiting open ends in a <see cref="CancellationToken"/>, and <see cref="CallAsync"/>
    /// supplies it</b> — the implementor's token parameter, which the awaiting root's lambda declares. An
    /// expression tree does not apply a default argument, so the token is appended there rather than
    /// written out at every call site.</para>
    ///
    /// <para>A member is added when the node that calls it is written, so that a name here always has a
    /// caller.</para>
    /// </remarks>
    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Slice0"/>.
        /// </summary>
        public static readonly MethodInfo Slice0 = Of(nameof(ClrDataCursorDefaults.Slice0));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Calc"/>.
        /// </summary>
        public static readonly MethodInfo Calc = Of(nameof(ClrDataCursorDefaults.Calc));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Select"/>.
        /// </summary>
        public static readonly MethodInfo Select = Of(nameof(ClrDataCursorDefaults.Select));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.OrderBy"/>.
        /// </summary>
        public static readonly MethodInfo OrderBy = Of(nameof(ClrDataCursorDefaults.OrderBy));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Skip"/>.
        /// </summary>
        /// <remarks>
        /// <c>BuiltInMethod.SKIP_BIG_DECIMAL</c>: a FETCH or an OFFSET arrives as a <c>BigDecimal</c>, which
        /// is CALCITE-7624.
        /// </remarks>
        public static readonly MethodInfo SkipBigDecimal = Of(nameof(ClrDataCursorDefaults.Skip));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Take"/>.
        /// </summary>
        public static readonly MethodInfo TakeBigDecimal = Of(nameof(ClrDataCursorDefaults.Take));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Concat"/>.
        /// </summary>
        public static readonly MethodInfo Concat = Of(nameof(ClrDataCursorDefaults.Concat));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Union"/>.
        /// </summary>
        public static readonly MethodInfo Union = Of(nameof(ClrDataCursorDefaults.Union));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsCursor{TSource}(TSource[])"/>, which a VALUES is built from.
        /// </summary>
        public static readonly MethodInfo AsCursorArray = Of(nameof(ClrDataCursorDefaults.AsCursor), p => p[0].ParameterType.IsArray);

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsCursor{TSource}(IEnumerable{TSource})"/>, which a scan of
        /// this project's own table SPI is built from.
        /// </summary>
        public static readonly MethodInfo AsCursor = Of(nameof(ClrDataCursorDefaults.AsCursor), p => p[0].ParameterType.IsArray == false);

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsEnumerable{TSource}"/>, which a converter into the sequence
        /// convention reads a cursor plan through.
        /// </summary>
        public static readonly MethodInfo AsEnumerable = Of(nameof(ClrDataCursorDefaults.AsEnumerable));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsAsyncEnumerable{TSource}"/>.
        /// </summary>
        public static readonly MethodInfo AsAsyncEnumerable = Of(nameof(ClrDataCursorDefaults.AsAsyncEnumerable));

        /// <summary>
        /// <see cref="JavaCursors.FromJava{TSource}"/>, which reads a linq4j sequence as a cursor.
        /// </summary>
        public static readonly MethodInfo FromJava = typeof(JavaCursors).GetMethod(nameof(JavaCursors.FromJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaCursors.FromJava)}' is missing.");

        /// <summary>
        /// <see cref="ClrDataCursors.Block{T}"/>, which reads an awaiting open across to a synchronous one by
        /// blocking for it.
        /// </summary>
        public static readonly MethodInfo Block = typeof(ClrDataCursors).GetMethod(nameof(ClrDataCursors.Block))
            ?? throw new InvalidOperationException($"'{nameof(ClrDataCursors.Block)}' is missing.");

        // ---- the awaiting half ----

        // The same opens awaiting their acquisition, named for the synchronous one they answer to with
        // Async on the end. A node's Implement names the first set and its ImplementAsync the second;
        // nothing dispatches, and the two are together here so that a member added to one is obviously
        // missing from the other.

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Slice0Async"/>.
        /// </summary>
        public static readonly MethodInfo Slice0Async = Of(nameof(ClrDataCursorDefaults.Slice0Async));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CalcAsync"/>.
        /// </summary>
        public static readonly MethodInfo CalcAsync = Of(nameof(ClrDataCursorDefaults.CalcAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SelectAsync"/>.
        /// </summary>
        public static readonly MethodInfo SelectAsync = Of(nameof(ClrDataCursorDefaults.SelectAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.OrderByAsync"/>.
        /// </summary>
        public static readonly MethodInfo OrderByAsync = Of(nameof(ClrDataCursorDefaults.OrderByAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SkipAsync"/>.
        /// </summary>
        public static readonly MethodInfo SkipBigDecimalAsync = Of(nameof(ClrDataCursorDefaults.SkipAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.TakeAsync"/>.
        /// </summary>
        public static readonly MethodInfo TakeBigDecimalAsync = Of(nameof(ClrDataCursorDefaults.TakeAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.ConcatAsync"/>.
        /// </summary>
        public static readonly MethodInfo ConcatAsync = Of(nameof(ClrDataCursorDefaults.ConcatAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.UnionAsync"/>.
        /// </summary>
        public static readonly MethodInfo UnionAsync = Of(nameof(ClrDataCursorDefaults.UnionAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsCursorAsync{TSource}(TSource[], CancellationToken)"/>.
        /// </summary>
        public static readonly MethodInfo AsCursorArrayAsync = Of(nameof(ClrDataCursorDefaults.AsCursorAsync), p => p[0].ParameterType.IsArray);

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsCursorAsync{TSource}(IAsyncEnumerable{TSource}, CancellationToken)"/>.
        /// </summary>
        public static readonly MethodInfo AsCursorAsync = Of(nameof(ClrDataCursorDefaults.AsCursorAsync), p => p[0].ParameterType.IsArray == false);

        /// <summary>
        /// <see cref="JavaCursors.FromJavaAsync{TSource}"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaAsync = typeof(JavaCursors).GetMethod(nameof(JavaCursors.FromJavaAsync))
            ?? throw new InvalidOperationException($"'{nameof(JavaCursors.FromJavaAsync)}' is missing.");

        /// <summary>
        /// <see cref="ClrDataCursors.Completed{T}"/>, which reads a synchronous open across to an awaiting
        /// one.
        /// </summary>
        public static readonly MethodInfo Completed = typeof(ClrDataCursors).GetMethod(nameof(ClrDataCursors.Completed))
            ?? throw new InvalidOperationException($"'{nameof(ClrDataCursors.Completed)}' is missing.");

        /// <summary>
        /// <see cref="ClrDataCursors.Untyped{T}"/>, which the awaiting root ends in.
        /// </summary>
        public static readonly MethodInfo Untyped = typeof(ClrDataCursors).GetMethod(nameof(ClrDataCursors.Untyped))
            ?? throw new InvalidOperationException($"'{nameof(ClrDataCursors.Untyped)}' is missing.");

        /// <summary>
        /// Builds a call to an awaiting open, appending the token it ends in.
        /// </summary>
        /// <param name="implementor">The implementor, whose token parameter is passed.</param>
        /// <param name="method">The open, with its type arguments already applied.</param>
        /// <param name="arguments">The arguments, less the cancellation token.</param>
        /// <returns></returns>
        /// <remarks>
        /// A node calls this where its synchronous body calls <c>Expression.Call(null, …)</c>. Every
        /// awaiting open takes a trailing <see cref="CancellationToken"/> and an expression tree does not
        /// apply a default argument, so the token is appended here. It is the implementor's parameter —
        /// the one the awaiting root's lambda declares, or the one a deferred open's lambda redeclares — so
        /// that the token a caller gives an open, or an advance, is the token every acquisition under it
        /// runs with.
        /// </remarks>
        public static MethodCallExpression CallAsync(ClrDataCursorRelImplementor implementor, MethodInfo method, params Expression[] arguments)
        {
            ArgumentNullException.ThrowIfNull(implementor);
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(arguments);

            var parameters = method.GetParameters();
            if (parameters.Length != arguments.Length + 1)
                throw new InvalidOperationException($"{method.Name} takes {parameters.Length} arguments and was given {arguments.Length} plus a token.");
            if (parameters[^1].ParameterType != typeof(CancellationToken))
                throw new InvalidOperationException($"{method.Name} does not end in a {nameof(CancellationToken)}.");

            var all = new Expression[arguments.Length + 1];
            arguments.CopyTo(all, 0);
            all[^1] = implementor.CancellationToken;

            return Expression.Call(null, method, all);
        }

        /// <summary>
        /// Finds a public static method of <see cref="ClrDataCursorDefaults"/> by name.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="matches">Distinguishes overloads by their parameters, or <see langword="null"/> where
        /// the name is unique.</param>
        /// <returns></returns>
        static MethodInfo Of(string name, Func<ParameterInfo[], bool>? matches = null)
        {
            MethodInfo? found = null;

            foreach (var method in typeof(ClrDataCursorDefaults).GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (method.Name != name || (matches != null && matches(method.GetParameters()) == false))
                    continue;

                if (found != null)
                    throw new InvalidOperationException($"'{name}' is ambiguous in {nameof(ClrDataCursorDefaults)}; distinguish its overloads.");

                found = method;
            }

            return found ?? throw new InvalidOperationException($"'{name}' is missing from {nameof(ClrDataCursorDefaults)}.");
        }

    }

}
