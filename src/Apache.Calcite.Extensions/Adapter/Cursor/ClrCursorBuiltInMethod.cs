using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrCursorConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, with members named after Calcite's so a node reads
    /// like its Calcite original. A generic method is stored as its open definition; a node closes it over its
    /// row types.
    ///
    /// <para>An unsuffixed member is an open that acquires its sources synchronously and is called from a node's
    /// <c>Implement</c>; the <c>Async</c>-suffixed member of the same name awaits its acquisition and is called
    /// from <c>ImplementAsync</c>. Both are in <see cref="ClrCursorDefaults"/> and produce the same cursor.</para>
    ///
    /// <para>Every awaiting open takes a trailing <see cref="CancellationToken"/>, which <see cref="CallAsync"/>
    /// supplies.</para>
    /// </remarks>
    static class ClrCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrCursorDefaults.Slice0"/>.
        /// </summary>
        public static readonly MethodInfo Slice0 = Of(nameof(ClrCursorDefaults.Slice0));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Calc"/>.
        /// </summary>
        public static readonly MethodInfo Calc = Of(nameof(ClrCursorDefaults.Calc));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Select"/>.
        /// </summary>
        public static readonly MethodInfo Select = Of(nameof(ClrCursorDefaults.Select));

        /// <summary>
        /// <see cref="ClrCursorDefaults.OrderBy"/>.
        /// </summary>
        public static readonly MethodInfo OrderBy = Of(nameof(ClrCursorDefaults.OrderBy));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Skip"/>.
        /// </summary>
        /// <remarks>
        /// <c>BuiltInMethod.SKIP_BIG_DECIMAL</c>: the offset is a <c>BigDecimal</c>, as <c>EnumerableLimit</c>
        /// passes it.
        /// </remarks>
        public static readonly MethodInfo SkipBigDecimal = Of(nameof(ClrCursorDefaults.Skip));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Take"/>.
        /// </summary>
        /// <remarks>
        /// <c>BuiltInMethod.TAKE_BIG_DECIMAL</c>: the fetch is a <c>BigDecimal</c>.
        /// </remarks>
        public static readonly MethodInfo TakeBigDecimal = Of(nameof(ClrCursorDefaults.Take));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Concat"/>.
        /// </summary>
        public static readonly MethodInfo Concat = Of(nameof(ClrCursorDefaults.Concat));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Union"/>.
        /// </summary>
        public static readonly MethodInfo Union = Of(nameof(ClrCursorDefaults.Union));

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsCursor{TSource}(TSource[])"/>, which a VALUES is built from.
        /// </summary>
        public static readonly MethodInfo AsCursorArray = Of(nameof(ClrCursorDefaults.AsCursor), p => p[0].ParameterType.IsArray);

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsCursor{TSource}(IEnumerable{TSource})"/>, which a scan of a table
        /// that returns an <see cref="IEnumerable{T}"/> is built from.
        /// </summary>
        public static readonly MethodInfo AsCursor = Of(nameof(ClrCursorDefaults.AsCursor), p => p[0].ParameterType.IsArray == false);

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsEnumerable{TSource}"/>, which presents an opener as a sequence that
        /// opens a cursor per enumeration.
        /// </summary>
        public static readonly MethodInfo AsEnumerable = Of(nameof(ClrCursorDefaults.AsEnumerable));

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsAsyncEnumerable{TSource}"/>.
        /// </summary>
        public static readonly MethodInfo AsAsyncEnumerable = Of(nameof(ClrCursorDefaults.AsAsyncEnumerable));

        /// <summary>
        /// <see cref="JavaCursors.FromJava{TSource}"/>, which reads a linq4j sequence as a cursor.
        /// </summary>
        public static readonly MethodInfo FromJava = typeof(JavaCursors).GetMethod(nameof(JavaCursors.FromJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaCursors.FromJava)}' is missing.");

        /// <summary>
        /// <see cref="ClrCursors.Block{T}"/>, which turns an awaiting open into a synchronous one by blocking the
        /// calling thread.
        /// </summary>
        public static readonly MethodInfo Block = typeof(ClrCursors).GetMethod(nameof(ClrCursors.Block))
            ?? throw new InvalidOperationException($"'{nameof(ClrCursors.Block)}' is missing.");

        // ---- the awaiting half ----

        // Each section lists its synchronous opens and then their awaiting counterparts, so a member added to
        // one set is visibly missing from the other.

        /// <summary>
        /// <see cref="ClrCursorDefaults.Slice0Async"/>.
        /// </summary>
        public static readonly MethodInfo Slice0Async = Of(nameof(ClrCursorDefaults.Slice0Async));

        /// <summary>
        /// <see cref="ClrCursorDefaults.CalcAsync"/>.
        /// </summary>
        public static readonly MethodInfo CalcAsync = Of(nameof(ClrCursorDefaults.CalcAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SelectAsync"/>.
        /// </summary>
        public static readonly MethodInfo SelectAsync = Of(nameof(ClrCursorDefaults.SelectAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.OrderByAsync"/>.
        /// </summary>
        public static readonly MethodInfo OrderByAsync = Of(nameof(ClrCursorDefaults.OrderByAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SkipAsync"/>.
        /// </summary>
        public static readonly MethodInfo SkipBigDecimalAsync = Of(nameof(ClrCursorDefaults.SkipAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.TakeAsync"/>.
        /// </summary>
        public static readonly MethodInfo TakeBigDecimalAsync = Of(nameof(ClrCursorDefaults.TakeAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.ConcatAsync"/>.
        /// </summary>
        public static readonly MethodInfo ConcatAsync = Of(nameof(ClrCursorDefaults.ConcatAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.UnionAsync"/>.
        /// </summary>
        public static readonly MethodInfo UnionAsync = Of(nameof(ClrCursorDefaults.UnionAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsCursorAsync{TSource}(TSource[], CancellationToken)"/>.
        /// </summary>
        public static readonly MethodInfo AsCursorArrayAsync = Of(nameof(ClrCursorDefaults.AsCursorAsync), p => p[0].ParameterType.IsArray);

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsCursorAsync{TSource}(IAsyncEnumerable{TSource}, CancellationToken)"/>.
        /// </summary>
        public static readonly MethodInfo AsCursorAsync = Of(nameof(ClrCursorDefaults.AsCursorAsync), p => p[0].ParameterType.IsArray == false);

        /// <summary>
        /// <see cref="JavaCursors.FromJavaAsync{TSource}"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaAsync = typeof(JavaCursors).GetMethod(nameof(JavaCursors.FromJavaAsync))
            ?? throw new InvalidOperationException($"'{nameof(JavaCursors.FromJavaAsync)}' is missing.");

        /// <summary>
        /// <see cref="ClrCursors.Completed{T}"/>, which turns a synchronous open into an awaiting one.
        /// </summary>
        public static readonly MethodInfo Completed = typeof(ClrCursors).GetMethod(nameof(ClrCursors.Completed))
            ?? throw new InvalidOperationException($"'{nameof(ClrCursors.Completed)}' is missing.");

        /// <summary>
        /// <see cref="ClrCursors.Untyped{T}"/>, which the awaiting root ends in.
        /// </summary>
        public static readonly MethodInfo Untyped = typeof(ClrCursors).GetMethod(nameof(ClrCursors.Untyped))
            ?? throw new InvalidOperationException($"'{nameof(ClrCursors.Untyped)}' is missing.");

        /// <summary>
        /// Builds a call to an awaiting open, appending the token it ends in.
        /// </summary>
        /// <param name="implementor">The implementor, whose token parameter is passed.</param>
        /// <param name="method">The open, with its type arguments already applied.</param>
        /// <param name="arguments">The arguments, less the cancellation token.</param>
        /// <returns>The call expression.</returns>
        /// <exception cref="InvalidOperationException">
        /// <paramref name="method"/> does not take exactly one more parameter than <paramref name="arguments"/>
        /// supplies, or its last parameter is not a <see cref="CancellationToken"/>.
        /// </exception>
        /// <remarks>
        /// The token passed is <see cref="ClrCursorRelImplementor.CancellationToken"/>, the parameter declared
        /// by the awaiting root's lambda or redeclared by a deferred opener's, so every acquisition runs with the
        /// token the caller passed to the open or advance that reached it. An expression tree does not apply
        /// default arguments, so the token has to be passed explicitly.
        /// </remarks>
        public static MethodCallExpression CallAsync(ClrCursorRelImplementor implementor, MethodInfo method, params Expression[] arguments)
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
        /// Finds a public static method of <see cref="ClrCursorDefaults"/> by name.
        /// </summary>
        /// <param name="name">The method name.</param>
        /// <param name="matches">Distinguishes overloads by their parameters, or <see langword="null"/> where
        /// the name is unique.</param>
        /// <returns>The method.</returns>
        /// <exception cref="InvalidOperationException">No method, or more than one, matches.</exception>
        static MethodInfo Of(string name, Func<ParameterInfo[], bool>? matches = null)
        {
            MethodInfo? found = null;

            foreach (var method in typeof(ClrCursorDefaults).GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (method.Name != name || (matches != null && matches(method.GetParameters()) == false))
                    continue;

                if (found != null)
                    throw new InvalidOperationException($"'{name}' is ambiguous in {nameof(ClrCursorDefaults)}; distinguish its overloads.");

                found = method;
            }

            return found ?? throw new InvalidOperationException($"'{name}' is missing from {nameof(ClrCursorDefaults)}.");
        }

        // ---- Aggregate ----


        /// <summary>
        /// <see cref="ClrCursorDefaults.Distinct"/>.
        /// </summary>
        public static readonly MethodInfo Distinct = Of(nameof(ClrCursorDefaults.Distinct));

        /// <summary>
        /// <see cref="ClrCursorDefaults.GroupBy"/>.
        /// </summary>
        public static readonly MethodInfo GroupBy = Of(nameof(ClrCursorDefaults.GroupBy));

        /// <summary>
        /// <see cref="ClrCursorDefaults.GroupByMultiple"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultiple = Of(nameof(ClrCursorDefaults.GroupByMultiple));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SortedGroupBy"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupBy = Of(nameof(ClrCursorDefaults.SortedGroupBy));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Aggregate"/>.
        /// </summary>
        public static readonly MethodInfo Aggregate = Of(nameof(ClrCursorDefaults.Aggregate));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo Singleton = Of(nameof(ClrCursorDefaults.Singleton));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.DistinctAsync"/>.
        /// </summary>
        public static readonly MethodInfo DistinctAsync = Of(nameof(ClrCursorDefaults.DistinctAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.GroupByAsync"/>.
        /// </summary>
        public static readonly MethodInfo GroupByAsync = Of(nameof(ClrCursorDefaults.GroupByAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.GroupByMultipleAsync"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultipleAsync = Of(nameof(ClrCursorDefaults.GroupByMultipleAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SortedGroupByAsync"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupByAsync = Of(nameof(ClrCursorDefaults.SortedGroupByAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SingletonAggregateAsync"/>: <see cref="Singleton"/> over
        /// <see cref="Aggregate"/> as one open, since the fold has to be awaited.
        /// </summary>
        public static readonly MethodInfo SingletonAggregateAsync = Of(nameof(ClrCursorDefaults.SingletonAggregateAsync));

        // ---- Collect ----
        // The methods a collect, an uncollect, a combine and a table function scan are built from.


        /// <summary>
        /// <see cref="ClrCursorDefaults.SelectMany"/>.
        /// </summary>
        public static readonly MethodInfo SelectMany = Of(nameof(ClrCursorDefaults.SelectMany));

        /// <summary>
        /// <see cref="ClrCursorDefaults.ToJavaList"/>.
        /// </summary>
        public static readonly MethodInfo ToJavaList = Of(nameof(ClrCursorDefaults.ToJavaList));

        /// <summary>
        /// <see cref="ClrCursorDefaults.FromJavaList"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaList = Of(nameof(ClrCursorDefaults.FromJavaList));

        /// <summary>
        /// <see cref="ClrCursorDefaults.ToJavaMap"/>.
        /// </summary>
        public static readonly MethodInfo ToJavaMap = Of(nameof(ClrCursorDefaults.ToJavaMap));

        /// <summary>
        /// <see cref="JavaSequences.ToJava"/>, through which a window table function passes its input to
        /// Calcite's generator.
        /// </summary>
        public static readonly MethodInfo ToJava = typeof(JavaSequences).GetMethod(nameof(JavaSequences.ToJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaSequences.ToJava)}' is missing.");

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.SelectManyAsync"/>.
        /// </summary>
        public static readonly MethodInfo SelectManyAsync = Of(nameof(ClrCursorDefaults.SelectManyAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.FromJavaListAsync"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaListAsync = Of(nameof(ClrCursorDefaults.FromJavaListAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SingletonJavaListAsync"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaListAsync = Of(nameof(ClrCursorDefaults.SingletonJavaListAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SingletonJavaMapAsync"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaMapAsync = Of(nameof(ClrCursorDefaults.SingletonJavaMapAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.CombineQueryResultsAsync"/>.
        /// </summary>
        public static readonly MethodInfo CombineQueryResultsAsync = Of(nameof(ClrCursorDefaults.CombineQueryResultsAsync));

        // ---- Correlate ----


        /// <summary>
        /// <see cref="ClrCursorDefaults.CorrelateJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoin = Of(nameof(ClrCursorDefaults.CorrelateJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.CorrelateLeftMarkJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoin = Of(nameof(ClrCursorDefaults.CorrelateLeftMarkJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.CorrelateBatchJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoin = Of(nameof(ClrCursorDefaults.CorrelateBatchJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsofJoin"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoin = Of(nameof(ClrCursorDefaults.AsofJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.IeJoin"/>.
        /// </summary>
        public static readonly MethodInfo IeJoin = Of(nameof(ClrCursorDefaults.IeJoin));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.CorrelateJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoinAsync = Of(nameof(ClrCursorDefaults.CorrelateJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.CorrelateLeftMarkJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoinAsync = Of(nameof(ClrCursorDefaults.CorrelateLeftMarkJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.CorrelateBatchJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoinAsync = Of(nameof(ClrCursorDefaults.CorrelateBatchJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsofJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoinAsync = Of(nameof(ClrCursorDefaults.AsofJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.IeJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo IeJoinAsync = Of(nameof(ClrCursorDefaults.IeJoinAsync));

        // ---- Join ----
        // The joins.


        /// <summary>
        /// <see cref="ClrCursorDefaults.HashJoin"/>.
        /// </summary>
        public static readonly MethodInfo HashJoin = Of(nameof(ClrCursorDefaults.HashJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SemiJoin"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoin = Of(nameof(ClrCursorDefaults.SemiJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.MergeJoin"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoin = Of(nameof(ClrCursorDefaults.MergeJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.NestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoin = Of(nameof(ClrCursorDefaults.NestedLoopJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.LeftMarkNestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoin = Of(nameof(ClrCursorDefaults.LeftMarkNestedLoopJoin));

        /// <summary>
        /// <see cref="ClrCursorDefaults.LeftMarkHashJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoin = Of(nameof(ClrCursorDefaults.LeftMarkHashJoin));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.HashJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo HashJoinAsync = Of(nameof(ClrCursorDefaults.HashJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.SemiJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoinAsync = Of(nameof(ClrCursorDefaults.SemiJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.MergeJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoinAsync = Of(nameof(ClrCursorDefaults.MergeJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.NestedLoopJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoinAsync = Of(nameof(ClrCursorDefaults.NestedLoopJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.LeftMarkNestedLoopJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoinAsync = Of(nameof(ClrCursorDefaults.LeftMarkNestedLoopJoinAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.LeftMarkHashJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoinAsync = Of(nameof(ClrCursorDefaults.LeftMarkHashJoinAsync));

        // ---- Recursion ----


        /// <summary>
        /// <see cref="ClrCursorDefaults.LazyCollectionSpool"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpool = Of(nameof(ClrCursorDefaults.LazyCollectionSpool));

        /// <summary>
        /// <see cref="ClrCursorDefaults.RepeatUnion"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnion = Of(nameof(ClrCursorDefaults.RepeatUnion));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.LazyCollectionSpoolAsync"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpoolAsync = Of(nameof(ClrCursorDefaults.LazyCollectionSpoolAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.RepeatUnionAsync"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnionAsync = Of(nameof(ClrCursorDefaults.RepeatUnionAsync));

        // ---- SetOp ----


        /// <summary>
        /// <see cref="ClrCursorDefaults.Intersect"/>.
        /// </summary>
        public static readonly MethodInfo Intersect = Of(nameof(ClrCursorDefaults.Intersect));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Except"/>.
        /// </summary>
        public static readonly MethodInfo Except = Of(nameof(ClrCursorDefaults.Except));

        /// <summary>
        /// <see cref="ClrCursorDefaults.MergeUnion"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnion = Of(nameof(ClrCursorDefaults.MergeUnion));

        /// <summary>
        /// <see cref="ClrCursorDefaults.OrderByWithFetchAndOffset"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffset = Of(nameof(ClrCursorDefaults.OrderByWithFetchAndOffset));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.IntersectAsync"/>.
        /// </summary>
        public static readonly MethodInfo IntersectAsync = Of(nameof(ClrCursorDefaults.IntersectAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.ExceptAsync"/>.
        /// </summary>
        public static readonly MethodInfo ExceptAsync = Of(nameof(ClrCursorDefaults.ExceptAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.MergeUnionAsync"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnionAsync = Of(nameof(ClrCursorDefaults.MergeUnionAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.OrderByWithFetchAndOffsetAsync"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffsetAsync = Of(nameof(ClrCursorDefaults.OrderByWithFetchAndOffsetAsync));

        // ---- Window ----


        /// <summary>
        /// <see cref="ClrCursorDefaults.Window"/>.
        /// </summary>
        public static readonly MethodInfo Window = Of(nameof(ClrCursorDefaults.Window));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.WindowAsync"/>.
        /// </summary>
        public static readonly MethodInfo WindowAsync = Of(nameof(ClrCursorDefaults.WindowAsync));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Match"/>.
        /// </summary>
        public static readonly MethodInfo Match = Of(nameof(ClrCursorDefaults.Match));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrCursorDefaults.MatchAsync"/>.
        /// </summary>
        public static readonly MethodInfo MatchAsync = Of(nameof(ClrCursorDefaults.MatchAsync));

    }

}
