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
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, named the same way, so a node reads as its Calcite
    /// original does. A generic one is the open definition, and a node closes it over the row type it is
    /// working with.
    ///
    /// <para><b>One table, two sets of names.</b> An unsuffixed member is an open that acquires
    /// synchronously, which a node's <c>Implement</c> names; the <c>Async</c>-suffixed member of the same
    /// name is the open that awaits its acquisition, which its <c>ImplementAsync</c> names. Both are in
    /// <see cref="ClrCursorDefaults"/> and both produce the same cursor.</para>
    ///
    /// <para><b>Every awaiting open ends in a <see cref="CancellationToken"/>, and <see cref="CallAsync"/>
    /// supplies it</b> — the implementor's token parameter, which the awaiting root's lambda declares. An
    /// expression tree does not apply a default argument, so the token is appended there rather than
    /// written out at every call site.</para>
    ///
    /// <para>A member is added when the node that calls it is written, so that a name here always has a
    /// caller.</para>
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
        /// <c>BuiltInMethod.SKIP_BIG_DECIMAL</c>: a FETCH or an OFFSET arrives as a <c>BigDecimal</c>, which
        /// is CALCITE-7624.
        /// </remarks>
        public static readonly MethodInfo SkipBigDecimal = Of(nameof(ClrCursorDefaults.Skip));

        /// <summary>
        /// <see cref="ClrCursorDefaults.Take"/>.
        /// </summary>
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
        /// <see cref="ClrCursorDefaults.AsCursor{TSource}(IEnumerable{TSource})"/>, which a scan of
        /// this project's own table SPI is built from.
        /// </summary>
        public static readonly MethodInfo AsCursor = Of(nameof(ClrCursorDefaults.AsCursor), p => p[0].ParameterType.IsArray == false);

        /// <summary>
        /// <see cref="ClrCursorDefaults.AsEnumerable{TSource}"/>, which a converter into the sequence
        /// convention reads a cursor plan through.
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
        /// <see cref="ClrCursors.Block{T}"/>, which reads an awaiting open across to a synchronous one by
        /// blocking for it.
        /// </summary>
        public static readonly MethodInfo Block = typeof(ClrCursors).GetMethod(nameof(ClrCursors.Block))
            ?? throw new InvalidOperationException($"'{nameof(ClrCursors.Block)}' is missing.");

        // ---- the awaiting half ----

        // The same opens awaiting their acquisition, named for the synchronous one they answer to with
        // Async on the end. A node's Implement names the first set and its ImplementAsync the second;
        // nothing dispatches, and the two are together here so that a member added to one is obviously
        // missing from the other.

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
        /// <see cref="ClrCursors.Completed{T}"/>, which reads a synchronous open across to an awaiting
        /// one.
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
        /// <returns></returns>
        /// <remarks>
        /// A node calls this where its synchronous body calls <c>Expression.Call(null, …)</c>. Every
        /// awaiting open takes a trailing <see cref="CancellationToken"/> and an expression tree does not
        /// apply a default argument, so the token is appended here. It is the implementor's parameter —
        /// the one the awaiting root's lambda declares, or the one a deferred open's lambda redeclares — so
        /// that the token a caller gives an open, or an advance, is the token every acquisition under it
        /// runs with.
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
        /// <param name="name"></param>
        /// <param name="matches">Distinguishes overloads by their parameters, or <see langword="null"/> where
        /// the name is unique.</param>
        /// <returns></returns>
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
        /// <see cref="ClrCursorDefaults.SingletonAggregateAsync"/>, which is <see cref="Singleton"/>
        /// over <see cref="Aggregate"/> as one open, the fold being awaited.
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
        /// <see cref="JavaSequences.ToJava"/>, which a window table function hands its input to Calcite's
        /// generator through.
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

    }

}
