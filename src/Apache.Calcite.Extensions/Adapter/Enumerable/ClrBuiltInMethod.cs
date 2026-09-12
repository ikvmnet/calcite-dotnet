using System;
using System.Reflection;

using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrEnumerableConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, named the same way, so a node reads as its Calcite
    /// original does. A generic one is the open definition, and a node closes it over the row type it is
    /// working with.
    ///
    /// <para><b>One table, two sets of names.</b> An unsuffixed member is the operator over
    /// <see cref="System.Collections.Generic.IEnumerable{T}"/> in <see cref="ClrEnumerableDefaults"/>, which
    /// a node's <c>Implement</c> names; the <c>Async</c>-suffixed member of the same name is the operator
    /// over <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> in
    /// <see cref="ClrEnumerableDefaults"/>, which its <c>ImplementAsync</c> names. Both are static
    /// fields and a node names one of them outright: which operator a call lands on is decided where the
    /// node is written, not by anything the plan carries. The planner never sees the difference, because
    /// there is no difference to see — the rows, the physical type, the Rex translation and the tree are the
    /// same.</para>
    ///
    /// <para><b>Every asynchronous operator ends in a <see cref="System.Threading.CancellationToken"/>, and
    /// <see cref="CallAsync"/> supplies it.</b> An expression tree does not apply a default argument, so the
    /// token is appended there, as <c>default</c>, rather than written out at forty call sites. That is not
    /// a token being thrown away: it is what <c>[EnumeratorCancellation]</c> reads, the compiler's iterator
    /// using the token given to <c>GetAsyncEnumerator</c> in place of a parameter that arrived as
    /// <c>default</c>. Passing <c>default</c> from the plan is what lets the caller's token reach every
    /// operator without the plan carrying one.</para>
    ///
    /// <para>A member is added when the node that calls it is written, so that a name here always has a
    /// caller.</para>
    /// </remarks>
    static class ClrBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Slice0"/>.
        /// </summary>
        public static readonly MethodInfo Slice0 = Of(nameof(ClrEnumerableDefaults.Slice0));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Calc"/>.
        /// </summary>
        public static readonly MethodInfo Calc = Of(nameof(ClrEnumerableDefaults.Calc));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Where"/>.
        /// </summary>
        public static readonly MethodInfo Where = Of(nameof(ClrEnumerableDefaults.Where));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Select{TSource, TResult}"/>.
        /// </summary>
        public static readonly MethodInfo Select = Of(nameof(ClrEnumerableDefaults.Select));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.OrderBy"/>.
        /// </summary>
        public static readonly MethodInfo OrderBy = Of(nameof(ClrEnumerableDefaults.OrderBy));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Skip"/>.
        /// </summary>
        public static readonly MethodInfo Skip = Of(nameof(ClrEnumerableDefaults.Skip));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Take{TSource}(System.Collections.Generic.IEnumerable{TSource}, int)"/>.
        /// </summary>
        /// <remarks>
        /// The <c>int</c> overload, which is <c>BuiltInMethod.TAKE</c>; a plan's fetch is an <c>int</c>. The
        /// <c>long</c> one is the row limit a caller asks a prepared statement for.
        /// </remarks>
        public static readonly MethodInfo Take = Of(nameof(ClrEnumerableDefaults.Take), null, typeof(int));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Concat"/>.
        /// </summary>
        public static readonly MethodInfo Concat = Of(nameof(ClrEnumerableDefaults.Concat));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Union"/>.
        /// </summary>
        public static readonly MethodInfo Union = Of(nameof(ClrEnumerableDefaults.Union));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Intersect"/>.
        /// </summary>
        public static readonly MethodInfo Intersect = Of(nameof(ClrEnumerableDefaults.Intersect));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Except"/>.
        /// </summary>
        public static readonly MethodInfo Except = Of(nameof(ClrEnumerableDefaults.Except));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Distinct"/>.
        /// </summary>
        public static readonly MethodInfo Distinct = Of(nameof(ClrEnumerableDefaults.Distinct));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.HashJoin"/>.
        /// </summary>
        public static readonly MethodInfo HashJoin = Of(nameof(ClrEnumerableDefaults.HashJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SemiJoin"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoin = Of(nameof(ClrEnumerableDefaults.SemiJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.MergeUnion"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnion = Of(nameof(ClrEnumerableDefaults.MergeUnion));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.MergeJoin"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoin = Of(nameof(ClrEnumerableDefaults.MergeJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.AsofJoin"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoin = Of(nameof(ClrEnumerableDefaults.AsofJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.NestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoin = Of(nameof(ClrEnumerableDefaults.NestedLoopJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LeftMarkNestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoin = Of(nameof(ClrEnumerableDefaults.LeftMarkNestedLoopJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LeftMarkHashJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoin = Of(nameof(ClrEnumerableDefaults.LeftMarkHashJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateLeftMarkJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoin = Of(nameof(ClrEnumerableDefaults.CorrelateLeftMarkJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateBatchJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoin = Of(nameof(ClrEnumerableDefaults.CorrelateBatchJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoin = Of(nameof(ClrEnumerableDefaults.CorrelateJoin));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.GroupBy"/>.
        /// </summary>
        public static readonly MethodInfo GroupBy = Of(nameof(ClrEnumerableDefaults.GroupBy));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.GroupByMultiple"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultiple = Of(nameof(ClrEnumerableDefaults.GroupByMultiple));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SortedGroupBy"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupBy = Of(nameof(ClrEnumerableDefaults.SortedGroupBy));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Aggregate"/>.
        /// </summary>
        public static readonly MethodInfo Aggregate = Of(nameof(ClrEnumerableDefaults.Aggregate));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Window"/>.
        /// </summary>
        public static readonly MethodInfo Window = Of(nameof(ClrEnumerableDefaults.Window));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SelectMany"/>.
        /// </summary>
        public static readonly MethodInfo SelectMany = Of(nameof(ClrEnumerableDefaults.SelectMany));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.OrderByWithFetchAndOffset"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffset = Of(nameof(ClrEnumerableDefaults.OrderByWithFetchAndOffset));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.ToJavaList"/>.
        /// </summary>
        public static readonly MethodInfo ToJavaList = Of(nameof(ClrEnumerableDefaults.ToJavaList));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.FromJavaList"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaList = Of(nameof(ClrEnumerableDefaults.FromJavaList));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.ToJavaMap"/>.
        /// </summary>
        public static readonly MethodInfo ToJavaMap = Of(nameof(ClrEnumerableDefaults.ToJavaMap));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LazyCollectionSpool"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpool = Of(nameof(ClrEnumerableDefaults.LazyCollectionSpool));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.RepeatUnion"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnion = Of(nameof(ClrEnumerableDefaults.RepeatUnion));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.AsEnumerable"/>.
        /// </summary>
        public static readonly MethodInfo AsEnumerable = Of(nameof(ClrEnumerableDefaults.AsEnumerable));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo Singleton = Of(nameof(ClrEnumerableDefaults.Singleton));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Empty"/>.
        /// </summary>
        public static readonly MethodInfo Empty = Of(nameof(ClrEnumerableDefaults.Empty));

        /// <summary>
        /// <see cref="JavaSequences.FromJava"/>.
        /// </summary>
        public static readonly MethodInfo FromJava = typeof(JavaSequences).GetMethod(nameof(JavaSequences.FromJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaSequences.FromJava)}' is missing.");

        /// <summary>
        /// <see cref="Runtime.ClrSequences.ToEnumerable{TSource}"/>, which reads an awaited sequence by
        /// blocking a thread on each row.
        /// </summary>
        /// <remarks>
        /// What a plan of this kind is given when a node hands up the other kind. An
        /// <see cref="System.Collections.Generic.IEnumerable{T}"/> has nowhere to suspend, so there is no
        /// version of this that does not block; the node that made that necessary is the one to look at.
        /// </remarks>
        public static readonly MethodInfo ToEnumerable = typeof(Runtime.ClrSequences).GetMethod(nameof(Runtime.ClrSequences.ToEnumerable))
            ?? throw new InvalidOperationException($"'{nameof(Runtime.ClrSequences.ToEnumerable)}' is missing.");

        /// <summary>
        /// <see cref="JavaSequences.ToJava"/>.
        /// </summary>
        public static readonly MethodInfo ToJava = typeof(JavaSequences).GetMethod(nameof(JavaSequences.ToJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaSequences.ToJava)}' is missing.");

        /// <summary>
        /// Returns the named operator, picked out by parameter type where the name is overloaded.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="parameterTypes">One entry per parameter, <see langword="null"/> where the parameter
        /// is generic and so has no <see cref="Type"/> to name. Empty where the name is enough.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>BuiltInMethod</c> names a method by its parameter types — <c>TAKE(ExtendedEnumerable.class,
        /// "take", int.class)</c> — because a name alone does not pick an overload, and
        /// <c>EnumerableDefaults.take</c> has two. <see cref="Type.GetMethod(string, BindingFlags)"/> throws
        /// on the ambiguity rather than reporting it, so the choice is made here.
        /// </remarks>
        static MethodInfo Of(string name, params Type?[] parameterTypes)
        {
            MethodInfo? found = null;

            foreach (var method in typeof(ClrEnumerableDefaults).GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (method.Name != name || Matches(method) == false)
                    continue;

                if (found != null)
                    throw new InvalidOperationException($"'{name}' is ambiguous in {nameof(ClrEnumerableDefaults)}; name its parameter types.");

                found = method;
            }

            return found ?? throw new InvalidOperationException($"'{name}' is missing from {nameof(ClrEnumerableDefaults)}.");

            bool Matches(MethodInfo method)
            {
                if (parameterTypes.Length == 0)
                    return true;

                var parameters = method.GetParameters();
                if (parameters.Length != parameterTypes.Length)
                    return false;

                for (var i = 0; i < parameters.Length; i++)
                    if (parameterTypes[i] != null && parameters[i].ParameterType != parameterTypes[i])
                        return false;

                return true;
            }
        }

        // ---- the awaiting half ----

        // The same operators over IAsyncEnumerable, in ClrEnumerableDefaults, named for the
        // synchronous one they answer to with Async on the end. A node's Implement names the first set
        // and its ImplementAsync the second; nothing dispatches, and the two are together here so that a
        // member added to one is obviously missing from the other.

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Slice0"/>.
        /// </summary>
        public static readonly MethodInfo Slice0Async = Of(nameof(ClrEnumerableDefaults.Slice0Async));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Calc"/>.
        /// </summary>
        public static readonly MethodInfo CalcAsync = Of(nameof(ClrEnumerableDefaults.CalcAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Where"/>.
        /// </summary>
        public static readonly MethodInfo WhereAsync = Of(nameof(ClrEnumerableDefaults.WhereAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Select{TSource, TResult}"/>.
        /// </summary>
        public static readonly MethodInfo SelectAsync = Of(nameof(ClrEnumerableDefaults.SelectAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.OrderBy"/>.
        /// </summary>
        public static readonly MethodInfo OrderByAsync = Of(nameof(ClrEnumerableDefaults.OrderByAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Skip"/>.
        /// </summary>
        public static readonly MethodInfo SkipAsync = Of(nameof(ClrEnumerableDefaults.SkipAsync));

        /// <summary>
        /// <c>ClrEnumerableDefaults.Take</c>.
        /// </summary>
        /// <remarks>
        /// The <c>int</c> overload, as <c>ClrBuiltInMethod.Take</c> is; a plan's fetch is an <c>int</c>. The
        /// <c>long</c> one is the row limit a caller asks a prepared statement for.
        /// </remarks>
        public static readonly MethodInfo TakeAsync = Of(nameof(ClrEnumerableDefaults.TakeAsync), null, typeof(int), typeof(System.Threading.CancellationToken));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.OrderByWithFetchAndOffset"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffsetAsync = Of(nameof(ClrEnumerableDefaults.OrderByWithFetchAndOffsetAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.AsEnumerable"/>.
        /// </summary>
        public static readonly MethodInfo AsEnumerableAsync = Of(nameof(ClrEnumerableDefaults.AsEnumerableAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo SingletonAsync = Of(nameof(ClrEnumerableDefaults.SingletonAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Empty"/>.
        /// </summary>
        public static readonly MethodInfo EmptyAsync = Of(nameof(ClrEnumerableDefaults.EmptyAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.FromJavaList"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaListAsync = Of(nameof(ClrEnumerableDefaults.FromJavaListAsync));


        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Concat"/>.
        /// </summary>
        public static readonly MethodInfo ConcatAsync = Of(nameof(ClrEnumerableDefaults.ConcatAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Union"/>.
        /// </summary>
        public static readonly MethodInfo UnionAsync = Of(nameof(ClrEnumerableDefaults.UnionAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Intersect"/>.
        /// </summary>
        public static readonly MethodInfo IntersectAsync = Of(nameof(ClrEnumerableDefaults.IntersectAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Except"/>.
        /// </summary>
        public static readonly MethodInfo ExceptAsync = Of(nameof(ClrEnumerableDefaults.ExceptAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Distinct"/>.
        /// </summary>
        public static readonly MethodInfo DistinctAsync = Of(nameof(ClrEnumerableDefaults.DistinctAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LeftMarkHashJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoinAsync = Of(nameof(ClrEnumerableDefaults.LeftMarkHashJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.HashJoin"/>.
        /// </summary>
        public static readonly MethodInfo HashJoinAsync = Of(nameof(ClrEnumerableDefaults.HashJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SemiJoin"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoinAsync = Of(nameof(ClrEnumerableDefaults.SemiJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.AsofJoin"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoinAsync = Of(nameof(ClrEnumerableDefaults.AsofJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SortedGroupBy"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupByAsync = Of(nameof(ClrEnumerableDefaults.SortedGroupByAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.MergeUnion"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnionAsync = Of(nameof(ClrEnumerableDefaults.MergeUnionAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.MergeJoin"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoinAsync = Of(nameof(ClrEnumerableDefaults.MergeJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateBatchJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoinAsync = Of(nameof(ClrEnumerableDefaults.CorrelateBatchJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LeftMarkNestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoinAsync = Of(nameof(ClrEnumerableDefaults.LeftMarkNestedLoopJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateLeftMarkJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoinAsync = Of(nameof(ClrEnumerableDefaults.CorrelateLeftMarkJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.NestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoinAsync = Of(nameof(ClrEnumerableDefaults.NestedLoopJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoinAsync = Of(nameof(ClrEnumerableDefaults.CorrelateJoinAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.GroupBy"/>.
        /// </summary>
        public static readonly MethodInfo GroupByAsync = Of(nameof(ClrEnumerableDefaults.GroupByAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.GroupByMultiple"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultipleAsync = Of(nameof(ClrEnumerableDefaults.GroupByMultipleAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Window"/>.
        /// </summary>
        public static readonly MethodInfo WindowAsync = Of(nameof(ClrEnumerableDefaults.WindowAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SelectMany"/>.
        /// </summary>
        public static readonly MethodInfo SelectManyAsync = Of(nameof(ClrEnumerableDefaults.SelectManyAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LazyCollectionSpool"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpoolAsync = Of(nameof(ClrEnumerableDefaults.LazyCollectionSpoolAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.RepeatUnion"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnionAsync = Of(nameof(ClrEnumerableDefaults.RepeatUnionAsync));


        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SingletonAggregate"/>.
        /// </summary>
        public static readonly MethodInfo SingletonAggregateAsync = Of(nameof(ClrEnumerableDefaults.SingletonAggregateAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SingletonJavaList"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaListAsync = Of(nameof(ClrEnumerableDefaults.SingletonJavaListAsync));

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SingletonJavaMap"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaMapAsync = Of(nameof(ClrEnumerableDefaults.SingletonJavaMapAsync));


        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CombineQueryResults"/>.
        /// </summary>
        public static readonly MethodInfo CombineQueryResultsAsync = Of(nameof(ClrEnumerableDefaults.CombineQueryResultsAsync));


        /// <summary>
        /// <see cref="Apache.Calcite.Extensions.Interop.JavaSequences.FromJavaAsync"/>.
        /// </summary>
        /// <remarks>
        /// Not an operator of <see cref="ClrEnumerableDefaults"/> but of the interop, as
        /// <c>ClrBuiltInMethod.FromJava</c> is. It is what a converter into this convention calls.
        /// </remarks>
        public static readonly MethodInfo FromJavaAsync = typeof(Apache.Calcite.Extensions.Interop.JavaSequences).GetMethod(nameof(Apache.Calcite.Extensions.Interop.JavaSequences.FromJavaAsync), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            ?? throw new InvalidOperationException("'FromJavaAsync' is missing.");

        /// <summary>
        /// <see cref="Apache.Calcite.Extensions.Runtime.ClrSequences.ToAsyncEnumerable{TSource}"/>, which
        /// reads a pulled sequence as an awaited one.
        /// </summary>
        /// <remarks>
        /// What a plan of this kind is given when a node hands up the other kind. Nothing suspends, because
        /// the source is pulled, so it costs a state machine and no thread.
        /// </remarks>
        public static readonly MethodInfo ToAsyncEnumerable = typeof(Apache.Calcite.Extensions.Runtime.ClrSequences).GetMethod(nameof(Apache.Calcite.Extensions.Runtime.ClrSequences.ToAsyncEnumerable))
            ?? throw new InvalidOperationException("'ToAsyncEnumerable' is missing.");

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
        public static System.Linq.Expressions.MethodCallExpression CallAsync(MethodInfo method, params System.Linq.Expressions.Expression[] arguments)
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

    }

}
