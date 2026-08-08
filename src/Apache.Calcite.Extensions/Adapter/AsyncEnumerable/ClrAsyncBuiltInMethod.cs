using System;
using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrAsyncEnumerableConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// <see cref="Apache.Calcite.Extensions.Adapter.Enumerable.ClrBuiltInMethod"/> over the asynchronous
    /// operator set. For all but seven members it is name for name, and that is the whole of what a node of
    /// this convention names differently from its synchronous counterpart: a node reads a member here where
    /// the other reads one there, and the rest of its <c>Implement</c> is the same code.
    ///
    /// <para><b>The seven that differ, and why.</b> Four are here and not there —
    /// <see cref="SingletonAggregate"/>, <see cref="SingletonJavaList"/>, <see cref="SingletonJavaMap"/> and
    /// <see cref="CombineQueryResults"/> — and each fuses a pair the synchronous convention nests in the
    /// tree. There <c>Singleton(Aggregate(source, …))</c> is two calls, the inner one returning a row; here
    /// the inner one has to be awaited and <b>an expression tree cannot await</b>, so the composition is an
    /// operator instead. <c>Aggregate</c>, <c>ToJavaList</c> and <c>ToJavaMap</c> are therefore absent, being
    /// the inner halves of those pairs.</para>
    ///
    /// <para>The other two absences are <c>FromJava</c> and <c>ToJava</c>, which carry a sequence across to
    /// linq4j and back. No node of this convention has a use for one: the nodes that needed them in the
    /// other — the table function scan and the interpreter — cannot exist here at all, because handing an
    /// asynchronous sequence to a generator of Calcite's is exactly the sync-over-async this convention
    /// refuses.</para>
    ///
    /// <para>A member is added when the node that calls it is written, so that a name in this class always
    /// has an operator and a caller.</para>
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
        /// <see cref="ClrAsyncEnumerableDefaults.AsEnumerable"/>.
        /// </summary>
        public static readonly MethodInfo AsEnumerable = Of(nameof(ClrAsyncEnumerableDefaults.AsEnumerable));

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
        /// <see cref="ClrAsyncEnumerableDefaults.Concat"/>.
        /// </summary>
        public static readonly MethodInfo Concat = Of(nameof(ClrAsyncEnumerableDefaults.Concat));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Union"/>.
        /// </summary>
        public static readonly MethodInfo Union = Of(nameof(ClrAsyncEnumerableDefaults.Union));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Intersect"/>.
        /// </summary>
        public static readonly MethodInfo Intersect = Of(nameof(ClrAsyncEnumerableDefaults.Intersect));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Except"/>.
        /// </summary>
        public static readonly MethodInfo Except = Of(nameof(ClrAsyncEnumerableDefaults.Except));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Distinct"/>.
        /// </summary>
        public static readonly MethodInfo Distinct = Of(nameof(ClrAsyncEnumerableDefaults.Distinct));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.LeftMarkHashJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoin = Of(nameof(ClrAsyncEnumerableDefaults.LeftMarkHashJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.HashJoin"/>.
        /// </summary>
        public static readonly MethodInfo HashJoin = Of(nameof(ClrAsyncEnumerableDefaults.HashJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.SemiJoin"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoin = Of(nameof(ClrAsyncEnumerableDefaults.SemiJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.AsofJoin"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoin = Of(nameof(ClrAsyncEnumerableDefaults.AsofJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.SortedGroupBy"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupBy = Of(nameof(ClrAsyncEnumerableDefaults.SortedGroupBy));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.MergeUnion"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnion = Of(nameof(ClrAsyncEnumerableDefaults.MergeUnion));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.MergeJoin"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoin = Of(nameof(ClrAsyncEnumerableDefaults.MergeJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.CorrelateBatchJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoin = Of(nameof(ClrAsyncEnumerableDefaults.CorrelateBatchJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.LeftMarkNestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoin = Of(nameof(ClrAsyncEnumerableDefaults.LeftMarkNestedLoopJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.CorrelateLeftMarkJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoin = Of(nameof(ClrAsyncEnumerableDefaults.CorrelateLeftMarkJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.NestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoin = Of(nameof(ClrAsyncEnumerableDefaults.NestedLoopJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.CorrelateJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoin = Of(nameof(ClrAsyncEnumerableDefaults.CorrelateJoin));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.GroupBy"/>.
        /// </summary>
        public static readonly MethodInfo GroupBy = Of(nameof(ClrAsyncEnumerableDefaults.GroupBy));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.GroupByMultiple"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultiple = Of(nameof(ClrAsyncEnumerableDefaults.GroupByMultiple));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.Window"/>.
        /// </summary>
        public static readonly MethodInfo Window = Of(nameof(ClrAsyncEnumerableDefaults.Window));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.SelectMany"/>.
        /// </summary>
        public static readonly MethodInfo SelectMany = Of(nameof(ClrAsyncEnumerableDefaults.SelectMany));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.LazyCollectionSpool"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpool = Of(nameof(ClrAsyncEnumerableDefaults.LazyCollectionSpool));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.RepeatUnion"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnion = Of(nameof(ClrAsyncEnumerableDefaults.RepeatUnion));


        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.SingletonAggregate"/>.
        /// </summary>
        public static readonly MethodInfo SingletonAggregate = Of(nameof(ClrAsyncEnumerableDefaults.SingletonAggregate));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.SingletonJavaList"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaList = Of(nameof(ClrAsyncEnumerableDefaults.SingletonJavaList));

        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.SingletonJavaMap"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaMap = Of(nameof(ClrAsyncEnumerableDefaults.SingletonJavaMap));


        /// <summary>
        /// <see cref="ClrAsyncEnumerableDefaults.CombineQueryResults"/>.
        /// </summary>
        public static readonly MethodInfo CombineQueryResults = Of(nameof(ClrAsyncEnumerableDefaults.CombineQueryResults));


        /// <summary>
        /// <see cref="Apache.Calcite.Extensions.Interop.JavaSequences.FromJavaAsync"/>.
        /// </summary>
        /// <remarks>
        /// Not an operator of <see cref="ClrAsyncEnumerableDefaults"/> but of the interop, as
        /// <c>ClrBuiltInMethod.FromJava</c> is. It is what a converter into this convention calls.
        /// </remarks>
        public static readonly MethodInfo FromJavaAsync = typeof(Apache.Calcite.Extensions.Interop.JavaSequences).GetMethod(nameof(Apache.Calcite.Extensions.Interop.JavaSequences.FromJavaAsync), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            ?? throw new InvalidOperationException("'FromJavaAsync' is missing.");

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
