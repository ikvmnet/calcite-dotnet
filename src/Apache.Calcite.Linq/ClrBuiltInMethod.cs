using System;
using System.Reflection;

using Apache.Calcite.Linq.Runtime;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrEnumerableConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, named the same way, so a node reads as its Calcite
    /// original does. A generic one is the open definition, and a node closes it over the row type it is
    /// working with.
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
        /// <see cref="ClrEnumerableDefaults.Take"/>.
        /// </summary>
        public static readonly MethodInfo Take = Of(nameof(ClrEnumerableDefaults.Take));

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
        /// <see cref="JavaSequences.ToJava"/>.
        /// </summary>
        public static readonly MethodInfo ToJava = typeof(JavaSequences).GetMethod(nameof(JavaSequences.ToJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaSequences.ToJava)}' is missing.");

        /// <summary>
        /// Returns the named operator.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static MethodInfo Of(string name)
        {
            return typeof(ClrEnumerableDefaults).GetMethod(name, BindingFlags.Public | BindingFlags.Static)
                ?? throw new InvalidOperationException($"'{name}' is missing from {nameof(ClrEnumerableDefaults)}.");
        }

    }

}
