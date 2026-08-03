using System;
using System.Reflection;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrEnumerableConvention"/> calling convention is built from.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, named the same way, so a node reads as its Calcite
    /// original does. A generic one is the open definition, and a node closes it over the row type it is
    /// working with.
    /// </remarks>
    public static class ClrBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrEnumerables.Slice0"/>.
        /// </summary>
        public static readonly MethodInfo Slice0 = Of(nameof(ClrEnumerables.Slice0));

        /// <summary>
        /// <see cref="ClrEnumerables.Calc"/>.
        /// </summary>
        public static readonly MethodInfo Calc = Of(nameof(ClrEnumerables.Calc));

        /// <summary>
        /// <see cref="ClrEnumerables.Where"/>.
        /// </summary>
        public static readonly MethodInfo Where = Of(nameof(ClrEnumerables.Where));

        /// <summary>
        /// <see cref="ClrEnumerables.Select{TSource, TResult}"/>.
        /// </summary>
        public static readonly MethodInfo Select = Of(nameof(ClrEnumerables.Select));

        /// <summary>
        /// <see cref="ClrEnumerables.OrderBy"/>.
        /// </summary>
        public static readonly MethodInfo OrderBy = Of(nameof(ClrEnumerables.OrderBy));

        /// <summary>
        /// <see cref="ClrEnumerables.Skip"/>.
        /// </summary>
        public static readonly MethodInfo Skip = Of(nameof(ClrEnumerables.Skip));

        /// <summary>
        /// <see cref="ClrEnumerables.Take"/>.
        /// </summary>
        public static readonly MethodInfo Take = Of(nameof(ClrEnumerables.Take));

        /// <summary>
        /// <see cref="ClrEnumerables.Concat"/>.
        /// </summary>
        public static readonly MethodInfo Concat = Of(nameof(ClrEnumerables.Concat));

        /// <summary>
        /// <see cref="ClrEnumerables.Union"/>.
        /// </summary>
        public static readonly MethodInfo Union = Of(nameof(ClrEnumerables.Union));

        /// <summary>
        /// <see cref="ClrEnumerables.Intersect"/>.
        /// </summary>
        public static readonly MethodInfo Intersect = Of(nameof(ClrEnumerables.Intersect));

        /// <summary>
        /// <see cref="ClrEnumerables.Except"/>.
        /// </summary>
        public static readonly MethodInfo Except = Of(nameof(ClrEnumerables.Except));

        /// <summary>
        /// <see cref="ClrEnumerables.Distinct"/>.
        /// </summary>
        public static readonly MethodInfo Distinct = Of(nameof(ClrEnumerables.Distinct));

        /// <summary>
        /// <see cref="ClrEnumerables.HashJoin"/>.
        /// </summary>
        public static readonly MethodInfo HashJoin = Of(nameof(ClrEnumerables.HashJoin));

        /// <summary>
        /// <see cref="ClrEnumerables.SemiJoin"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoin = Of(nameof(ClrEnumerables.SemiJoin));

        /// <summary>
        /// <see cref="ClrEnumerables.NestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoin = Of(nameof(ClrEnumerables.NestedLoopJoin));

        /// <summary>
        /// <see cref="ClrEnumerables.CorrelateJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoin = Of(nameof(ClrEnumerables.CorrelateJoin));

        /// <summary>
        /// <see cref="ClrEnumerables.GroupBy"/>.
        /// </summary>
        public static readonly MethodInfo GroupBy = Of(nameof(ClrEnumerables.GroupBy));

        /// <summary>
        /// <see cref="ClrEnumerables.Aggregate"/>.
        /// </summary>
        public static readonly MethodInfo Aggregate = Of(nameof(ClrEnumerables.Aggregate));

        /// <summary>
        /// <see cref="ClrEnumerables.AsEnumerable"/>.
        /// </summary>
        public static readonly MethodInfo AsEnumerable = Of(nameof(ClrEnumerables.AsEnumerable));

        /// <summary>
        /// <see cref="ClrEnumerables.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo Singleton = Of(nameof(ClrEnumerables.Singleton));

        /// <summary>
        /// <see cref="ClrEnumerables.Empty"/>.
        /// </summary>
        public static readonly MethodInfo Empty = Of(nameof(ClrEnumerables.Empty));

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
            return typeof(ClrEnumerables).GetMethod(name, BindingFlags.Public | BindingFlags.Static)
                ?? throw new InvalidOperationException($"'{name}' is missing from {nameof(ClrEnumerables)}.");
        }

    }

}
