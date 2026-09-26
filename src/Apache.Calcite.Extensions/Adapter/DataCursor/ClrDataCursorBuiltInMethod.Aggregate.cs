using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Distinct"/>.
        /// </summary>
        public static readonly MethodInfo Distinct = Of(nameof(ClrDataCursorDefaults.Distinct));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.GroupBy"/>.
        /// </summary>
        public static readonly MethodInfo GroupBy = Of(nameof(ClrDataCursorDefaults.GroupBy));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.GroupByMultiple"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultiple = Of(nameof(ClrDataCursorDefaults.GroupByMultiple));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SortedGroupBy"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupBy = Of(nameof(ClrDataCursorDefaults.SortedGroupBy));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Aggregate"/>.
        /// </summary>
        public static readonly MethodInfo Aggregate = Of(nameof(ClrDataCursorDefaults.Aggregate));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo Singleton = Of(nameof(ClrDataCursorDefaults.Singleton));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.DistinctAsync"/>.
        /// </summary>
        public static readonly MethodInfo DistinctAsync = Of(nameof(ClrDataCursorDefaults.DistinctAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.GroupByAsync"/>.
        /// </summary>
        public static readonly MethodInfo GroupByAsync = Of(nameof(ClrDataCursorDefaults.GroupByAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.GroupByMultipleAsync"/>.
        /// </summary>
        public static readonly MethodInfo GroupByMultipleAsync = Of(nameof(ClrDataCursorDefaults.GroupByMultipleAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SortedGroupByAsync"/>.
        /// </summary>
        public static readonly MethodInfo SortedGroupByAsync = Of(nameof(ClrDataCursorDefaults.SortedGroupByAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SingletonAggregateAsync"/>, which is <see cref="Singleton"/>
        /// over <see cref="Aggregate"/> as one open, the fold being awaited.
        /// </summary>
        public static readonly MethodInfo SingletonAggregateAsync = Of(nameof(ClrDataCursorDefaults.SingletonAggregateAsync));

    }

}
