using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Intersect"/>.
        /// </summary>
        public static readonly MethodInfo Intersect = Of(nameof(ClrDataCursorDefaults.Intersect));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Except"/>.
        /// </summary>
        public static readonly MethodInfo Except = Of(nameof(ClrDataCursorDefaults.Except));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.MergeUnion"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnion = Of(nameof(ClrDataCursorDefaults.MergeUnion));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.OrderByWithFetchAndOffset"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffset = Of(nameof(ClrDataCursorDefaults.OrderByWithFetchAndOffset));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.IntersectAsync"/>.
        /// </summary>
        public static readonly MethodInfo IntersectAsync = Of(nameof(ClrDataCursorDefaults.IntersectAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.ExceptAsync"/>.
        /// </summary>
        public static readonly MethodInfo ExceptAsync = Of(nameof(ClrDataCursorDefaults.ExceptAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.MergeUnionAsync"/>.
        /// </summary>
        public static readonly MethodInfo MergeUnionAsync = Of(nameof(ClrDataCursorDefaults.MergeUnionAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.OrderByWithFetchAndOffsetAsync"/>.
        /// </summary>
        public static readonly MethodInfo OrderByWithFetchAndOffsetAsync = Of(nameof(ClrDataCursorDefaults.OrderByWithFetchAndOffsetAsync));

    }

}
