using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The joins.
    /// </summary>
    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.HashJoin"/>.
        /// </summary>
        public static readonly MethodInfo HashJoin = Of(nameof(ClrDataCursorDefaults.HashJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SemiJoin"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoin = Of(nameof(ClrDataCursorDefaults.SemiJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.MergeJoin"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoin = Of(nameof(ClrDataCursorDefaults.MergeJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.NestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoin = Of(nameof(ClrDataCursorDefaults.NestedLoopJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.LeftMarkNestedLoopJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoin = Of(nameof(ClrDataCursorDefaults.LeftMarkNestedLoopJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.LeftMarkHashJoin"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoin = Of(nameof(ClrDataCursorDefaults.LeftMarkHashJoin));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.HashJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo HashJoinAsync = Of(nameof(ClrDataCursorDefaults.HashJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SemiJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo SemiJoinAsync = Of(nameof(ClrDataCursorDefaults.SemiJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.MergeJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo MergeJoinAsync = Of(nameof(ClrDataCursorDefaults.MergeJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.NestedLoopJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo NestedLoopJoinAsync = Of(nameof(ClrDataCursorDefaults.NestedLoopJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.LeftMarkNestedLoopJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkNestedLoopJoinAsync = Of(nameof(ClrDataCursorDefaults.LeftMarkNestedLoopJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.LeftMarkHashJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo LeftMarkHashJoinAsync = Of(nameof(ClrDataCursorDefaults.LeftMarkHashJoinAsync));

    }

}
