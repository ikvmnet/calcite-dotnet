using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CorrelateJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoin = Of(nameof(ClrDataCursorDefaults.CorrelateJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CorrelateLeftMarkJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoin = Of(nameof(ClrDataCursorDefaults.CorrelateLeftMarkJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CorrelateBatchJoin"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoin = Of(nameof(ClrDataCursorDefaults.CorrelateBatchJoin));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsofJoin"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoin = Of(nameof(ClrDataCursorDefaults.AsofJoin));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CorrelateJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateJoinAsync = Of(nameof(ClrDataCursorDefaults.CorrelateJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CorrelateLeftMarkJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateLeftMarkJoinAsync = Of(nameof(ClrDataCursorDefaults.CorrelateLeftMarkJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CorrelateBatchJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo CorrelateBatchJoinAsync = Of(nameof(ClrDataCursorDefaults.CorrelateBatchJoinAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.AsofJoinAsync"/>.
        /// </summary>
        public static readonly MethodInfo AsofJoinAsync = Of(nameof(ClrDataCursorDefaults.AsofJoinAsync));

    }

}
