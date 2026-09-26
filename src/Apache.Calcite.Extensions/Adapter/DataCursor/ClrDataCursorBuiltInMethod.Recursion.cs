using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.LazyCollectionSpool"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpool = Of(nameof(ClrDataCursorDefaults.LazyCollectionSpool));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.RepeatUnion"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnion = Of(nameof(ClrDataCursorDefaults.RepeatUnion));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.LazyCollectionSpoolAsync"/>.
        /// </summary>
        public static readonly MethodInfo LazyCollectionSpoolAsync = Of(nameof(ClrDataCursorDefaults.LazyCollectionSpoolAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.RepeatUnionAsync"/>.
        /// </summary>
        public static readonly MethodInfo RepeatUnionAsync = Of(nameof(ClrDataCursorDefaults.RepeatUnionAsync));

    }

}
