using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Window"/>.
        /// </summary>
        public static readonly MethodInfo Window = Of(nameof(ClrDataCursorDefaults.Window));

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.WindowAsync"/>.
        /// </summary>
        public static readonly MethodInfo WindowAsync = Of(nameof(ClrDataCursorDefaults.WindowAsync));

    }

}
