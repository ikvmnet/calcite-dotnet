using System;
using System.Reflection;

using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// The methods a collect, an uncollect, a combine and a table function scan are built from.
    /// </summary>
    static partial class ClrDataCursorBuiltInMethod
    {

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SelectMany"/>.
        /// </summary>
        public static readonly MethodInfo SelectMany = Of(nameof(ClrDataCursorDefaults.SelectMany));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.ToJavaList"/>.
        /// </summary>
        public static readonly MethodInfo ToJavaList = Of(nameof(ClrDataCursorDefaults.ToJavaList));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.FromJavaList"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaList = Of(nameof(ClrDataCursorDefaults.FromJavaList));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.ToJavaMap"/>.
        /// </summary>
        public static readonly MethodInfo ToJavaMap = Of(nameof(ClrDataCursorDefaults.ToJavaMap));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.Singleton"/>.
        /// </summary>
        public static readonly MethodInfo Singleton = Of(nameof(ClrDataCursorDefaults.Singleton));

        /// <summary>
        /// <see cref="JavaSequences.ToJava"/>, which a window table function hands its input to Calcite's
        /// generator through.
        /// </summary>
        public static readonly MethodInfo ToJava = typeof(JavaSequences).GetMethod(nameof(JavaSequences.ToJava))
            ?? throw new InvalidOperationException($"'{nameof(JavaSequences.ToJava)}' is missing.");

        // ---- the awaiting half ----

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SelectManyAsync"/>.
        /// </summary>
        public static readonly MethodInfo SelectManyAsync = Of(nameof(ClrDataCursorDefaults.SelectManyAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.FromJavaListAsync"/>.
        /// </summary>
        public static readonly MethodInfo FromJavaListAsync = Of(nameof(ClrDataCursorDefaults.FromJavaListAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SingletonJavaListAsync"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaListAsync = Of(nameof(ClrDataCursorDefaults.SingletonJavaListAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.SingletonJavaMapAsync"/>.
        /// </summary>
        public static readonly MethodInfo SingletonJavaMapAsync = Of(nameof(ClrDataCursorDefaults.SingletonJavaMapAsync));

        /// <summary>
        /// <see cref="ClrDataCursorDefaults.CombineQueryResultsAsync"/>.
        /// </summary>
        public static readonly MethodInfo CombineQueryResultsAsync = Of(nameof(ClrDataCursorDefaults.CombineQueryResultsAsync));

    }

}
