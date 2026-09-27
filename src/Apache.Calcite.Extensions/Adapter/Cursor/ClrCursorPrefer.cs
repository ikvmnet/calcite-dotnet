using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The representation a consumer of a plan would prefer its rows to arrive in.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRel.Prefer</c>, with the same five values.
    /// <see cref="ClrCursorPrefers"/> has the methods that answer what format a preference asks for, and
    /// converts to and from Calcite's enum.
    /// </remarks>
    public enum ClrCursorPrefer
    {

        /// <summary>
        /// Records must be represented as arrays.
        /// </summary>
        Array,

        /// <summary>
        /// Consumer would prefer that records are represented as arrays, but can accommodate records
        /// represented as objects.
        /// </summary>
        ArrayNice,

        /// <summary>
        /// Records must be represented as objects.
        /// </summary>
        Custom,

        /// <summary>
        /// Consumer would prefer that records are represented as objects, but can accommodate records
        /// represented as arrays.
        /// </summary>
        CustomNice,

        /// <summary>
        /// Consumer has no preferred representation.
        /// </summary>
        Any,

    }

    /// <summary>
    /// The methods <c>EnumerableRel.Prefer</c> carries on its values, which a C# enum cannot.
    /// </summary>
    public static class ClrCursorPrefers
    {

        /// <summary>
        /// Returns the format to use where objects are wanted but arrays would do.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public static JavaRowFormat PreferCustom(this ClrCursorPrefer prefer)
        {
            return prefer.Prefer(JavaRowFormat.CUSTOM);
        }

        /// <summary>
        /// Returns the format to use where arrays are wanted but objects would do.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public static JavaRowFormat PreferArray(this ClrCursorPrefer prefer)
        {
            return prefer.Prefer(JavaRowFormat.ARRAY);
        }

        /// <summary>
        /// Returns the format to use, which is the one asked for unless the preference insists.
        /// </summary>
        /// <param name="prefer"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        public static JavaRowFormat Prefer(this ClrCursorPrefer prefer, JavaRowFormat format)
        {
            return prefer switch
            {
                ClrCursorPrefer.Custom => JavaRowFormat.CUSTOM,
                ClrCursorPrefer.Array => JavaRowFormat.ARRAY,
                _ => format,
            };
        }

        /// <summary>
        /// Returns the preference that insists on a format.
        /// </summary>
        /// <param name="format"></param>
        /// <returns></returns>
        public static ClrCursorPrefer Of(JavaRowFormat format)
        {
            return format.name() == nameof(JavaRowFormat.ARRAY) ? ClrCursorPrefer.Array : ClrCursorPrefer.Custom;
        }

        /// <summary>
        /// Returns the <c>EnumerableRel.Prefer</c> that means the same, for a sub-plan of Calcite's own
        /// convention.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public static EnumerableRel.Prefer ToCalcite(this ClrCursorPrefer prefer)
        {
            return prefer switch
            {
                ClrCursorPrefer.Array => EnumerableRel.Prefer.ARRAY,
                ClrCursorPrefer.ArrayNice => EnumerableRel.Prefer.ARRAY_NICE,
                ClrCursorPrefer.Custom => EnumerableRel.Prefer.CUSTOM,
                ClrCursorPrefer.CustomNice => EnumerableRel.Prefer.CUSTOM_NICE,
                _ => EnumerableRel.Prefer.ANY,
            };
        }

        /// <summary>
        /// Returns the <see cref="ClrCursorPrefer"/> that means the same, for a sub-plan of this
        /// convention under one of Calcite's.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        /// <remarks>
        /// Dispatched on the name, because a Java enum's ordinals are not stable across versions and its
        /// names are.
        /// </remarks>
        public static ClrCursorPrefer FromCalcite(EnumerableRel.Prefer prefer)
        {
            return prefer.name() switch
            {
                nameof(EnumerableRel.Prefer.ARRAY) => ClrCursorPrefer.Array,
                nameof(EnumerableRel.Prefer.ARRAY_NICE) => ClrCursorPrefer.ArrayNice,
                nameof(EnumerableRel.Prefer.CUSTOM) => ClrCursorPrefer.Custom,
                nameof(EnumerableRel.Prefer.CUSTOM_NICE) => ClrCursorPrefer.CustomNice,
                _ => ClrCursorPrefer.Any,
            };
        }

    }

}
