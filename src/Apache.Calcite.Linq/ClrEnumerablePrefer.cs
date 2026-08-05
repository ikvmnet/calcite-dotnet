using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// The representation a consumer of a plan would prefer its rows to arrive in.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableRel.Prefer</c>, with the same five values.
    /// <see cref="ClrEnumerablePrefers"/> has the methods that answer what format a preference asks for, and
    /// converts to and from Calcite's enum.
    /// </remarks>
    public enum ClrEnumerablePrefer
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
    public static class ClrEnumerablePrefers
    {

        /// <summary>
        /// Returns the format to use where objects are wanted but arrays would do.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public static JavaRowFormat PreferCustom(this ClrEnumerablePrefer prefer)
        {
            return prefer.Prefer(JavaRowFormat.CUSTOM);
        }

        /// <summary>
        /// Returns the format to use where arrays are wanted but objects would do.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public static JavaRowFormat PreferArray(this ClrEnumerablePrefer prefer)
        {
            return prefer.Prefer(JavaRowFormat.ARRAY);
        }

        /// <summary>
        /// Returns the format to use, which is the one asked for unless the preference insists.
        /// </summary>
        /// <param name="prefer"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        public static JavaRowFormat Prefer(this ClrEnumerablePrefer prefer, JavaRowFormat format)
        {
            return prefer switch
            {
                ClrEnumerablePrefer.Custom => JavaRowFormat.CUSTOM,
                ClrEnumerablePrefer.Array => JavaRowFormat.ARRAY,
                _ => format,
            };
        }

        /// <summary>
        /// Returns the preference that insists on a format.
        /// </summary>
        /// <param name="format"></param>
        /// <returns></returns>
        public static ClrEnumerablePrefer Of(JavaRowFormat format)
        {
            return format.name() == nameof(JavaRowFormat.ARRAY) ? ClrEnumerablePrefer.Array : ClrEnumerablePrefer.Custom;
        }

        /// <summary>
        /// Returns the <c>EnumerableRel.Prefer</c> that means the same, for a sub-plan of Calcite's own
        /// convention.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public static EnumerableRel.Prefer ToCalcite(this ClrEnumerablePrefer prefer)
        {
            return prefer switch
            {
                ClrEnumerablePrefer.Array => EnumerableRel.Prefer.ARRAY,
                ClrEnumerablePrefer.ArrayNice => EnumerableRel.Prefer.ARRAY_NICE,
                ClrEnumerablePrefer.Custom => EnumerableRel.Prefer.CUSTOM,
                ClrEnumerablePrefer.CustomNice => EnumerableRel.Prefer.CUSTOM_NICE,
                _ => EnumerableRel.Prefer.ANY,
            };
        }

        /// <summary>
        /// Returns the <see cref="ClrEnumerablePrefer"/> that means the same, for a sub-plan of this
        /// convention under one of Calcite's.
        /// </summary>
        /// <param name="prefer"></param>
        /// <returns></returns>
        /// <remarks>
        /// Dispatched on the name, because a Java enum's ordinals are not stable across versions and its
        /// names are.
        /// </remarks>
        public static ClrEnumerablePrefer FromCalcite(EnumerableRel.Prefer prefer)
        {
            return prefer.name() switch
            {
                nameof(EnumerableRel.Prefer.ARRAY) => ClrEnumerablePrefer.Array,
                nameof(EnumerableRel.Prefer.ARRAY_NICE) => ClrEnumerablePrefer.ArrayNice,
                nameof(EnumerableRel.Prefer.CUSTOM) => ClrEnumerablePrefer.Custom,
                nameof(EnumerableRel.Prefer.CUSTOM_NICE) => ClrEnumerablePrefer.CustomNice,
                _ => ClrEnumerablePrefer.Any,
            };
        }

    }

}
