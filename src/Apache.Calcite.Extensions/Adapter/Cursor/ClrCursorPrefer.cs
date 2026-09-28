using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The row representation a consumer of a node's output prefers or requires.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableRel.Prefer</c>, with the same five values. <see cref="ClrCursorPrefers"/> holds the
    /// methods Calcite declares on the enum, and the conversions to and from it.
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
    /// Methods on <see cref="ClrCursorPrefer"/>, mirroring those Calcite declares on <c>EnumerableRel.Prefer</c>.
    /// </summary>
    public static class ClrCursorPrefers
    {

        /// <summary>
        /// Returns the row format to use when the producer would choose <see cref="JavaRowFormat.CUSTOM"/>.
        /// </summary>
        /// <param name="prefer">The consumer's preference.</param>
        /// <returns><see cref="JavaRowFormat.ARRAY"/> if <paramref name="prefer"/> is
        /// <see cref="ClrCursorPrefer.Array"/>; otherwise <see cref="JavaRowFormat.CUSTOM"/>.</returns>
        public static JavaRowFormat PreferCustom(this ClrCursorPrefer prefer)
        {
            return prefer.Prefer(JavaRowFormat.CUSTOM);
        }

        /// <summary>
        /// Returns the row format to use when the producer would choose <see cref="JavaRowFormat.ARRAY"/>.
        /// </summary>
        /// <param name="prefer">The consumer's preference.</param>
        /// <returns><see cref="JavaRowFormat.CUSTOM"/> if <paramref name="prefer"/> is
        /// <see cref="ClrCursorPrefer.Custom"/>; otherwise <see cref="JavaRowFormat.ARRAY"/>.</returns>
        public static JavaRowFormat PreferArray(this ClrCursorPrefer prefer)
        {
            return prefer.Prefer(JavaRowFormat.ARRAY);
        }

        /// <summary>
        /// Returns the row format to use given the format the producer would choose.
        /// </summary>
        /// <param name="prefer">The consumer's preference.</param>
        /// <param name="format">The format the producer would choose.</param>
        /// <returns><see cref="JavaRowFormat.CUSTOM"/> for <see cref="ClrCursorPrefer.Custom"/>,
        /// <see cref="JavaRowFormat.ARRAY"/> for <see cref="ClrCursorPrefer.Array"/>, and
        /// <paramref name="format"/> otherwise.</returns>
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
        /// Returns the preference that requires a format.
        /// </summary>
        /// <param name="format">The required format.</param>
        /// <returns><see cref="ClrCursorPrefer.Array"/> for <see cref="JavaRowFormat.ARRAY"/>; otherwise
        /// <see cref="ClrCursorPrefer.Custom"/>.</returns>
        public static ClrCursorPrefer Of(JavaRowFormat format)
        {
            return format.name() == nameof(JavaRowFormat.ARRAY) ? ClrCursorPrefer.Array : ClrCursorPrefer.Custom;
        }

        /// <summary>
        /// Returns the equivalent <c>EnumerableRel.Prefer</c>, for implementing a sub-plan in
        /// <c>EnumerableConvention</c>.
        /// </summary>
        /// <param name="prefer">The preference to convert.</param>
        /// <returns>The Calcite value of the same name.</returns>
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
        /// Returns the equivalent <see cref="ClrCursorPrefer"/>, for implementing a sub-plan of this convention
        /// under a node of <c>EnumerableConvention</c>.
        /// </summary>
        /// <param name="prefer">The Calcite preference to convert.</param>
        /// <returns>The value of the same name.</returns>
        /// <remarks>
        /// Matches on the name, because a Java enum's ordinals are not stable across versions.
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
