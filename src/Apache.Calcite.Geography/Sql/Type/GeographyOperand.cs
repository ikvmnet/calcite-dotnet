namespace Apache.Calcite.Geography.Sql.Type
{

    /// <summary>
    /// What an <c>ST_GEOG_*</c> operator will take in a given position.
    /// </summary>
    /// <remarks>
    /// Not a <c>SqlTypeFamily</c>, and it cannot be one: a geography answers <c>OTHER</c>, which is the
    /// family every unmapped Java type is in, so a family is exactly the thing that cannot tell a geography
    /// from anything else carried by a class Calcite has no <c>SqlTypeName</c> for.
    /// </remarks>
    public enum GeographyOperand
    {

        /// <summary>
        /// A geography, and nothing else — not a geometry, which is the same class read as a plane.
        /// </summary>
        Geography,

        /// <summary>
        /// A geometry as Calcite means one.
        /// </summary>
        Geometry,

        /// <summary>
        /// A character string.
        /// </summary>
        Character,

        /// <summary>
        /// A number.
        /// </summary>
        Numeric,

    }

}
