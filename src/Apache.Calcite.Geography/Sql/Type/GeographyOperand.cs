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
        /// A whole number — an index, a count, an SRID.
        /// </summary>
        /// <remarks>
        /// Integral and fractional are separate kinds because a schema function's parameter type is derived
        /// from the method's signature and then compared under Calcite's assignment rules, so it has to be a
        /// class those rules know. <c>java.lang.Number</c> is not one of them: it has no entry in
        /// <c>JavaToSqlTypeConversionRules</c>, so it becomes <c>SqlTypeName.OTHER</c>, which has no
        /// entry in <c>SqlTypeAssignmentRule</c>, and routine resolution asserts rather than rejects.
        /// <c>Integer</c> and <c>Double</c> are both named there.
        /// </remarks>
        Integral,

        /// <summary>
        /// A number that may have a fraction — a distance, a tolerance, a coordinate.
        /// </summary>
        /// <inheritdoc cref="Integral" />
        Fractional,

        /// <summary>
        /// A string of bytes.
        /// </summary>
        Binary,

    }

}
