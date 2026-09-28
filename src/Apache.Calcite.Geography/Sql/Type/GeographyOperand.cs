namespace Apache.Calcite.Geography.Sql.Type
{

    /// <summary>
    /// What a <c>CLR_ST_GEOG_*</c> operator accepts in one operand position.
    /// </summary>
    /// <remarks>
    /// A <c>SqlTypeFamily</c> cannot express this: a type Calcite has no <c>SqlTypeName</c> for falls in the
    /// <c>OTHER</c> family, so a family cannot tell a geometry from any other such type.
    /// </remarks>
    public enum GeographyOperand
    {

        /// <summary>
        /// A geometry. Geographies are geometries; the operator's name says how it is read.
        /// </summary>
        Geometry,

        /// <summary>
        /// A character string.
        /// </summary>
        Character,

        /// <summary>
        /// A whole number, such as an index, a count or an SRID. The operand check accepts any numeric type; the
        /// parameter is declared as <c>INTEGER</c>.
        /// </summary>
        Integral,

        /// <summary>
        /// A number that may have a fraction, such as a distance, a tolerance or a coordinate. Any numeric type is
        /// accepted, and the parameter is declared as <c>ANY</c> so that a literal of any numeric type can be passed.
        /// </summary>
        Fractional,

        /// <summary>
        /// A string of bytes.
        /// </summary>
        Binary,

    }

}
