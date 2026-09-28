using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using JtsGeometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Rel.Type
{

    /// <summary>
    /// The SQL type of a geography, which is Calcite's own <c>GEOMETRY</c>.
    /// </summary>
    /// <remarks>
    /// There is no <c>GEOGRAPHY</c> type. A value is read geodesically because a <c>CLR_ST_GEOG_*</c> operator is
    /// applied to it, and the type does not record which reading is meant.
    ///
    /// <para><c>SqlTypeName</c> is a closed enum, and a type outside it (reported as <c>OTHER</c>) makes Calcite's
    /// routine resolution fail with an assertion when a function over it is declared on a schema. Using
    /// <c>GEOMETRY</c> is what allows <see cref="Schema.GeographySchema"/> to exist.</para>
    ///
    /// <para>As a consequence nothing prevents mixing the readings: <c>CLR_ST_GEOG_DISTANCE(ST_BUFFER(g, 0.1), h)</c>
    /// buffers by 0.1 degrees and then measures in metres. Nor can an SRID guard against it, because Calcite's own
    /// spatial functions return results with an SRID of zero.</para>
    /// </remarks>
    public static class GeographyTypes
    {

        /// <summary>
        /// Returns the type to declare a geography column with.
        /// </summary>
        /// <param name="typeFactory">The type factory.</param>
        /// <returns>The type <c>createJavaType</c> gives JTS <c>Geometry</c>, which Calcite reports as <c>GEOMETRY</c>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="typeFactory"/> is <c>null</c>.</exception>
        /// <remarks>
        /// A Java type rather than <c>createSqlType(GEOMETRY)</c>, because that is the type <c>ScalarFunctionImpl</c>
        /// derives from a method taking a <c>Geometry</c>, and so the type Calcite's <c>ST_*</c> functions and these
        /// operators are declared with.
        /// </remarks>
        public static RelDataType Of(RelDataTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return typeFactory.createJavaType((java.lang.Class)typeof(JtsGeometry));
        }

        /// <summary>
        /// Returns whether a type is a geometry, and so can be passed to a <c>CLR_ST_GEOG_*</c> operator.
        /// </summary>
        /// <param name="type">The type, or <c>null</c>.</param>
        /// <returns>
        /// <c>true</c> for the Java type of JTS <c>Geometry</c> or any type whose <c>SqlTypeName</c> is
        /// <c>GEOMETRY</c>; <c>false</c> otherwise, including for <c>null</c>.
        /// </returns>
        public static bool IsGeometry(RelDataType? type)
        {
            if (type is null)
                return false;

            if (type is RelDataTypeFactoryImpl.JavaType javaType)
                return Equals(javaType.getJavaClass(), (java.lang.Class)typeof(JtsGeometry));

            return type.getSqlTypeName() == SqlTypeName.GEOMETRY;
        }

    }

}
