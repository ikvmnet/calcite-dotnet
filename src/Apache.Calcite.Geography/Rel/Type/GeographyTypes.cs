using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using JtsGeometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Rel.Type
{

    /// <summary>
    /// The two types this package deals in, and the questions asked about them.
    /// </summary>
    /// <remarks>
    /// <c>GEOGRAPHY</c> is <see cref="GeographySqlType"/>. <c>GEOMETRY</c> is whatever Calcite's own spatial
    /// library produces and consumes, which is <c>createJavaType(Geometry.class)</c> — the type
    /// <c>ScalarFunctionImpl</c> derives by reflection from an <c>ST_*</c> method's signature. Asking the type
    /// factory for it here rather than for <c>createSqlType(GEOMETRY)</c> is what makes the crossing
    /// operators line up with Calcite's declarations exactly.
    /// </remarks>
    public static class GeographyTypes
    {

        /// <summary>
        /// Returns the <c>GEOGRAPHY</c> type for the given type factory.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        /// <remarks>
        /// The round trip through <c>copyType</c> is how the result gets interned. <c>canonize</c> is
        /// protected on <c>RelDataTypeFactoryImpl</c>, so a caller outside the class cannot reach it; but
        /// <c>copyType</c> is <c>createTypeWithNullability(type, type.isNullable())</c>, which finds the
        /// nullability unchanged, keeps the instance and canonizes it. The type factory's own
        /// <c>createJavaType</c> does the same thing by the same interner.
        /// </remarks>
        public static RelDataType Of(RelDataTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return typeFactory.copyType(new GeographySqlType((RelDataTypeFactoryImpl)typeFactory));
        }

        /// <summary>
        /// Returns the <c>GEOMETRY</c> type Calcite's spatial library uses, which is the Java type over
        /// <c>org.locationtech.jts.geom.Geometry</c>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        public static RelDataType GeometryOf(RelDataTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return typeFactory.createJavaType((java.lang.Class)typeof(JtsGeometry));
        }

        /// <summary>
        /// Returns whether the given type is <c>GEOGRAPHY</c>.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static bool IsGeography(RelDataType? type)
        {
            return type is GeographySqlType;
        }

        /// <summary>
        /// Returns whether the given type is a geometry as Calcite means one — planar, in the units of
        /// whatever coordinate system it is written in.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// A geography is not one. It is carried by the same class, so <c>getJavaClass</c> cannot tell them
        /// apart; what tells them apart is that a geography answers <see cref="SqlTypeName.OTHER"/> where a
        /// geometry answers <see cref="SqlTypeName.GEOMETRY"/>.
        /// </remarks>
        public static bool IsGeometry(RelDataType? type)
        {
            return type is not null && !IsGeography(type) && type.getSqlTypeName() == SqlTypeName.GEOMETRY;
        }

    }

}
