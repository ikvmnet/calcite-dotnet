using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using JtsGeometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Rel.Type
{

    /// <summary>
    /// The type this package deals in, which is Calcite's own <c>GEOMETRY</c>.
    /// </summary>
    /// <remarks>
    /// There is no <c>GEOGRAPHY</c> type. A geography and a geometry are the same type, carried by the same
    /// class, and what says a value is to be read geodesically is the name of the operator applied to it —
    /// <c>ST_GEOG_DISTANCE</c> rather than <c>ST_DISTANCE</c>. That is the whole marking.
    ///
    /// <para>It is not the design anyone would choose first, and it is the only one Calcite permits.
    /// <c>SqlTypeName</c> is a closed enum, so a type of this package's own has to impersonate one of
    /// Calcite's; and the enum is not a label but the key to the tables that make a type behave — the
    /// assignment rules, the coercion rules, the family map, <c>getJavaClass</c>. A name with no entry in
    /// the assignment table is not rejected but asserted on, so a function declared through a schema over
    /// such a type takes the validator down. Since a schema is the only way an adapter can bring its
    /// functions with it, and bringing them is the point, the type gives way to the registration.</para>
    ///
    /// <para>What that costs is a mixed expression nothing refuses:
    /// <c>ST_GEOG_DISTANCE(ST_BUFFER(g, 0.1), h)</c> buffers in degrees and then measures in metres, and
    /// both halves run. There is no run-time guard underneath either — see
    /// <c>SridPropagationTests</c>: Calcite's own spatial functions drop the SRID off a geometry they
    /// derive, so a stamp cannot be relied on to say what a value means.</para>
    /// </remarks>
    public static class GeographyTypes
    {

        /// <summary>
        /// Returns the type a geography column has, which is Calcite's <c>GEOMETRY</c>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>createJavaType(Geometry.class)</c> rather than <c>createSqlType(GEOMETRY)</c>, because that is
        /// what <c>ScalarFunctionImpl</c> derives from an <c>ST_*</c> method's signature and so what every
        /// declaration here has to line up with.
        /// </remarks>
        public static RelDataType Of(RelDataTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return typeFactory.createJavaType((java.lang.Class)typeof(JtsGeometry));
        }

        /// <summary>
        /// Returns whether the given type is a geometry — which is to say, whether an <c>ST_GEOG_</c>
        /// operator can be applied to it.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// There is no predicate that tells a geography from a geometry, because there is nothing to tell
        /// apart. Both spellings of the question answer this.
        /// </remarks>
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
