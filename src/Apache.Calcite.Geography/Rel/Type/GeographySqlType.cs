using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Rel.Type
{

    /// <summary>
    /// The <c>GEOGRAPHY</c> type: a geometry whose coordinates are read as WGS84 latitude and longitude
    /// rather than as a plane.
    /// </summary>
    /// <remarks>
    /// A <c>RelDataTypeFactoryImpl.JavaType</c> over <c>org.locationtech.jts.geom.Geometry</c> that answers a
    /// different name. <c>SqlTypeName</c> is a closed enum, so <c>GEOGRAPHY</c> cannot be added to it
    /// downstream; what can be done is give the type system something whose digest nothing else produces,
    /// while leaving the runtime carrier alone.
    ///
    /// <para>Three things are overridden and one is deliberately not. <c>getSqlTypeName</c> answers
    /// <see cref="SqlTypeName.OTHER"/> — the base would answer <see cref="SqlTypeName.GEOMETRY"/>, because
    /// <c>JavaToSqlTypeConversionRules</c> maps <c>Geometry.class</c> to it, and that mapping is the whole of
    /// what makes Calcite's planar functions accept a value. <c>generateTypeString</c> writes
    /// <c>GEOGRAPHY</c>, which is the digest, and a digest distinct from
    /// <c>JavaType(class org.locationtech.jts.geom.Geometry)</c> is what keeps the two apart everywhere
    /// Calcite compares types. <c>getJavaClass</c> is left alone, so it still answers <c>Geometry</c>: the
    /// runtime carrier is an ordinary JTS geometry, the class Calcite's own generated code already names, and
    /// there is no new class for Janino to resolve.</para>
    ///
    /// <para>The consequence, and it is the same one PostGIS lives with: the marking exists only in the type
    /// system. A geography and a geometry are indistinguishable at run time, so anywhere the type is erased —
    /// a value on an <c>ANY</c> path, a third-party function declared over <c>Geometry</c> — the geodesic
    /// reading is silently lost.</para>
    ///
    /// <para>The type is <em>nullable</em>, and that is load-bearing rather than incidental.
    /// <c>RelDataTypeFactoryImpl.copySimpleType</c> answers a change of nullability on any <c>JavaType</c>
    /// with a plain <c>new JavaType(clazz, nullable)</c> — it does not copy the subclass — so
    /// <c>createTypeWithNullability(geography, false)</c> hands back an ordinary <c>JavaType(Geometry)</c>
    /// and the marking is gone. A nullable type asked for nullability it already has is returned unchanged,
    /// which is the path everything actually takes. Nothing here may run a return type through
    /// <c>SqlTypeTransforms.TO_NULLABLE</c> for that reason; see
    /// <c>Apache.Calcite.Geography.Sql.GeographyReturnTypes</c>.</para>
    /// </remarks>
    public sealed class GeographySqlType : RelDataTypeFactoryImpl.JavaType
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory">The factory this type belongs to. <c>JavaType</c> is a non-static inner
        /// class, so it is the enclosing instance, and IKVM makes it the constructor's first argument.</param>
        /// <remarks>
        /// Prefer <c>GeographyTypes.Of</c>, which interns the result the way the type factory's own
        /// <c>createJavaType</c> does. Two uninterned instances are equal and have the same digest, so
        /// nothing goes wrong if one escapes, but reference comparisons of types are common in Calcite and
        /// cheap to keep true.
        /// </remarks>
        public GeographySqlType(RelDataTypeFactoryImpl typeFactory) :
            base(typeFactory, (java.lang.Class)typeof(Geometry))
        {

        }

        /// <inheritdoc />
        public override SqlTypeName getSqlTypeName()
        {
            return SqlTypeName.OTHER;
        }

        /// <inheritdoc />
        protected override void generateTypeString(java.lang.StringBuilder sb, bool withDetail)
        {
            sb.append("GEOGRAPHY");
        }

    }

}
