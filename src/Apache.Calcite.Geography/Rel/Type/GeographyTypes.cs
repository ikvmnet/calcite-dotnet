using System;

using Apache.Calcite.Geography.Runtime;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using JtsGeometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Rel.Type
{

    /// <summary>
    /// The two types this package deals in, and the questions asked about them.
    /// </summary>
    /// <remarks>
    /// <c>GEOGRAPHY</c> is <c>createJavaType(Geography.class)</c> and <c>GEOMETRY</c> is
    /// <c>createJavaType(Geometry.class)</c> — two ordinary <c>RelDataTypeFactoryImpl.JavaType</c>s over two
    /// different classes. Neither is a type of this package's own making, and that is why both behave.
    ///
    /// <para>The alternative, a <c>JavaType</c> subclass answering a different <c>SqlTypeName</c>, does not
    /// survive: <c>RelDataTypeFactoryImpl.copySimpleType</c> answers a change of nullability on a
    /// <c>JavaType</c> by constructing a plain one, so the subclass is dropped the first time an adapter
    /// declares a column <c>NOT NULL</c>. Distinguishing by class rather than by subclass puts the identity
    /// somewhere that method copies rather than discards. See <see cref="Geography"/>.</para>
    /// </remarks>
    public static class GeographyTypes
    {

        /// <summary>
        /// Returns the <c>GEOGRAPHY</c> type for the given type factory.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        public static RelDataType Of(RelDataTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return typeFactory.createJavaType((java.lang.Class)typeof(Runtime.Geography));
        }

        /// <summary>
        /// Returns the <c>GEOGRAPHY</c> type of the given nullability for the given type factory.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="nullable"></param>
        /// <returns></returns>
        /// <remarks>
        /// Both nullabilities are reachable, and a stock <c>JavaTypeFactoryImpl</c> answers both correctly.
        /// </remarks>
        public static RelDataType Of(RelDataTypeFactory typeFactory, bool nullable)
        {
            return typeFactory.createTypeWithNullability(Of(typeFactory), nullable);
        }

        /// <summary>
        /// Returns the type as something a schema can be given a name for.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>SchemaPlus.add(name, RelProtoDataType)</c> is how Calcite lets a name stand for a type in SQL,
        /// and registering this under <c>GEOGRAPHY</c> is what makes <c>CAST(x AS GEOGRAPHY)</c> resolve.
        /// </remarks>
        public static RelProtoDataType Proto()
        {
            return new GeographyProtoType();
        }

        sealed class GeographyProtoType : RelProtoDataType
        {

            public object apply(object typeFactory)
            {
                return Of((RelDataTypeFactory)typeFactory);
            }

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
            return IsJavaType(type, typeof(Runtime.Geography));
        }

        /// <summary>
        /// Returns whether the given type is a geometry as Calcite means one — planar, in the units of
        /// whatever coordinate system it is written in.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// A geography is not one, and now cannot be mistaken for one: the two are different classes, so
        /// the digests differ and so does <c>getJavaClass</c>. Calcite's own <c>ST_*</c> are declared over
        /// <c>Geometry</c> and will not take a geography.
        /// </remarks>
        public static bool IsGeometry(RelDataType? type)
        {
            return IsJavaType(type, typeof(JtsGeometry)) || (type is not null && type.getSqlTypeName() == SqlTypeName.GEOMETRY);
        }

        static bool IsJavaType(RelDataType? type, System.Type clazz)
        {
            return type is RelDataTypeFactoryImpl.JavaType javaType && Equals(javaType.getJavaClass(), (java.lang.Class)clazz);
        }

    }

}
