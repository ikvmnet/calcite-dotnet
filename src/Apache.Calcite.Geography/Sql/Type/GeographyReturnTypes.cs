using Apache.Calcite.Geography.Rel.Type;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Geography.Sql.Type
{

    /// <summary>
    /// The return type strategies the <c>ST_GEOG_*</c> operators use, for the two types Calcite has no
    /// strategy of its own for.
    /// </summary>
    /// <remarks>
    /// The rest come from <c>ReturnTypes</c> unchanged: a predicate is <c>BOOLEAN_NULLABLE</c> and a
    /// measurement is <c>DOUBLE_NULLABLE</c>, both of which are right, because each body answers null exactly
    /// when one of its arguments is null.
    ///
    /// <para>Neither of these two runs through <c>SqlTypeTransforms.TO_NULLABLE</c>, and that is deliberate.
    /// The transform calls <c>createTypeWithNullability</c>, and a change of nullability on a <c>JavaType</c>
    /// is answered by <c>RelDataTypeFactoryImpl.copySimpleType</c> with a plain <c>new JavaType(clazz,
    /// nullable)</c> — the subclass is not copied, so a geography asked to become <c>NOT NULL</c> comes back
    /// an ordinary geometry and every guarantee this package makes is gone. The type is nullable already,
    /// which is the answer the transform would give for a nullable argument anyway.</para>
    /// </remarks>
    public static class GeographyReturnTypes
    {

        /// <summary>
        /// Returns <c>GEOGRAPHY</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Geography = new GeographyReturnTypeInference();

        /// <summary>
        /// Returns <c>GEOMETRY</c>, as Calcite's own spatial library declares it.
        /// </summary>
        public static readonly SqlReturnTypeInference Geometry = new GeometryReturnTypeInference();

        /// <summary>
        /// The half of <c>SqlReturnTypeInference</c> that is the same whatever the type is.
        /// </summary>
        /// <remarks>
        /// IKVM does not project a Java default method as a C# default interface member, so an implementer
        /// written here has to restate every one of them. These are Calcite's own bodies.
        /// </remarks>
        abstract class ReturnTypeInference : SqlReturnTypeInference
        {

            public abstract RelDataType inferReturnType(SqlOperatorBinding opBinding);

            public SqlReturnTypeInference andThen(SqlTypeTransform transform)
            {
                return ReturnTypes.cascade(this, transform);
            }

            public SqlReturnTypeInference orElse(SqlReturnTypeInference transform)
            {
                return ReturnTypes.chain(this, transform);
            }

        }

        sealed class GeographyReturnTypeInference : ReturnTypeInference
        {

            public override RelDataType inferReturnType(SqlOperatorBinding opBinding)
            {
                return GeographyTypes.Of(opBinding.getTypeFactory());
            }

        }

        sealed class GeometryReturnTypeInference : ReturnTypeInference
        {

            public override RelDataType inferReturnType(SqlOperatorBinding opBinding)
            {
                return GeographyTypes.GeometryOf(opBinding.getTypeFactory());
            }

        }

    }

}
