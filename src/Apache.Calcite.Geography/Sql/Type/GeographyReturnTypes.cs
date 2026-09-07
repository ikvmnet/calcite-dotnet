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
    /// <para>Neither runs through <c>SqlTypeTransforms.TO_NULLABLE</c>, and nothing turns on that any
    /// more. It mattered when a geography was a <c>JavaType</c> subclass, because the transform calls
    /// <c>createTypeWithNullability</c> and <c>copySimpleType</c> answers that on a <c>JavaType</c> by
    /// constructing a plain one, dropping the subclass. There is no subclass now. The transform is left off
    /// because it would not change the answer: every body here returns null exactly when an argument is
    /// null, so the nullable type is right whatever the operands are.</para>
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
        /// Returns the type the type factory gives a Java class, which is how Calcite's own spatial
        /// functions are typed.
        /// </summary>
        /// <param name="clazz"></param>
        /// <returns></returns>
        /// <remarks>
        /// A function declared through a schema is typed by <c>createJavaType</c> over the return type of the
        /// method behind it, so <c>ST_ASTEXT</c> is <c>JavaType(String)</c> and not <c>VARCHAR(2000)</c>.
        /// Naming the class rather than picking a <c>SqlTypeName</c> and a precision is what keeps an
        /// <c>ST_GEOG_</c> operator typed exactly as the <c>ST_</c> one it mirrors.
        /// </remarks>
        public static SqlReturnTypeInference Of(java.lang.Class clazz)
        {
            return new JavaReturnTypeInference(clazz);
        }

        /// <summary>
        /// Returns <c>DOUBLE</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Double = Of((java.lang.Class)typeof(java.lang.Double));

        /// <summary>
        /// Returns <c>INTEGER</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Integer = Of((java.lang.Class)typeof(java.lang.Integer));

        /// <summary>
        /// Returns <c>BOOLEAN</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Boolean = Of((java.lang.Class)typeof(java.lang.Boolean));

        /// <summary>
        /// Returns <c>VARCHAR</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Text = Of((java.lang.Class)typeof(string));

        /// <summary>
        /// Returns <c>VARBINARY</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Binary = Of((java.lang.Class)typeof(org.apache.calcite.avatica.util.ByteString));

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

        sealed class JavaReturnTypeInference : ReturnTypeInference
        {

            readonly java.lang.Class clazz;

            public JavaReturnTypeInference(java.lang.Class clazz)
            {
                this.clazz = clazz;
            }

            public override RelDataType inferReturnType(SqlOperatorBinding opBinding)
            {
                return opBinding.getTypeFactory().createJavaType(clazz);
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
