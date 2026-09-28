using Apache.Calcite.Geography.Rel.Type;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Geography.Sql.Type
{

    /// <summary>
    /// Return type inferences for the <c>CLR_ST_GEOG_*</c> operators.
    /// </summary>
    /// <remarks>
    /// Each gives the type the type factory creates for a Java class, which is how Calcite types a function declared
    /// through a schema and so how its own <c>ST_*</c> functions are typed. A few operators use
    /// <c>ReturnTypes.BOOLEAN_NULLABLE</c> or <c>ReturnTypes.DOUBLE_NULLABLE</c> instead. Every result is nullable.
    ///
    /// <para>A nullable result does not mean the operator returns null only for a null argument; many return null
    /// for other inputs too. <see cref="GeographyOperatorTable.IsStrict"/> lists those that do not.</para>
    /// </remarks>
    public static class GeographyReturnTypes
    {

        /// <summary>
        /// Infers the geography type, <see cref="GeographyTypes.Of"/>.
        /// </summary>
        public static readonly SqlReturnTypeInference Geography = new GeographyReturnTypeInference();

        /// <summary>
        /// Returns an inference that gives the type the type factory creates for a Java class.
        /// </summary>
        /// <param name="clazz">The Java class.</param>
        /// <returns>The inference.</returns>
        /// <remarks>
        /// A function declared through a schema is typed by <c>createJavaType</c> over its method's return type, so
        /// <c>ST_ASTEXT</c> is <c>JavaType(String)</c> rather than <c>VARCHAR(2000)</c>. Naming the class keeps each
        /// <c>CLR_ST_GEOG_*</c> operator typed as the <c>ST_*</c> function it mirrors.
        /// </remarks>
        public static SqlReturnTypeInference Of(java.lang.Class clazz)
        {
            return new JavaReturnTypeInference(clazz);
        }

        /// <summary>
        /// Infers the type of <c>java.lang.Double</c>, a nullable <c>DOUBLE</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Double = Of((java.lang.Class)typeof(java.lang.Double));

        /// <summary>
        /// Infers the type of <c>java.lang.Integer</c>, a nullable <c>INTEGER</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Integer = Of((java.lang.Class)typeof(java.lang.Integer));

        /// <summary>
        /// Infers the type of <c>java.lang.Boolean</c>, a nullable <c>BOOLEAN</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Boolean = Of((java.lang.Class)typeof(java.lang.Boolean));

        /// <summary>
        /// Infers the type of <c>java.lang.String</c>, a <c>VARCHAR</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Text = Of((java.lang.Class)typeof(string));

        /// <summary>
        /// Infers the type of <c>ByteString</c>, a <c>VARBINARY</c>.
        /// </summary>
        public static readonly SqlReturnTypeInference Binary = Of((java.lang.Class)typeof(org.apache.calcite.avatica.util.ByteString));

        /// <summary>
        /// The members of <c>SqlReturnTypeInference</c> that do not depend on the type.
        /// </summary>
        /// <remarks>
        /// IKVM does not project a Java default method as a C# default interface member, so an implementer restates
        /// them. These are Calcite's own bodies.
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

    }

}
