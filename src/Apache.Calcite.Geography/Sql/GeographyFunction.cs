using org.apache.calcite.plan;
using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Geography.Sql
{

    /// <summary>
    /// A <c>CLR_ST_GEOG_*</c> operator, declaring the properties Calcite's own simplifications read.
    /// </summary>
    /// <remarks>
    /// <para><c>SqlOperator</c> has three hooks for this, <c>getStrongPolicyInference</c>, <c>isSymmetrical</c> and
    /// <c>reverse</c>. Answering them lets <c>RelOptUtil.simplifyJoin</c>, <c>RexSimplify</c>,
    /// <c>RelMdPredicates</c> and <c>RexNormalize</c> treat these like any other operator without a rule of this
    /// package's.</para>
    ///
    /// <para>A call resolved through <see cref="Schema.GeographySchema"/> carries a <c>SqlUserDefinedFunction</c>
    /// that <c>CalciteCatalogReader.toOp</c> built around the bare <c>Function</c>, not this object, and so has none
    /// of these properties until <see cref="GeographyOperatorTable.Rebind"/> replaces it.</para>
    /// </remarks>
    sealed class GeographyFunction : SqlUserDefinedFunction
    {

        /// <summary>
        /// Supplies a fixed <c>Strong.Policy</c>.
        /// </summary>
        sealed class Policy(Strong.Policy policy) : java.util.function.Supplier
        {

            public object get()
            {
                return policy;
            }

        }

        static readonly java.util.function.Supplier any = new Policy(Strong.Policy.ANY);

        readonly bool strict;
        readonly bool symmetrical;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="name">The SQL name.</param>
        /// <param name="returnType">How the call's type is inferred.</param>
        /// <param name="operandMetadata">The operand checker.</param>
        /// <param name="function">The function that implements the call.</param>
        /// <param name="strict">
        /// Whether the call is null if and only if an argument is null; declared as <c>Strong.Policy.ANY</c>, which
        /// Calcite reads in both directions (see <see cref="GeographyOperatorTable.IsStrict"/>).
        /// </param>
        /// <param name="symmetrical">Whether the call returns the same with its two operands swapped.</param>
        public GeographyFunction(SqlIdentifier name, SqlReturnTypeInference returnType, SqlOperandMetadata operandMetadata, Function function, bool strict, bool symmetrical) :
            base(name, SqlKind.OTHER_FUNCTION, returnType, null, operandMetadata, function)
        {
            this.strict = strict;
            this.symmetrical = symmetrical;
        }

        /// <inheritdoc />
        public override java.util.function.Supplier? getStrongPolicyInference()
        {
            return strict ? any : null;
        }

        /// <inheritdoc />
        public override bool isSymmetrical()
        {
            return symmetrical;
        }

        /// <inheritdoc />
        /// <remarks>
        /// A symmetrical operator is its own reverse. <c>RexNormalize.normalize</c> requires a non-null reverse when it
        /// swaps a symmetrical call's operands, so declaring symmetry without this would fail.
        /// </remarks>
        public override SqlOperator? reverse()
        {
            return symmetrical ? this : null;
        }

    }

}
