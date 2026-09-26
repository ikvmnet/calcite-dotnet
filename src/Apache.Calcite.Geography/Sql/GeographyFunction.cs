using org.apache.calcite.plan;
using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Geography.Sql
{

    /// <summary>
    /// One <c>CLR_ST_GEOG_*</c> operator, with the facts about it that Calcite's own simplifications read.
    /// </summary>
    /// <remarks>
    /// <para><b>These are declarations, not rewrites.</b> <c>SqlOperator</c> carries three hooks an extension
    /// is meant to answer — <c>getStrongPolicyInference</c>, <c>isSymmetrical</c> and <c>reverse</c> — and
    /// nothing in <c>calcite-core</c> overrides any of them. Answering them is what lets
    /// <c>RelOptUtil.simplifyJoin</c>, <c>RexSimplify</c>, <c>RelMdPredicates</c> and <c>RexNormalize</c>
    /// treat one of these like any other operator, with no rule of this package's involved.</para>
    ///
    /// <para><b>They reach a plan only where the operator object does.</b> A name resolved through
    /// <see cref="Schema.GeographySchema"/> arrives as a <c>SqlUserDefinedFunction</c> that
    /// <c>CalciteCatalogReader.toOp</c> built around the bare <c>Function</c>, which is not this object and
    /// carries none of this. <see cref="GeographyOperatorTable.Rebind"/> is what puts it back, and
    /// <c>GeographyRules</c> runs it.</para>
    /// </remarks>
    sealed class GeographyFunction : SqlUserDefinedFunction
    {

        /// <summary>
        /// Says a call is null whenever an argument is null.
        /// </summary>
        /// <remarks>
        /// <b><c>Policy.ANY</c> is a biconditional and is read in both directions.</b>
        /// <c>RexSimplify.simplifyIsNull</c> turns <c>f(a, b) IS NULL</c> into <c>a IS NULL OR b IS NULL</c>,
        /// which claims the converse: that a call over non-null arguments is never null. That is why only the
        /// operators answering a boxed primitive carry it — see <see cref="GeographyOperatorTable.IsStrict"/>
        /// for which, and why the rest do not.
        /// </remarks>
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
        /// <param name="name"></param>
        /// <param name="returnType"></param>
        /// <param name="operandMetadata"></param>
        /// <param name="function"></param>
        /// <param name="strict">Whether the call is null whenever an argument is null, and only then.</param>
        /// <param name="symmetrical">Whether the call answers the same with its two operands the other way
        /// round.</param>
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
        /// A symmetrical operator is its own reverse, which is <c>SqlBasicFunction</c>'s answer and is not
        /// optional: <c>RexNormalize.normalize</c> calls <c>requireNonNull(operator.reverse())</c> the moment
        /// it decides to swap a symmetrical call's operands, so declaring the symmetry without this would
        /// fail building the digest.
        /// </remarks>
        public override SqlOperator? reverse()
        {
            return symmetrical ? this : null;
        }

    }

}
