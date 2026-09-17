using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// Tells the validator what each position of a <c>CLR_FT_*</c> call is expected to be.
    /// </summary>
    /// <remarks>
    /// <para><b>Here because the other route has one and the plans have to match.</b>
    /// <c>CalciteCatalogReader.toOp</c> gives every schema function <c>InferTypes.explicit</c> over its
    /// declared parameter types, so the validator types an untyped literal as the parameter — a keyword
    /// arrives as <c>'steel':VARCHAR</c> rather than as a <c>CHAR(5)</c>, and a literal in a numeric position
    /// is coerced. Without the same inference on the operator, the identical statement produced two different
    /// trees depending on which route resolved the name, differing in the declared type of every literal.
    /// Measured, and the only difference there was between the two.</para>
    ///
    /// <para><c>InferTypes.explicit</c> itself cannot be used: it takes a fixed list, and four of these five
    /// operators take as many keywords as a caller writes. The types are the same ones — the checker's, for
    /// the position — so this is that class over an arity known only at the call.</para>
    /// </remarks>
    public sealed class FullTextOperandTypeInference : SqlOperandTypeInference
    {

        readonly FullTextOperandTypeChecker checker;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="checker">The checker that says what each position takes.</param>
        public FullTextOperandTypeInference(FullTextOperandTypeChecker checker)
        {
            ArgumentNullException.ThrowIfNull(checker);

            this.checker = checker;
        }

        /// <inheritdoc />
        public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes)
        {
            var typeFactory = callBinding.getTypeFactory();

            for (var i = 0; i < operandTypes.Length; i++)
                operandTypes[i] = FullTextOperandTypeChecker.TypeOf(checker.At(i), typeFactory);
        }

    }

}
