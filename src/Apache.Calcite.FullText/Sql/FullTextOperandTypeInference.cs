using System;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// Infers the type of each operand of a <c>CLR_FT_*</c> call from its position, as
    /// <see cref="FullTextOperandTypeChecker.TypeOf"/> gives it.
    /// </summary>
    /// <remarks>
    /// <c>CalciteCatalogReader.toOp</c> gives a schema function <c>InferTypes.explicit</c> over its declared
    /// parameter types. This does the same for the operator table's operators, so a statement produces the
    /// same plan, with the same literal types, whichever route resolved the name. <c>InferTypes.explicit</c>
    /// itself takes a fixed list and so cannot serve a variadic operator.
    /// </remarks>
    public sealed class FullTextOperandTypeInference : SqlOperandTypeInference
    {

        readonly FullTextOperandTypeChecker checker;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="checker">The checker that says what each position takes.</param>
        /// <exception cref="ArgumentNullException"><paramref name="checker"/> is <c>null</c>.</exception>
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
