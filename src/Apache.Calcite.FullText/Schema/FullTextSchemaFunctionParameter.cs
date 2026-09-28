using Apache.Calcite.FullText.Sql;

using org.apache.calcite.rel.type;
using org.apache.calcite.schema;

namespace Apache.Calcite.FullText.Schema
{

    /// <summary>
    /// One parameter of a <see cref="FullTextSchemaFunction"/>.
    /// </summary>
    /// <remarks>
    /// The type is <see cref="FullTextOperandTypeChecker.TypeOf"/> for the position. <c>CalciteCatalogReader.toOp</c>
    /// builds a schema function's operand checker from the families of its declared parameter types, so
    /// declaring a type in the family the operator's own checker requires makes both routes accept the same
    /// operands.
    /// </remarks>
    public sealed class FullTextSchemaFunctionParameter : FunctionParameter
    {

        readonly int ordinal;
        readonly FullTextOperand operand;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="ordinal">The parameter's zero-based position.</param>
        /// <param name="operand">What the position takes.</param>
        public FullTextSchemaFunctionParameter(int ordinal, FullTextOperand operand)
        {
            this.ordinal = ordinal;
            this.operand = operand;
        }

        /// <inheritdoc />
        public int getOrdinal()
        {
            return ordinal;
        }

        /// <summary>
        /// Returns the parameter's name: <c>SEARCHED</c> for the searched operand, <c>SCORE</c> followed by the
        /// ordinal for a score, and <c>KEYWORD</c> followed by the ordinal for any other position.
        /// </summary>
        /// <returns>The name.</returns>
        public string getName()
        {
            return operand switch
            {
                FullTextOperand.Searched => "SEARCHED",
                FullTextOperand.Score => "SCORE" + ordinal,
                _ => "KEYWORD" + ordinal,
            };
        }

        /// <inheritdoc />
        public RelDataType getType(RelDataTypeFactory typeFactory)
        {
            return FullTextOperandTypeChecker.TypeOf(operand, typeFactory);
        }

        /// <summary>
        /// Returns <c>false</c>: every parameter is required.
        /// </summary>
        /// <remarks>
        /// Calcite pads a call with <c>DEFAULT</c> for an omitted optional parameter, and no store can render
        /// <c>DEFAULT</c>. Variable arity is provided by declaring one function per arity instead (see
        /// <see cref="FullTextSchema.VariadicOperandLimit"/>).
        /// </remarks>
        /// <returns><c>false</c>.</returns>
        public bool isOptional()
        {
            return false;
        }

    }

}
