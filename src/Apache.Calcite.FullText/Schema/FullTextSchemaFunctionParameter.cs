using Apache.Calcite.FullText.Sql;

using org.apache.calcite.rel.type;
using org.apache.calcite.schema;

namespace Apache.Calcite.FullText.Schema
{

    /// <summary>
    /// One parameter of a <see cref="FullTextSchemaFunction"/>.
    /// </summary>
    /// <remarks>
    /// <para>The type is the one <see cref="FullTextOperandTypeChecker.TypeOf"/> gives the position, and that
    /// is the whole of what keeps the two routes agreeing. <c>CalciteCatalogReader.toOp</c> builds a schema
    /// function's operand checker from its declared parameter types, mapping each to
    /// <c>getSqlTypeName().getFamily()</c> — so declaring the type whose family the operator's own checker
    /// looks for makes the derived checker accept what the operator accepts, rather than leaving the two to
    /// be kept in step by hand.</para>
    ///
    /// <para>A searched position is <c>ANY</c>, which a schema function may declare and a geography may not:
    /// <c>SqlTypeAssignmentRule</c> has an entry for <c>ANY</c>, and routine resolution reaches that table
    /// through <c>SqlUtil.filterRoutinesByParameterTypeAndName</c>. <c>OTHER</c> has no entry, which is why
    /// <c>Apache.Calcite.Geography</c> had to give up a distinct <c>GEOGRAPHY</c> type to be registerable on
    /// a schema at all.</para>
    /// </remarks>
    public sealed class FullTextSchemaFunctionParameter : FunctionParameter
    {

        readonly int ordinal;
        readonly FullTextOperand operand;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="ordinal">The parameter's position.</param>
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

        /// <inheritdoc />
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
        /// Returns <c>false</c>: a declaration takes exactly the operands it names.
        /// </summary>
        /// <remarks>
        /// This is what stops the <c>DEFAULT</c> padding. <c>SqlCallBinding.operands</c> pads a call out to
        /// the whole parameter list where there is room under the count range, the position is optional, and
        /// the checker's parameters are fixed — and no store has a rendering for <c>DEFAULT</c>. One
        /// declaration per arity with every parameter required is what
        /// <see cref="FullTextSchema.VariadicOperandLimit"/> exists to make possible.
        /// </remarks>
        /// <returns><c>false</c>.</returns>
        public bool isOptional()
        {
            return false;
        }

    }

}
