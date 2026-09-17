using System;
using System.Linq;

using Apache.Calcite.Geography.Rel.Type;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Geography.Sql.Type
{

    /// <summary>
    /// Checks that each operand of an <c>ST_GEOG_*</c> call is what that position takes.
    /// </summary>
    /// <remarks>
    /// This is why a geography could not be its own type. A function declared through a schema carries its
    /// parameter types, and routine resolution runs an assignability check keyed on the parameter's
    /// <c>SqlTypeName</c>: <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> reaches
    /// <c>SqlTypeMappingRule.canApplyFrom</c>, which throws <c>AssertionError: No assign rules for OTHER
    /// defined</c> because no rule is keyed on <c>OTHER</c> and none can be supplied — the path consults the
    /// immutable assignment rule, so <c>SqlTypeCoercionRule.THREAD_PROVIDERS</c> does not reach it. Typing
    /// the operators over <c>GEOMETRY</c> is what removed that, and operand checking is our code besides,
    /// consulting no rules at all.
    ///
    /// <para>That filter runs only on a checker whose parameters are fixed, and skips one whose are not, so
    /// <see cref="isFixedParameters"/> decides whether it is reached at all; see there.
    /// <c>SqlOperandMetadata</c> is implemented because <c>SqlUserDefinedFunction.getOperandTypeChecker</c>
    /// narrows its return type to it, and because those filters cast to it unguarded.</para>
    ///
    /// <para>The checker is also the whole of the error a caller sees. One that accepted anything would let
    /// <c>ST_GEOG_DISTANCE('a', 'b')</c> validate and fail somewhere further down, or not at all.</para>
    /// </remarks>
    public sealed class GeographyOperandTypeChecker : SqlOperandMetadata
    {

        readonly GeographyOperand[] operands;
        readonly string[] names;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="operands">What each position takes.</param>
        /// <param name="names">The name of each position.</param>
        public GeographyOperandTypeChecker(GeographyOperand[] operands, string[] names)
        {
            ArgumentNullException.ThrowIfNull(operands);
            ArgumentNullException.ThrowIfNull(names);

            if (operands.Length != names.Length)
                throw new ArgumentException("An operand list and a name list must be the same length.", nameof(names));

            this.operands = operands;
            this.names = names;
        }

        /// <inheritdoc />
        public bool checkOperandTypes(SqlCallBinding callBinding, bool throwOnFailure)
        {
            for (var i = 0; i < operands.Length; i++)
            {
                if (Matches(operands[i], callBinding.getOperandType(i)))
                    continue;

                if (throwOnFailure)
                    throw callBinding.newValidationSignatureError();

                return false;
            }

            return true;
        }

        /// <inheritdoc />
        public SqlOperandCountRange getOperandCountRange()
        {
            return SqlOperandCountRanges.of(operands.Length);
        }

        /// <inheritdoc />
        public string getAllowedSignatures(SqlOperator op, string opName)
        {
            return SqlUtil.getAliasedSignature(op, opName, java.util.Arrays.asList([.. operands.Select(NameOf)]));
        }

        /// <inheritdoc />
        public java.util.List paramTypes(RelDataTypeFactory typeFactory)
        {
            var list = new java.util.ArrayList(operands.Length);

            foreach (var operand in operands)
                list.add(TypeOf(operand, typeFactory));

            return list;
        }

        /// <inheritdoc />
        public java.util.List paramNames()
        {
            return java.util.Arrays.asList([.. names]);
        }

        /// <summary>
        /// Returns <c>true</c>: every one of these takes exactly the operands it declares.
        /// </summary>
        /// <remarks>
        /// <para>The flag is a promise that this object is a <c>SqlOperandMetadata</c>, which it is:
        /// <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> and <c>bestMatch</c> both cast to that type
        /// without a guard whenever a checker answers <c>true</c>, and both then read
        /// <see cref="paramTypes"/>. Answering <c>false</c> would be safe and would cost the type coercion
        /// those filters apply.</para>
        ///
        /// <para>It had to be <c>false</c> while a geography was its own type: the parameter types reach
        /// <c>SqlTypeMappingRule.canApplyFrom</c>, which has no rule keyed on <c>OTHER</c> and asserts rather
        /// than rejecting. Typing the operators over <c>GEOMETRY</c> is what removed that, and is what lets
        /// these be declared on a schema at all.</para>
        /// </remarks>
        /// <returns><c>true</c>.</returns>
        public bool isFixedParameters()
        {
            return true;
        }

        // IKVM does not project a Java default method as a C# default interface member, so an implementer
        // written here has to restate every one of them. These are Calcite's own bodies.

        /// <inheritdoc />
        public SqlOperandTypeChecker.Consistency getConsistency()
        {
            return SqlOperandTypeChecker.Consistency.NONE;
        }

        /// <inheritdoc />
        public bool isOptional(int i)
        {
            return false;
        }

        /// <inheritdoc />
        public SqlOperandTypeInference typeInference()
        {
            return null!;
        }

        /// <inheritdoc />
        public CompositeOperandTypeChecker withGenerator(java.util.function.BiFunction signatureGenerator)
        {
            throw new java.lang.UnsupportedOperationException("withGenerator");
        }

        /// <inheritdoc />
        public SqlOperandTypeChecker and(SqlOperandTypeChecker checker)
        {
            return OperandTypes.and(this, checker);
        }

        /// <inheritdoc />
        public SqlOperandTypeChecker or(SqlOperandTypeChecker checker)
        {
            return OperandTypes.or(this, checker);
        }

        static bool Matches(GeographyOperand operand, RelDataType type)
        {
            // a NULL argument is legal in every position; each body answers null for one
            if (type.getSqlTypeName() == SqlTypeName.NULL)
                return true;

            return operand switch
            {
                GeographyOperand.Geometry => GeographyTypes.IsGeometry(type),
                GeographyOperand.Character => SqlTypeUtil.inCharFamily(type),
                GeographyOperand.Integral => SqlTypeUtil.isNumeric(type),
                GeographyOperand.Fractional => SqlTypeUtil.isNumeric(type),
                GeographyOperand.Binary => SqlTypeUtil.isBinary(type),
                _ => false,
            };
        }

        static RelDataType TypeOf(GeographyOperand operand, RelDataTypeFactory typeFactory)
        {
            return operand switch
            {
                GeographyOperand.Geometry => GeographyTypes.Of(typeFactory),
                GeographyOperand.Character => typeFactory.createSqlType(SqlTypeName.VARCHAR),
                GeographyOperand.Integral => typeFactory.createSqlType(SqlTypeName.INTEGER),
                GeographyOperand.Fractional => typeFactory.createSqlType(SqlTypeName.ANY),
                GeographyOperand.Binary => typeFactory.createSqlType(SqlTypeName.VARBINARY),
                _ => throw new NotSupportedException($"No type for '{operand}'."),
            };
        }

        static string NameOf(GeographyOperand operand)
        {
            return operand switch
            {
                GeographyOperand.Geometry => "GEOMETRY",
                GeographyOperand.Character => "CHARACTER",
                GeographyOperand.Integral => "INTEGER",
                GeographyOperand.Fractional => "DOUBLE",
                GeographyOperand.Binary => "BINARY",
                _ => throw new NotSupportedException($"No name for '{operand}'."),
            };
        }

    }

}
