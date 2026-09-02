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
    /// This is why the declarations are operators rather than schema functions. A function declared through a
    /// schema carries its parameter types, and routine resolution runs an assignability check keyed on the
    /// parameter's <c>SqlTypeName</c>: <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> reaches
    /// <c>SqlTypeMappingRule.canApplyFrom</c>, which throws <c>AssertionError: No assign rules for OTHER
    /// defined</c> because no rule is keyed on <c>OTHER</c> and none can be supplied — the path consults the
    /// immutable assignment rule, so <c>SqlTypeCoercionRule.THREAD_PROVIDERS</c> does not reach it. Operand
    /// checking is our code and consults no rules at all.
    ///
    /// <para>That same filter is what <see cref="isFixedParameters"/> has to answer <c>false</c> for. It
    /// runs the check only on a checker that says its parameters are fixed, and skips one that does not; a
    /// <c>true</c> here would walk back into the assertion by way of our own declared parameter types.
    /// <c>SqlOperandMetadata</c> is implemented because <c>SqlUserDefinedFunction.getOperandTypeChecker</c>
    /// narrows its return type to it, not because the parameters are meant to be fixed.</para>
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
        /// Returns whether the list of parameters is fixed-length, which for this checker is always
        /// <c>false</c>.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// Answering <c>true</c> puts the declared parameter types back in front of
        /// <c>SqlUtil.filterRoutinesByParameterTypeAndName</c>, and a geography parameter there is the
        /// <c>No assign rules for OTHER defined</c> assertion. Nothing else is lost by saying no: the flag
        /// otherwise only tells the validator it may supply <c>DEFAULT</c> for a missing argument, and none
        /// of these has an optional one.
        /// </remarks>
        public bool isFixedParameters()
        {
            return false;
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
                GeographyOperand.Geography => GeographyTypes.IsGeography(type),
                GeographyOperand.Geometry => GeographyTypes.IsGeometry(type),
                GeographyOperand.Character => SqlTypeUtil.inCharFamily(type),
                GeographyOperand.Numeric => SqlTypeUtil.isNumeric(type),
                GeographyOperand.Binary => SqlTypeUtil.isBinary(type),
                _ => false,
            };
        }

        static RelDataType TypeOf(GeographyOperand operand, RelDataTypeFactory typeFactory)
        {
            return operand switch
            {
                GeographyOperand.Geography => GeographyTypes.Of(typeFactory),
                GeographyOperand.Geometry => GeographyTypes.GeometryOf(typeFactory),
                GeographyOperand.Character => typeFactory.createSqlType(SqlTypeName.VARCHAR),
                GeographyOperand.Numeric => typeFactory.createSqlType(SqlTypeName.DOUBLE),
                GeographyOperand.Binary => typeFactory.createSqlType(SqlTypeName.VARBINARY),
                _ => throw new NotSupportedException($"No type for '{operand}'."),
            };
        }

        static string NameOf(GeographyOperand operand)
        {
            return operand switch
            {
                GeographyOperand.Geography => "GEOGRAPHY",
                GeographyOperand.Geometry => "GEOMETRY",
                GeographyOperand.Character => "CHARACTER",
                GeographyOperand.Numeric => "NUMERIC",
                GeographyOperand.Binary => "BINARY",
                _ => throw new NotSupportedException($"No name for '{operand}'."),
            };
        }

    }

}
