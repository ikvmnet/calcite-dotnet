using System;
using System.Linq;

using Apache.Calcite.Geography.Rel.Type;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Geography.Sql.Type
{

    /// <summary>
    /// Checks that each operand of a <c>CLR_ST_GEOG_*</c> call is of a kind its position accepts.
    /// </summary>
    /// <remarks>
    /// The checker reports the parameter types <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> compares under
    /// Calcite's assignment rules, so every type it reports must be one those rules know; <c>OTHER</c> is not, and
    /// makes resolution fail with an assertion. The operand check itself consults no rules.
    ///
    /// <para><c>SqlOperandMetadata</c> is implemented because <c>SqlUserDefinedFunction.getOperandTypeChecker</c>
    /// returns that type, and because Calcite's routine filters cast a fixed-parameter checker to it without a
    /// check.</para>
    /// </remarks>
    public sealed class GeographyOperandTypeChecker : SqlOperandMetadata
    {

        readonly GeographyOperand[] operands;
        readonly string[] names;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="operands">What each position accepts.</param>
        /// <param name="names">The name of each position.</param>
        /// <exception cref="ArgumentNullException"><paramref name="operands"/> or <paramref name="names"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">The two arrays differ in length.</exception>
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
        /// Returns <c>true</c>: every operator takes exactly the operands it declares.
        /// </summary>
        /// <returns><c>true</c>.</returns>
        /// <remarks>
        /// With <c>true</c>, <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> and <c>bestMatch</c> cast this to
        /// <c>SqlOperandMetadata</c> and compare <see cref="paramTypes"/> under the assignment rules, applying type
        /// coercion.
        /// </remarks>
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
