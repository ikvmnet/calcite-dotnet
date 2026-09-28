using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// Checks the operands of a <c>CLR_FT_*</c> call: a fixed run of leading positions, then optionally one
    /// kind repeated for every further operand.
    /// </summary>
    /// <remarks>
    /// <para>Calcite's own checkers cover a fixed list of families (<c>OperandTypes.family</c>) or one family
    /// repeated (<c>OperandTypes.repeat</c>), but not a leading position followed by a repeat of a different
    /// family, which is the shape of the variadic operators here.</para>
    ///
    /// <para>The families checked are those of the types <see cref="TypeOf"/> gives the schema declarations.
    /// <c>CalciteCatalogReader.toOp</c> builds a schema function's checker from the families of its parameter
    /// types, so a call validates alike whichever route resolved it.</para>
    /// </remarks>
    public sealed class FullTextOperandTypeChecker : SqlOperandTypeChecker
    {

        readonly FullTextOperand[] leading;
        readonly FullTextOperand? repeating;
        readonly SqlOperandCountRange range;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="leading">What the first positions take.</param>
        /// <param name="repeating">What every position after them takes, or <c>null</c> where there are none.</param>
        /// <param name="range">How many operands the call may carry.</param>
        /// <exception cref="ArgumentNullException"><paramref name="leading"/> or <paramref name="range"/> is <c>null</c>.</exception>
        public FullTextOperandTypeChecker(FullTextOperand[] leading, FullTextOperand? repeating, SqlOperandCountRange range)
        {
            ArgumentNullException.ThrowIfNull(leading);
            ArgumentNullException.ThrowIfNull(range);

            this.leading = leading;
            this.repeating = repeating;
            this.range = range;
        }

        /// <summary>
        /// Returns what the position at the given ordinal takes.
        /// </summary>
        /// <remarks>
        /// Past the leading positions this is the repeating kind, or, where there is none, the last leading
        /// kind. The count is not checked.
        /// </remarks>
        /// <param name="ordinal">The zero-based position.</param>
        /// <returns>The operand kind.</returns>
        public FullTextOperand At(int ordinal)
        {
            if (ordinal < leading.Length)
                return leading[ordinal];

            return repeating ?? leading[leading.Length - 1];
        }

        /// <summary>
        /// Checks the operand count against the range, then checks the types with Calcite's
        /// <c>FamilyOperandTypeChecker</c> over the family of each position.
        /// </summary>
        /// <remarks>
        /// The schema route's checker is also a <c>FamilyOperandTypeChecker</c>, built by
        /// <c>CalciteCatalogReader.toOp</c> from the same families, so both routes apply the same rules,
        /// including its implicit coercions.
        /// </remarks>
        /// <param name="callBinding">The call.</param>
        /// <param name="throwOnFailure">Whether to throw a validation error rather than return <c>false</c>.</param>
        /// <returns>Whether the operands are acceptable.</returns>
        public bool checkOperandTypes(SqlCallBinding callBinding, bool throwOnFailure)
        {
            var count = callBinding.getOperandCount();

            if (range.isValidCount(count) == false)
            {
                if (throwOnFailure)
                    throw callBinding.newValidationSignatureError();

                return false;
            }

            var families = new java.util.ArrayList(count);

            for (var i = 0; i < count; i++)
                families.add(FamilyOf(At(i)));

            return OperandTypes.family(families).checkOperandTypes(callBinding, throwOnFailure);
        }

        /// <inheritdoc />
        public SqlOperandCountRange getOperandCountRange()
        {
            return range;
        }

        /// <inheritdoc />
        public string getAllowedSignatures(SqlOperator op, string opName)
        {
            var names = new List<string>();

            foreach (var operand in leading)
                names.Add(NameOf(operand));

            // the ellipsis marks the repeating position in the signature shown in a validation error
            if (repeating is FullTextOperand more)
                names.Add(NameOf(more) + "...");

            return SqlUtil.getAliasedSignature(op, opName, java.util.Arrays.asList([.. names]));
        }

        /// <summary>
        /// Returns <c>false</c>, because this checker is not a <c>SqlOperandMetadata</c>.
        /// </summary>
        /// <remarks>
        /// <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> casts a checker that answers <c>true</c> to
        /// <c>SqlOperandMetadata</c> without checking, so answering <c>true</c> here would throw during
        /// validation. A routine whose checker answers <c>false</c> is kept by that filter, and its operands
        /// are still checked by <see cref="checkOperandTypes"/>.
        /// </remarks>
        /// <returns><c>false</c>.</returns>
        public bool isFixedParameters()
        {
            return false;
        }

        /// <summary>
        /// Returns <c>false</c>: no position is optional, so Calcite never pads a call with <c>DEFAULT</c>.
        /// </summary>
        /// <param name="i">The position.</param>
        /// <returns><c>false</c>.</returns>
        public bool isOptional(int i)
        {
            return false;
        }

        // IKVM does not expose a Java default method as a C# default interface member, so each is restated
        // here with Calcite's own body.

        /// <inheritdoc />
        public SqlOperandTypeChecker.Consistency getConsistency()
        {
            return SqlOperandTypeChecker.Consistency.NONE;
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

        /// <summary>
        /// Returns the type family a position accepts.
        /// </summary>
        /// <remarks>
        /// A searched position is <c>ANY</c>. <c>FamilyOperandTypeChecker</c> accepts any operand type but
        /// <c>CURSOR</c> against the <c>ANY</c> family without consulting <c>SqlTypeFamily.ANY.getTypeNames()</c>, so <c>ARRAY</c>,
        /// <c>MAP</c> and <c>ROW</c> operands pass although that list omits them.
        /// </remarks>
        /// <param name="operand">The operand kind.</param>
        /// <returns>The family.</returns>
        public static SqlTypeFamily FamilyOf(FullTextOperand operand)
        {
            return operand switch
            {
                FullTextOperand.Searched => SqlTypeFamily.ANY,
                FullTextOperand.Term => SqlTypeFamily.CHARACTER,
                FullTextOperand.Text => SqlTypeFamily.CHARACTER,
                FullTextOperand.Distance => SqlTypeFamily.INTEGER,
                FullTextOperand.Score => SqlTypeFamily.NUMERIC,
                FullTextOperand.Weight => SqlTypeFamily.NUMERIC,
                _ => throw new NotSupportedException($"No family for '{operand}'."),
            };
        }

        /// <summary>
        /// Returns the type a schema function declares for a position.
        /// </summary>
        /// <remarks>
        /// A type in the family <see cref="FamilyOf"/> returns, so that the checker Calcite derives from a
        /// schema declaration accepts what this checker accepts. The operator table's type inference uses the
        /// same types, so literals are typed alike on both routes.
        /// </remarks>
        /// <param name="operand">The operand kind.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <returns>The type, nullable.</returns>
        public static RelDataType TypeOf(FullTextOperand operand, RelDataTypeFactory typeFactory)
        {
            var type = operand switch
            {
                FullTextOperand.Searched => typeFactory.createSqlType(SqlTypeName.ANY),
                FullTextOperand.Term => typeFactory.createSqlType(SqlTypeName.VARCHAR),
                FullTextOperand.Text => typeFactory.createSqlType(SqlTypeName.VARCHAR),
                FullTextOperand.Distance => typeFactory.createSqlType(SqlTypeName.INTEGER),
                FullTextOperand.Score => typeFactory.createSqlType(SqlTypeName.DOUBLE),
                FullTextOperand.Weight => typeFactory.createSqlType(SqlTypeName.DOUBLE),
                _ => throw new NotSupportedException($"No type for '{operand}'."),
            };

            return typeFactory.createTypeWithNullability(type, true);
        }

        /// <summary>
        /// Returns the name a position is given in the allowed signatures shown in a validation error.
        /// </summary>
        /// <param name="operand">The operand kind.</param>
        /// <returns>The name.</returns>
        public static string NameOf(FullTextOperand operand)
        {
            return operand switch
            {
                FullTextOperand.Searched => "ANY",
                FullTextOperand.Term => "CHARACTER",
                FullTextOperand.Text => "CHARACTER",
                FullTextOperand.Distance => "INTEGER",
                FullTextOperand.Score => "NUMERIC",
                FullTextOperand.Weight => "NUMERIC",
                _ => throw new NotSupportedException($"No name for '{operand}'."),
            };
        }

    }

}
