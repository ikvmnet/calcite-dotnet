using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// Checks the operands of a <c>CLR_FT_*</c> call: a fixed run of leading positions, then the same kind
    /// repeated for as many more as the call carries.
    /// </summary>
    /// <remarks>
    /// <para>Calcite has a checker for a fixed list of families (<c>OperandTypes.family</c>), one for the same
    /// family repeated (<c>OperandTypes.repeat</c>), and one that checks the count and nothing else
    /// (<c>OperandTypes.variadic</c>). None of them is <em>one</em> position followed by a repeat of a
    /// different one, which is the shape of every variadic operator here: something searched, then keywords.
    /// So the choice was this, or <c>variadic</c> and no type checking at all.</para>
    ///
    /// <para><b>The families are the same ones the schema route derives.</b>
    /// <c>CalciteCatalogReader.toOp</c> builds a schema function's checker out of its declared parameter
    /// types, mapping each to <c>type.getSqlTypeName().getFamily()</c>. <see cref="FamilyOf"/> and
    /// <see cref="TypeOf"/> are the two ends of that: the type a parameter declares is a type in the family
    /// this checks for. That is what makes a call resolved through a chained operator table and a call
    /// resolved through a schema validate alike, rather than merely being intended to.</para>
    ///
    /// <para><b>Every position is required, and that is load bearing.</b> <c>SqlCallBinding.operands</c> pads
    /// a call out to the whole parameter list with <c>DEFAULT</c> where three things hold at once — room
    /// under the count range's maximum, <c>isOptional</c> at that position, and <c>isFixedParameters</c> —
    /// and no store has a rendering for <c>DEFAULT</c>. <see cref="isOptional"/> answering <c>false</c> stops
    /// it here; <see cref="Schema.FullTextSchema"/> stops it on the other route by declaring one function per
    /// arity.</para>
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
        /// <param name="ordinal">The position.</param>
        /// <returns>The operand kind.</returns>
        public FullTextOperand At(int ordinal)
        {
            if (ordinal < leading.Length)
                return leading[ordinal];

            return repeating ?? leading[leading.Length - 1];
        }

        /// <summary>
        /// Checks the count here and hands the types to Calcite.
        /// </summary>
        /// <remarks>
        /// <para><b>Delegated rather than restated, and that is the whole point.</b> The other route's checker
        /// is a <c>FamilyOperandTypeChecker</c> that <c>CalciteCatalogReader.toOp</c> builds from the declared
        /// parameter types, so building one here over the same families makes the two routes apply not merely
        /// equivalent rules but the same code. Restating the rule was tried and the two disagreed
        /// immediately: Calcite's coerces where it can — a character literal in a numeric position, an
        /// integer in a character one — and a hand-written comparison does not, so a call refused by a host
        /// that chains this table was accepted by a connection that does not.</para>
        ///
        /// <para>The count is checked here because <c>FamilyOperandTypeChecker</c> has no opinion on arity
        /// beyond the length of its family list, which is built per call from the count it is given.</para>
        /// </remarks>
        /// <param name="callBinding">The call.</param>
        /// <param name="throwOnFailure">Whether to throw rather than answer.</param>
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

            // the ellipsis is not a family Calcite knows; it is the signature a caller reads in the error,
            // and there is nothing else to say about a position that repeats without end
            if (repeating is FullTextOperand more)
                names.Add(NameOf(more) + "...");

            return SqlUtil.getAliasedSignature(op, opName, java.util.Arrays.asList([.. names]));
        }

        /// <summary>
        /// Returns <c>false</c>: this checker is not a <c>SqlOperandMetadata</c>.
        /// </summary>
        /// <remarks>
        /// <para><b>The flag is a promise about this object's type, not a statement about the parameters.</b>
        /// <c>SqlUtil.filterRoutinesByParameterTypeAndName</c> keeps a routine whose checker says <c>false</c>
        /// and <em>casts</em> one that says <c>true</c> to <c>SqlOperandMetadata</c>, unguarded. It was
        /// answering <c>true</c> for the one operator here that has no repeating position, and every statement
        /// naming that operator died in an <c>InvalidCastException</c> out of the validator — measured, and
        /// not something the operator's own arity would lead anyone to expect.</para>
        ///
        /// <para>Nothing is lost by declining. The comment at that call site says as much: a routine with no
        /// parameter metadata is kept, only without the coercion that filter would have applied — and the
        /// operand checker still runs, which is where these are checked. It is also not what stops the
        /// <c>DEFAULT</c> padding: <see cref="isOptional"/> is.</para>
        ///
        /// <para><c>SqlFunctionCategory.SYSTEM</c> does not avoid any of this. <c>SqlFunction.deriveType</c>
        /// calls <c>SqlUtil.lookupRoutine</c> for every function it types, whatever the category.</para>
        /// </remarks>
        /// <returns><c>false</c>.</returns>
        public bool isFixedParameters()
        {
            return false;
        }

        /// <summary>
        /// Returns <c>false</c>: a <c>CLR_FT_*</c> call takes exactly the operands it is written with.
        /// </summary>
        /// <param name="i">The position.</param>
        /// <returns><c>false</c>.</returns>
        public bool isOptional(int i)
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
        /// A searched position is <c>ANY</c>, and that accepts <em>everything</em> rather than everything
        /// <c>SqlTypeFamily.ANY.getTypeNames()</c> lists: <c>FamilyOperandTypeChecker</c> switches on the
        /// declared family and returns true for <c>ANY</c> before consulting any type name, refusing only a
        /// <c>CURSOR</c>. That distinction is worth knowing, because the list it does not consult is
        /// <c>SqlTypeName.ALL_TYPES</c>, which despite its name has no <c>ARRAY</c> and no <c>MAP</c> — and a
        /// searchable array column is ordinary, Cosmos declaring one for <c>/tags</c> and PostgreSQL having
        /// <c>text[]</c>. A row passes for the same reason, which is how SQL Server and MySQL name the list of
        /// columns they search.
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
        /// The type whose family is <see cref="FamilyOf"/>, so that the checker Calcite derives from a
        /// declaration accepts what this one accepts. <c>ANY</c> is a <c>SqlTypeName</c> in its own right and
        /// is safe to declare — <c>SqlTypeAssignmentRule</c> has an entry for it, which is the thing
        /// <c>OTHER</c> lacks and the reason a geography cannot be a schema function's parameter.
        /// </remarks>
        /// <param name="operand">The operand kind.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <returns>The type.</returns>
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
        /// Returns what a position is called in a signature.
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
