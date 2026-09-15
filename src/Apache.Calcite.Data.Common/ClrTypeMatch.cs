using System;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Which lookups an entry of a <see cref="ClrTypeMappingCollection"/> is willing to answer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A lookup carries two keys and either may be absent: a result column knows its Calcite type and not
    /// which CLR type is wanted, a parameter carrying a bare value knows the reverse, and
    /// <c>GetFieldValue&lt;T&gt;</c> knows both. An entry that is a legal answer when both are named is not
    /// necessarily the answer when one is missing, and the two directions are independent — <c>Guid</c> is
    /// what a caller writing one means and never what a <c>CHAR(36)</c> column reads back as, while
    /// <c>DateTime</c> is what a <c>DATE</c> column reads back as and never what a bare
    /// <see cref="DateTime"/> is written as, that being <c>TIMESTAMP</c>.
    /// </para>
    /// <para>
    /// Npgsql spends a three-valued <c>MatchRequirement</c> on the same question and needs a fourth rule —
    /// a fallback pass — to make a CLR type on its own resolve. Two flags say it directly, because here the
    /// two defaults genuinely are separate facts rather than one relaxation of a strict rule.
    /// </para>
    /// </remarks>
    [Flags]
    public enum ClrTypeMatch
    {

        /// <summary>
        /// Answers only when both the CLR type and the Calcite type are named. A conversion that is legal
        /// when asked for and is nobody's default.
        /// </summary>
        Named = 0,

        /// <summary>
        /// Also answers when only the CLR type is named: this is the Calcite type that CLR type is written
        /// as.
        /// </summary>
        ClrDefault = 1,

        /// <summary>
        /// Also answers when only the Calcite type is named: this is the CLR type that Calcite type is read
        /// back as.
        /// </summary>
        RelDefault = 2,

        /// <summary>
        /// Both, which is the ordinary case and the default.
        /// </summary>
        Default = ClrDefault | RelDefault,

    }

}
