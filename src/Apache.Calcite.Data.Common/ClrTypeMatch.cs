using System;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Which lookups an entry of a <see cref="ClrTypeMappingCollection"/> answers.
    /// </summary>
    /// <remarks>
    /// A lookup names a CLR type, a Calcite type, or both: a result column knows only its Calcite type, a
    /// parameter holding a bare value knows only its CLR type, and <c>GetFieldValue&lt;T&gt;</c> knows both.
    /// Every entry answers a lookup that names both types and that it accepts. The two flags say, independently,
    /// whether it also answers when one key is missing. For example, <see cref="DateTime"/> is what a
    /// <c>DATE</c> column reads back as, but a bare <see cref="DateTime"/> parameter is written as a
    /// <c>TIMESTAMP</c>, so the <c>DATE</c> entry for it is <see cref="RelDefault"/> only.
    /// </remarks>
    [Flags]
    public enum ClrTypeMatch
    {

        /// <summary>
        /// Answers only when both the CLR type and the Calcite type are named: a conversion that is allowed
        /// when asked for and is not a default in either direction.
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
