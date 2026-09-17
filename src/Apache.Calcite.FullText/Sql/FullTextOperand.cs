namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// What a <c>CLR_FT_*</c> operator will take in a given position.
    /// </summary>
    /// <remarks>
    /// A <c>SqlTypeFamily</c> each, rather than a set of classes, because these operators have no body: the
    /// only thing a declaration here has to do is decide what validates. That is the opposite of
    /// <c>Apache.Calcite.Geography</c>, whose operand kinds exist to pick a .NET method's signature.
    /// </remarks>
    public enum FullTextOperand
    {

        /// <summary>
        /// The thing being searched.
        /// </summary>
        /// <remarks>
        /// Unconstrained, and that is not laziness. What a store searches differs in type as well as in
        /// spelling: a SQL Server or PostgreSQL column is character, a Cosmos property path is <c>ANY</c>
        /// because a container has no row schema, a Cosmos <c>/tags</c> is an array of strings, and a
        /// PostgreSQL <c>tsvector</c> is a type Calcite has no name for at all. A shared package that
        /// narrowed this would be refusing a store rather than catching a mistake.
        /// </remarks>
        Searched,

        /// <summary>
        /// A keyword to look for, or a term constructor describing one.
        /// </summary>
        /// <remarks>
        /// <para>Character, in every store. Declaring it costs a document adapter nothing —
        /// <c>FamilyOperandTypeChecker.checkSingleOperandType</c> passes an operand whose own type family is
        /// <c>ANY</c> against whatever family is declared — and it is what refuses
        /// <c>CLR_FT_CONTAINS(BODY, 42)</c> in the validator rather than in whatever the adapter does with a
        /// number it cannot quote.</para>
        ///
        /// <para>That same rule is what lets a term constructor stand here. <c>CLR_FT_PHRASE</c>,
        /// <c>CLR_FT_PREFIX</c> and <c>CLR_FT_FUZZY</c> are typed <c>ANY</c>, so they satisfy this position
        /// and an adapter reads the operand to find out which kind of term it has.</para>
        /// </remarks>
        Term,

        /// <summary>
        /// The text a term constructor is built from.
        /// </summary>
        /// <remarks>
        /// <para>Character, and it means the same as <see cref="Term"/> to the validator: the two differ only
        /// in what they document. A constructor describes a piece of text, and nesting one inside another is
        /// not a thing any store offers — Elasticsearch is explicit that it silently ignores fuzziness on a
        /// phrase query.</para>
        ///
        /// <para><b>The distinction cannot be enforced, and that is a property of Calcite rather than a gap
        /// here.</b> A constructor is typed <c>ANY</c> so that it satisfies a <c>CHARACTER</c> keyword
        /// position, and <c>FamilyOperandTypeChecker</c> passes an <c>ANY</c> operand against every declared
        /// family. The rule that admits a constructor where a keyword goes is the rule that admits one here.
        /// A nested constructor therefore validates, and an adapter declines it.</para>
        /// </remarks>
        Text,

        /// <summary>
        /// How many edits a fuzzy term tolerates.
        /// </summary>
        /// <remarks>
        /// Integral. Cosmos caps it at two and Atlas Search's <c>maxEdits</c> at two; the cap is the store's
        /// to enforce, since it is the store that has to honour it.
        /// </remarks>
        Distance,

        /// <summary>
        /// A relevance score, which is what <c>CLR_FT_RRF</c> fuses.
        /// </summary>
        Score,

        /// <summary>
        /// How much a score counts for, relative to the others fused with it.
        /// </summary>
        Weight,

    }

}
