namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// The kind of value a <c>CLR_FT_*</c> operator takes in a given operand position.
    /// </summary>
    /// <remarks>
    /// Each kind maps to one <c>SqlTypeFamily</c> (<see cref="FullTextOperandTypeChecker.FamilyOf"/>) and one
    /// declared parameter type (<see cref="FullTextOperandTypeChecker.TypeOf"/>).
    /// </remarks>
    public enum FullTextOperand
    {

        /// <summary>
        /// What is searched. Family <c>ANY</c>: any type but <c>CURSOR</c> is accepted.
        /// </summary>
        /// <remarks>
        /// Stores search values of different types: a character column, an untyped document property, an array
        /// of strings, or a row of several columns. Which of these a store can search is for its adapter to
        /// decide.
        /// </remarks>
        Searched,

        /// <summary>
        /// A keyword, or a term constructor standing for one. Family <c>CHARACTER</c>.
        /// </summary>
        /// <remarks>
        /// <c>FamilyOperandTypeChecker</c> accepts an operand of family <c>ANY</c> against any declared family,
        /// so an untyped document property and a term constructor (typed <c>ANY</c>) are both accepted here.
        /// </remarks>
        Term,

        /// <summary>
        /// The text a term constructor is built from. Family <c>CHARACTER</c>.
        /// </summary>
        /// <remarks>
        /// Validates exactly as <see cref="Term"/> does, so a term constructor nested in another is accepted
        /// here too; an adapter declines such a call.
        /// </remarks>
        Text,

        /// <summary>
        /// How many edits a fuzzy term tolerates. Family <c>INTEGER</c>.
        /// </summary>
        /// <remarks>
        /// No upper limit is enforced; a store that caps the edit count is left to reject a larger one.
        /// </remarks>
        Distance,

        /// <summary>
        /// A relevance score, as fused by <c>CLR_FT_RRF</c> or weighted by <c>CLR_FT_WEIGHT</c>. Family
        /// <c>NUMERIC</c>.
        /// </summary>
        Score,

        /// <summary>
        /// How much a score counts for, relative to the others fused with it. Family <c>NUMERIC</c>.
        /// </summary>
        Weight,

    }

}
