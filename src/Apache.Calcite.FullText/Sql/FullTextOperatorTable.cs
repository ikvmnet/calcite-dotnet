using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// The operator table holding the <c>CLR_FT_*</c> full text operators: their names, arities, operand
    /// types and return types.
    /// </summary>
    /// <remarks>
    /// <para>A host that builds its own validator chains this table onto the ones it already uses:</para>
    ///
    /// <code>
    /// SqlOperatorTables.chain(SqlStdOperatorTable.instance(), FullTextOperatorTable.Instance())
    /// </code>
    ///
    /// <para>A caller that cannot chain an operator table, such as one using a plain <c>jdbc:calcite:</c>
    /// connection, declares the same operators on a schema with <see cref="Schema.FullTextSchema"/> instead.
    /// Use one route or the other, not both: with both registered, a call whose searched operand is an
    /// <c>ARRAY</c> column fails in Calcite's overload resolution.</para>
    ///
    /// <para>None of these operators has an in-process implementation. Which documents match, and how they
    /// score, is decided by the store's analyzer, so a store adapter has to push each call down into the
    /// store's own query. A call that no rule pushed down fails when the plan is compiled.</para>
    ///
    /// <para>A call resolved through a schema carries a <c>SqlUserDefinedFunction</c> that Calcite builds
    /// around the declaration, not the operator in this table. Recognise a call with <see cref="Matches"/>,
    /// <see cref="IsFullText"/>, <see cref="IsScoring"/> and <see cref="IsTerm"/>, which compare names and so
    /// work for either route, rather than by reference to these fields.</para>
    ///
    /// <para>Keywords are passed as a list of separate operands rather than as a query string in a store's
    /// grammar, so that no adapter has to parse another store's syntax. Phrase, prefix and fuzzy terms are
    /// expressed with the term constructors <see cref="ClrFtPhrase"/>, <see cref="ClrFtPrefix"/> and
    /// <see cref="ClrFtFuzzy"/>, which stand where a keyword goes. There is no language or analyzer argument,
    /// and no proximity, highlighting or snippet operator.</para>
    /// </remarks>
    public sealed class FullTextOperatorTable : SqlOperatorTable
    {

        /// <summary>
        /// <c>CLR_FT_CONTAINS(searched, keyword)</c>: whether the keyword occurs in what is searched. Answers a
        /// nullable <c>BOOLEAN</c>.
        /// </summary>
        /// <remarks>
        /// Takes exactly two operands. It means the same as <c>CLR_FT_CONTAINS_ALL</c> or
        /// <c>CLR_FT_CONTAINS_ANY</c> with a single keyword.
        /// </remarks>
        public static readonly SqlFunction ClrFtContains =
            Predicate("CLR_FT_CONTAINS", [FullTextOperand.Searched, FullTextOperand.Term], null, SqlOperandCountRanges.of(2));

        /// <summary>
        /// <c>CLR_FT_CONTAINS_ALL(searched, keyword, …)</c>: whether every keyword occurs. Takes one or more
        /// keywords and answers a nullable <c>BOOLEAN</c>.
        /// </summary>
        public static readonly SqlFunction ClrFtContainsAll =
            Predicate("CLR_FT_CONTAINS_ALL", [FullTextOperand.Searched], FullTextOperand.Term, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_CONTAINS_ANY(searched, keyword, …)</c>: whether any keyword occurs. Takes one or more
        /// keywords and answers a nullable <c>BOOLEAN</c>.
        /// </summary>
        public static readonly SqlFunction ClrFtContainsAny =
            Predicate("CLR_FT_CONTAINS_ANY", [FullTextOperand.Searched], FullTextOperand.Term, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_SCORE(searched, keyword, …)</c>: how well what is searched matches the keywords. Takes one
        /// or more keywords and answers a nullable <c>DOUBLE</c>.
        /// </summary>
        /// <remarks>
        /// <para>The value is whatever the store's ranking function computes, so it is not comparable across
        /// stores; the order it puts rows in is what a query can rely on.</para>
        ///
        /// <para>It is declared as an ordinary scalar function, and each adapter decides where its store
        /// allows one. Cosmos DB accepts a score only in <c>ORDER BY RANK</c>; PostgreSQL's <c>ts_rank</c> can
        /// be projected; SQL Server has no scalar rank, so an adapter there has to join to
        /// <c>CONTAINSTABLE</c> on the full text key, or decline the call.</para>
        /// </remarks>
        public static readonly SqlFunction ClrFtScore =
            Scoring("CLR_FT_SCORE", [FullTextOperand.Searched], FullTextOperand.Term, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_RRF(score, score, …)</c>: two or more scores fused into one by reciprocal rank fusion.
        /// Answers a nullable <c>DOUBLE</c>.
        /// </summary>
        /// <remarks>
        /// Every operand is numeric, so a full text score can be fused with any other score an adapter
        /// offers, such as a vector similarity, for hybrid search. It has no searched operand.
        /// </remarks>
        public static readonly SqlFunction ClrFtRrf =
            Scoring("CLR_FT_RRF", [], FullTextOperand.Score, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_PHRASE(text)</c>: the text as an ordered phrase. A term constructor, used where a keyword
        /// goes.
        /// </summary>
        /// <remarks>
        /// Use this for a multi-word search that must match the words together and in order. Stores differ in
        /// how they read a bare multi-word keyword: Cosmos DB treats it as a phrase, while PostgreSQL's
        /// <c>plainto_tsquery</c> matches the words anywhere in the document.
        /// </remarks>
        public static readonly SqlFunction ClrFtPhrase =
            Term("CLR_FT_PHRASE", [FullTextOperand.Text]);

        /// <summary>
        /// <c>CLR_FT_PREFIX(text)</c>: any word beginning with the text. A term constructor, used where a
        /// keyword goes.
        /// </summary>
        /// <remarks>
        /// Not every store has prefix search (Cosmos DB does not); an adapter for such a store declines the call.
        /// </remarks>
        public static readonly SqlFunction ClrFtPrefix =
            Term("CLR_FT_PREFIX", [FullTextOperand.Text]);

        /// <summary>
        /// <c>CLR_FT_FUZZY(text, edits)</c>: the text, matched within an <c>INTEGER</c> number of edits. A term
        /// constructor, used where a keyword goes.
        /// </summary>
        /// <remarks>
        /// No upper limit on the edit count is enforced here; a store that caps it (Cosmos DB and Atlas Search
        /// allow at most two) is left to reject a larger one.
        /// </remarks>
        public static readonly SqlFunction ClrFtFuzzy =
            Term("CLR_FT_FUZZY", [FullTextOperand.Text, FullTextOperand.Distance]);

        /// <summary>
        /// <c>CLR_FT_WEIGHT(score, weight)</c>: a score weighted relative to the others it is fused with.
        /// Answers a nullable <c>DOUBLE</c>.
        /// </summary>
        /// <remarks>
        /// The weight travels with the score it applies to, so an adapter whose store takes weights
        /// positionally (Cosmos DB's <c>RRF(f1, f2, [0.9, 0.1])</c>) builds that list from the operands. A
        /// store with no weighting can render the inner score where the weight is one and decline otherwise.
        /// </remarks>
        public static readonly SqlFunction ClrFtWeight =
            Scoring("CLR_FT_WEIGHT", [FullTextOperand.Score, FullTextOperand.Weight], null, SqlOperandCountRanges.of(2));

        /// <summary>
        /// Determines whether an operator is the given full text operator, whichever route resolved it.
        /// </summary>
        /// <remarks>
        /// Compares names. A call resolved through a schema carries a <c>SqlUserDefinedFunction</c> with the
        /// same name as the field in this class but a different identity, so a reference comparison would
        /// recognise the call only when the operator table was chained.
        /// </remarks>
        /// <param name="op">The operator to test; may be <c>null</c>.</param>
        /// <param name="function">The operator it should be, normally one of the fields of this class.</param>
        /// <returns><c>true</c> where both are non-null and have the same name.</returns>
        public static bool Matches(SqlOperator? op, SqlFunction function)
        {
            return op is not null && function is not null && op.getName() == function.getName();
        }

        /// <summary>
        /// Determines by name whether an operator is one of the <c>CLR_FT_*</c> operators, whichever route
        /// resolved it.
        /// </summary>
        /// <param name="op">The operator to test; may be <c>null</c>.</param>
        /// <returns><c>true</c> where the operator is a full text operator.</returns>
        public static bool IsFullText(SqlOperator? op)
        {
            return op?.getName() switch
            {
                "CLR_FT_CONTAINS" or "CLR_FT_CONTAINS_ALL" or "CLR_FT_CONTAINS_ANY" or "CLR_FT_SCORE" or "CLR_FT_RRF"
                    or "CLR_FT_PHRASE" or "CLR_FT_PREFIX" or "CLR_FT_FUZZY" or "CLR_FT_WEIGHT" => true,
                _ => false,
            };
        }

        /// <summary>
        /// Determines by name whether an operator answers a relevance score: <c>CLR_FT_SCORE</c>,
        /// <c>CLR_FT_RRF</c> or <c>CLR_FT_WEIGHT</c>.
        /// </summary>
        /// <remarks>
        /// Stores often allow scores in fewer places than predicates, so an adapter uses this to decide where
        /// a call may be pushed down.
        /// </remarks>
        /// <param name="op">The operator to test; may be <c>null</c>.</param>
        /// <returns><c>true</c> where the operator answers a score.</returns>
        public static bool IsScoring(SqlOperator? op)
        {
            return op?.getName() switch
            {
                "CLR_FT_SCORE" or "CLR_FT_RRF" or "CLR_FT_WEIGHT" => true,
                _ => false,
            };
        }

        /// <summary>
        /// Defines a full text predicate answering a nullable <c>BOOLEAN</c>.
        /// </summary>
        /// <remarks>
        /// Nullable because a store may have no answer for some rows a plan keeps, such as an outer join's
        /// unmatched side; declaring it <c>NOT NULL</c> would let the planner rewrite on a guarantee the store
        /// does not give.
        /// </remarks>
        static SqlFunction Predicate(string name, FullTextOperand[] leading, FullTextOperand? repeating, SqlOperandCountRange range)
        {
            var checker = new FullTextOperandTypeChecker(leading, repeating, range);

            return SqlBasicFunction
                .create(name, ReturnTypes.BOOLEAN_NULLABLE, checker, SqlFunctionCategory.SYSTEM)
                .withOperandTypeInference(new FullTextOperandTypeInference(checker));
        }

        /// <summary>
        /// Defines a scoring function answering a nullable <c>DOUBLE</c>, which a query can order by.
        /// </summary>
        static SqlFunction Scoring(string name, FullTextOperand[] leading, FullTextOperand? repeating, SqlOperandCountRange range)
        {
            var checker = new FullTextOperandTypeChecker(leading, repeating, range);

            return SqlBasicFunction
                .create(name, ReturnTypes.DOUBLE_NULLABLE, checker, SqlFunctionCategory.SYSTEM)
                .withOperandTypeInference(new FullTextOperandTypeInference(checker));
        }

        /// <summary>
        /// Defines a term constructor: a call that stands in a keyword position and says what kind of term it
        /// is.
        /// </summary>
        /// <remarks>
        /// Typed <c>ANY</c>. <c>FamilyOperandTypeChecker</c> accepts an operand of family <c>ANY</c> against
        /// any declared family, so a constructor satisfies a <c>CHARACTER</c> keyword position while a plain
        /// keyword is still required to be character. The same rule means one constructor nested in another
        /// also validates.
        /// </remarks>
        static SqlFunction Term(string name, FullTextOperand[] operands)
        {
            var checker = new FullTextOperandTypeChecker(operands, null, SqlOperandCountRanges.of(operands.Length));

            return SqlBasicFunction
                .create(name, ReturnTypes.@explicit(SqlTypeName.ANY), checker, SqlFunctionCategory.SYSTEM)
                .withOperandTypeInference(new FullTextOperandTypeInference(checker));
        }

        /// <summary>
        /// Determines by name whether an operator is a term constructor: <c>CLR_FT_PHRASE</c>,
        /// <c>CLR_FT_PREFIX</c> or <c>CLR_FT_FUZZY</c>.
        /// </summary>
        /// <remarks>
        /// An adapter reading an operand in a keyword position uses this to tell a term constructor from a
        /// plain keyword expression.
        /// </remarks>
        /// <param name="op">The operator to test; may be <c>null</c>.</param>
        /// <returns><c>true</c> where the operator constructs a term.</returns>
        public static bool IsTerm(SqlOperator? op)
        {
            return op?.getName() switch
            {
                "CLR_FT_PHRASE" or "CLR_FT_PREFIX" or "CLR_FT_FUZZY" => true,
                _ => false,
            };
        }

        static readonly FullTextOperatorTable instance = new();

        /// <summary>
        /// Gets the shared instance of the operator table.
        /// </summary>
        /// <returns>The table, holding all nine <c>CLR_FT_*</c> operators.</returns>
        public static FullTextOperatorTable Instance()
        {
            return instance;
        }

        readonly SqlOperatorTable operators;

        FullTextOperatorTable()
        {
            operators = SqlOperatorTables.of([
                ClrFtContains, ClrFtContainsAll, ClrFtContainsAny,
                ClrFtScore, ClrFtRrf, ClrFtWeight,
                ClrFtPhrase, ClrFtPrefix, ClrFtFuzzy,
            ]);
        }

        /// <inheritdoc />
        public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, java.util.List operatorList, SqlNameMatcher nameMatcher)
        {
            operators.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        }

        /// <inheritdoc />
        public java.util.List getOperatorList()
        {
            return operators.getOperatorList();
        }

    }

}
