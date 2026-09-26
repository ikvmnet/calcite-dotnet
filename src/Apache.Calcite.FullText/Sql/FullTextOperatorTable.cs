using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.FullText.Sql
{

    /// <summary>
    /// The <c>CLR_FT_*</c> operators: a full text surface a query can be written against without naming a store.
    /// </summary>
    /// <remarks>
    /// <para>Chained by a host onto whatever it already has:</para>
    ///
    /// <code>
    /// SqlOperatorTables.chain(SqlStdOperatorTable.instance(), FullTextOperatorTable.Instance())
    /// </code>
    ///
    /// <para>and declared on a schema by <see cref="Schema.FullTextSchema"/> for everyone else, because a
    /// plain <c>CalciteConnection</c> chains nothing and a host is the only party that can. The two routes
    /// are both needed and neither is a duplicate of the other; see that class.</para>
    ///
    /// <para><b>Nothing here has a body, and nothing here will.</b> A full text answer is the store's
    /// analyzer — tokenising, case folding, stemming, stopwords, per language — and which documents match is
    /// whatever that analyzer decides. An in-process evaluator would answer differently from the store for
    /// the same query, which is worse than not answering: a predicate rechecked against it would discard rows
    /// the store correctly returned. So a call that reaches code generation is refused, and
    /// <see cref="Schema.FullTextSchemaFunction"/> is where the refusal gets a sentence saying why.</para>
    ///
    /// <para><b>Each operator is a public field</b>, so an adapter can recognise one by identity when it has
    /// the operator to hand. It must not rely on that: what arrives in a plan resolved through a schema is a
    /// <c>SqlUserDefinedFunction</c> Calcite built around the declaration, carrying the name and the arity and
    /// not being this object. <see cref="Matches"/> is the check that works on both routes.</para>
    ///
    /// <h4>Where the names come from</h4>
    ///
    /// <para>Full text <em>is</em> standardised — ISO/IEC 13249-2, <i>SQL multimedia and application
    /// packages, Part 2: Full-Text</i>, the same series whose Part 3 is Spatial. It defines a
    /// <c>FullText</c> type whose <c>Contains</c> and <c>Score</c> methods take a structured pattern, with
    /// plain SQL functions of those names beside them, and it prefixes the pattern type hierarchy
    /// <c>FT_</c>: <c>FT_Pattern</c>, <c>FT_WordOrPhrase</c>, <c>FT_StemmedWord</c>, <c>FT_Proxi</c>,
    /// <c>FT_Soundex</c>, <c>FT_Fuzzy</c>, <c>FT_IsAbout</c> and the rest.</para>
    ///
    /// <para><b>So the prefix is borrowed and the shape is not.</b> The standard's <c>Contains(doc, pattern)</c>
    /// is binary, with every structure — <c>&amp;</c>, <c>|</c>, <c>NOT</c>, <c>STEMMED FORM OF</c>,
    /// <c>SOUNDS LIKE</c>, <c>IN SAME SENTENCE AS</c>, <c>THESAURUS</c> — inside the pattern string. Adopting
    /// that would put a pattern parser in every adapter, which is the analyzer problem above moved from
    /// evaluation into parsing. A list of bare keywords is the one thing every store can express, so that is
    /// what these take. Its own spelling, bare <c>CONTAINS</c>, is unavailable regardless:
    /// <c>SqlStdOperatorTable.CONTAINS</c> is the SQL:2011 period predicate and the parser reserves the
    /// word.</para>
    ///
    /// <para><b>Taking a store's spellings instead was considered</b>, that being what Calcite did for
    /// spatial — 69 of the 144 <c>ST_*</c> in its reference are PostGIS or H2GIS extensions rather than
    /// OpenGIS, and its own acknowledgements name PostGIS's tests as a reference implementation. It works
    /// there because PostGIS's spatial surface already <em>is</em> a function family over one type Calcite
    /// models as <c>GEOMETRY</c>: only the names had to be borrowed. Full text has no such store. PostgreSQL
    /// spells it <c>tsvector @@ tsquery</c> — two types Calcite has no <c>SqlTypeName</c> for and a query
    /// grammar; SQL Server <c>CONTAINS</c> and <c>FREETEXT</c>; MySQL <c>MATCH … AGAINST</c>; SQLite FTS5
    /// <c>MATCH</c>. They agree on nothing, so a name taken from one is a name the others must map anyway,
    /// and it would additionally be a name Calcite might one day give a library function.</para>
    ///
    /// <h4>What is deliberately not here</h4>
    ///
    /// <para><b>No pattern or query string.</b> See above: a grammar in a shared package is a grammar every
    /// adapter has to parse.</para>
    ///
    /// <para><b>No language or analyzer argument</b>, and the reason is structural rather than a judgement
    /// about where it belongs. A leading configuration name and a keyword are both character strings, so
    /// <c>CLR_FT_CONTAINS_ALL(BODY, 'english', 'steel')</c> and <c>CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame')</c>
    /// would be the same call: a variadic keyword list has no room for an optional string in front of it.
    /// Two stores do take one per call — PostgreSQL as <c>to_tsvector('english', body)</c> and SQL Server as
    /// the optional <c>LANGUAGE</c> term on all four of its constructs — and both have a default standing
    /// behind it, from the database in one case and the full text index in the other. Everywhere else it is a
    /// property of the thing searched: Cosmos's container full text policy, MySQL's column collation, SQLite
    /// FTS5's tokenizer, Elasticsearch's field mapping. An adapter that needs one takes it from its own
    /// configuration.</para>
    ///
    /// <para><b>No proximity, highlighting or snippets.</b> Each is offered by some stores and not others,
    /// and where two offer one they disagree about what it means — proximity is measured in tokens by one
    /// store and in positions by another, and SQL Server's count excludes the search terms themselves. A name
    /// in a shared package that means something different per adapter is worse than no name, because a query
    /// written against one store then plans against another and answers differently rather than failing.
    /// Phrase, prefix and fuzzy are here, as <see cref="ClrFtPhrase"/>, <see cref="ClrFtPrefix"/> and
    /// <see cref="ClrFtFuzzy"/>: every surveyed store has the first, and the three agree on what they
    /// mean.</para>
    /// </remarks>
    public sealed class FullTextOperatorTable : SqlOperatorTable
    {

        /// <summary>
        /// <c>CLR_FT_CONTAINS(searched, keyword)</c>. Whether the keyword occurs in what is searched.
        /// </summary>
        /// <remarks>
        /// Exactly two operands. <c>CLR_FT_CONTAINS_ALL</c> with one keyword means the same thing and is
        /// accepted; this exists because a single-keyword search is the common one and reads better written
        /// as itself.
        /// </remarks>
        public static readonly SqlFunction ClrFtContains =
            Predicate("CLR_FT_CONTAINS", [FullTextOperand.Searched, FullTextOperand.Term], null, SqlOperandCountRanges.of(2));

        /// <summary>
        /// <c>CLR_FT_CONTAINS_ALL(searched, keyword, …)</c>. Whether every keyword occurs.
        /// </summary>
        public static readonly SqlFunction ClrFtContainsAll =
            Predicate("CLR_FT_CONTAINS_ALL", [FullTextOperand.Searched], FullTextOperand.Term, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_CONTAINS_ANY(searched, keyword, …)</c>. Whether any keyword occurs.
        /// </summary>
        public static readonly SqlFunction ClrFtContainsAny =
            Predicate("CLR_FT_CONTAINS_ANY", [FullTextOperand.Searched], FullTextOperand.Term, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_SCORE(searched, keyword, …)</c>. How well what is searched matches the keywords.
        /// </summary>
        /// <remarks>
        /// <para>The number itself means nothing across stores and is not meant to: BM25 from one, cover
        /// density from another, a normalised similarity from a third. What is portable is the <em>order</em>
        /// it puts rows in, which is what a query ordering by it is asking for.</para>
        ///
        /// <para><b>Where a score may appear is the adapter's to say, not this package's.</b> Cosmos permits
        /// one in an <c>ORDER BY RANK</c> clause and nowhere else — a projected score is rejected outright by
        /// the service — while PostgreSQL's <c>ts_rank</c> projects like any other function. So this is
        /// declared as an ordinary scalar and an adapter refuses the placements its store refuses.</para>
        ///
        /// <para><b>And whether it is an expression at all is the adapter's too.</b> SQL Server has no scalar
        /// rank: <c>CONTAINS</c> and <c>FREETEXT</c> are predicates answering true or false, and the rank is a
        /// <c>RANK</c> column of the table <c>CONTAINSTABLE</c> or <c>FREETEXTTABLE</c> returns, reached by
        /// joining it to the base table on the full text index's unique key. So an adapter there turns this
        /// call into a join rather than into a fragment of one, and one that cannot supply the key declines
        /// it. That is the widest thing an adapter has to do with one of these, and it is why the scalar form
        /// is the right thing to declare: it is what a query wants to write, and what realising it costs is a
        /// property of the store.</para>
        /// </remarks>
        public static readonly SqlFunction ClrFtScore =
            Scoring("CLR_FT_SCORE", [FullTextOperand.Searched], FullTextOperand.Term, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_RRF(score, score, …)</c>. Several scores fused into one by reciprocal rank fusion.
        /// </summary>
        /// <remarks>
        /// Here because hybrid search is what full text is usually half of: a keyword score and a vector
        /// similarity, combined into one ordering. The operands are scores rather than keywords, which is why
        /// it has no searched position.
        /// </remarks>
        public static readonly SqlFunction ClrFtRrf =
            Scoring("CLR_FT_RRF", [], FullTextOperand.Score, SqlOperandCountRanges.from(2));

        /// <summary>
        /// <c>CLR_FT_PHRASE(text)</c>. The text as an ordered phrase, in a keyword position.
        /// </summary>
        /// <remarks>
        /// <para><b>This exists because a multi-word keyword is not portable.</b> Cosmos reads
        /// <c>FullTextContains(c.text, "red bicycle")</c> as a phrase; PostgreSQL's <c>plainto_tsquery</c>
        /// reads the same two words as <c>red &amp; bicycle</c>, which matches a document holding them
        /// paragraphs apart. So a bare multi-word keyword means different things per store &#8212; the failure
        /// this package exists to prevent &#8212; and saying which is meant is the fix. A single-word term is
        /// the same either way and needs none of this.</para>
        ///
        /// <para>Every surveyed store has one: <c>phraseto_tsquery</c> and <c>&lt;-&gt;</c>, SQL Server's
        /// quoted term, MySQL's quoted boolean term, FTS5's quoted phrase, <c>match_phrase</c>, Atlas's
        /// <c>phrase</c>. Cosmos's is a plain multi-word term, so an adapter there renders it by
        /// unwrapping.</para>
        /// </remarks>
        public static readonly SqlFunction ClrFtPhrase =
            Term("CLR_FT_PHRASE", [FullTextOperand.Text]);

        /// <summary>
        /// <c>CLR_FT_PREFIX(text)</c>. Anything beginning with the text, in a keyword position.
        /// </summary>
        /// <remarks>
        /// <c>to_tsquery('a:*')</c>, SQL Server's <c>"a*"</c>, MySQL's <c>a*</c>, FTS5's <c>a*</c>,
        /// <c>match_phrase_prefix</c>, Atlas's <c>wildcard</c>. Cosmos has no prefix search, so a Cosmos
        /// adapter declines this one &#8212; which is the point of an adapter being allowed to decline.
        /// </remarks>
        public static readonly SqlFunction ClrFtPrefix =
            Term("CLR_FT_PREFIX", [FullTextOperand.Text]);

        /// <summary>
        /// <c>CLR_FT_FUZZY(text, edits)</c>. The text within a number of edits, in a keyword position.
        /// </summary>
        /// <remarks>
        /// Cosmos's <c>{"term": "bycycle", "distance": 2}</c>, Elasticsearch's <c>fuzziness</c>, Atlas
        /// Search's <c>fuzzy.maxEdits</c>. The edit count is Levenshtein in all three and capped at two by
        /// Cosmos and by Atlas; the cap is not enforced here, because the store is what has to honour it and
        /// a package that guessed the cap would refuse a store that later raised it.
        /// </remarks>
        public static readonly SqlFunction ClrFtFuzzy =
            Term("CLR_FT_FUZZY", [FullTextOperand.Text, FullTextOperand.Distance]);

        /// <summary>
        /// <c>CLR_FT_WEIGHT(score, weight)</c>. A score counting for more or less than the others fused with
        /// it.
        /// </summary>
        /// <remarks>
        /// <para>Cosmos's <c>RRF</c> takes its weights as a trailing array &#8212;
        /// <c>RRF(f1, f2, [0.9, 0.1])</c> &#8212; which is positional, so a weight and the score it belongs to
        /// are kept in step by counting. Written as a wrapper they cannot come apart, and an adapter whose
        /// store wants the array builds it by walking operands it already has to walk.</para>
        ///
        /// <para>It answers a score, so it goes wherever a score goes, and a store with no weighting renders
        /// the inner one and declines only where the weight is not one.</para>
        /// </remarks>
        public static readonly SqlFunction ClrFtWeight =
            Scoring("CLR_FT_WEIGHT", [FullTextOperand.Score, FullTextOperand.Weight], null, SqlOperandCountRanges.of(2));

        /// <summary>
        /// Determines whether an operator is the named one, whichever route resolved it.
        /// </summary>
        /// <remarks>
        /// <b>By name, and never by identity.</b> A call resolved through a schema carries a
        /// <c>SqlUserDefinedFunction</c> Calcite built around the declaration — same name, different object —
        /// so an identity test recognises a call reached through a chained operator table and silently fails
        /// to recognise the same call reached through a connection. What an adapter does with the answer is
        /// usually to render the call, and the name is the whole of what rendering needs.
        /// </remarks>
        /// <param name="op">The operator to test.</param>
        /// <param name="function">The operator it should be.</param>
        /// <returns><c>true</c> where the operator is that one.</returns>
        public static bool Matches(SqlOperator? op, SqlFunction function)
        {
            return op is not null && function is not null && op.getName() == function.getName();
        }

        /// <summary>
        /// Determines whether an operator is one of these, whichever route resolved it.
        /// </summary>
        /// <param name="op">The operator to test.</param>
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
        /// Determines whether an operator answers a relevance score rather than a match.
        /// </summary>
        /// <remarks>
        /// The distinction an adapter needs most often, because the two halves are legal in different places:
        /// a predicate belongs in a <c>WHERE</c>, and where a score belongs is the store's business. Cosmos
        /// takes one in <c>ORDER BY RANK</c> alone.
        /// </remarks>
        /// <param name="op">The operator to test.</param>
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
        /// Defines a full text predicate.
        /// </summary>
        /// <remarks>
        /// Nullable, because a store may have nothing to say about a row a plan keeps — an outer join's
        /// unmatched side, a row outside the searched partition. The failure modes are not symmetrical:
        /// declaring it <c>NOT NULL</c> licences the planner to rewrite on a guarantee the data does not
        /// provide and the answer is wrong, while declaring it nullable at worst costs a rewrite.
        /// </remarks>
        static SqlFunction Predicate(string name, FullTextOperand[] leading, FullTextOperand? repeating, SqlOperandCountRange range)
        {
            var checker = new FullTextOperandTypeChecker(leading, repeating, range);

            return SqlBasicFunction
                .create(name, ReturnTypes.BOOLEAN_NULLABLE, checker, SqlFunctionCategory.SYSTEM)
                .withOperandTypeInference(new FullTextOperandTypeInference(checker));
        }

        /// <summary>
        /// Defines a scoring function.
        /// </summary>
        /// <remarks>
        /// <c>DOUBLE</c> so that a query can order by one and the validator will accept the sort. Nullable for
        /// the reason a predicate is.
        /// </remarks>
        static SqlFunction Scoring(string name, FullTextOperand[] leading, FullTextOperand? repeating, SqlOperandCountRange range)
        {
            var checker = new FullTextOperandTypeChecker(leading, repeating, range);

            return SqlBasicFunction
                .create(name, ReturnTypes.DOUBLE_NULLABLE, checker, SqlFunctionCategory.SYSTEM)
                .withOperandTypeInference(new FullTextOperandTypeInference(checker));
        }

        /// <summary>
        /// Defines a term constructor: a value that occupies a keyword position and says what kind of term it
        /// is.
        /// </summary>
        /// <remarks>
        /// <para><b>Typed <c>ANY</c>, and that is what makes the idea work at all.</b>
        /// <c>FamilyOperandTypeChecker</c> passes an operand whose own family is <c>ANY</c> against any
        /// declared family, so one of these satisfies a <c>CHARACTER</c> keyword position without that
        /// position having to be widened &#8212; a plain keyword is still held to being text.</para>
        ///
        /// <para><b>Structural rather than textual, deliberately.</b> The alternative is what every store
        /// does: put the structure in the string, as <c>to_tsquery('a:*')</c> or <c>'"a*"'</c> or
        /// <c>{"term": …, "distance": …}</c>. A shared package cannot adopt one store's grammar without
        /// making every other adapter parse it, and cannot invent its own without making every adapter parse
        /// that. Written as calls, the structure is in the plan an adapter already walks, and one it cannot
        /// render is one it declines by name.</para>
        ///
        /// <para>These are not values and mean nothing outside a keyword position; one that reaches code
        /// generation is refused like the rest.</para>
        /// </remarks>
        static SqlFunction Term(string name, FullTextOperand[] operands)
        {
            var checker = new FullTextOperandTypeChecker(operands, null, SqlOperandCountRanges.of(operands.Length));

            return SqlBasicFunction
                .create(name, ReturnTypes.@explicit(SqlTypeName.ANY), checker, SqlFunctionCategory.SYSTEM)
                .withOperandTypeInference(new FullTextOperandTypeInference(checker));
        }

        /// <summary>
        /// Determines whether an operator describes a term rather than answering about one.
        /// </summary>
        /// <remarks>
        /// What an adapter asks of an operand in a keyword position: a call this answers for is a term
        /// constructor to be read, and anything else is the keyword itself.
        /// </remarks>
        /// <param name="op">The operator to test.</param>
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
        /// Gets the operator table.
        /// </summary>
        /// <returns>The table.</returns>
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
