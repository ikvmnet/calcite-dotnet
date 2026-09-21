# Apache.Calcite.FullText

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.FullText)](https://www.nuget.org/packages/Apache.Calcite.FullText)

**Apache.Calcite.FullText** gives [Apache Calcite](https://calcite.apache.org/) a set of `CLR_FT_*` operators for full text search, and the plumbing that makes them nameable from a query.

Calcite has no full text operator — not in `SqlStdOperatorTable`, and not in any of the fourteen `SqlLibrary` tables, at any casing. Every store that offers full text meets the same problem and each solves it privately: PostgreSQL has `@@` over `tsvector`, SQL Server `CONTAINS` and `FREETEXT`, MySQL `MATCH … AGAINST`, SQLite FTS5 `MATCH`, Cosmos DB `FullTextContains`, Elasticsearch and MongoDB their own. So an adapter that wants any of them invents names, invents the resolution plumbing, and a query written against one store cannot be planned against another.

This package declares the names, arities and types once, offers them by both routes a query can reach a name, and owns the collision tripwire. It is **optional** and nothing else in the repository depends on it.

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

## Install

```sh
dotnet add package Apache.Calcite.FullText
```

## Using it

A host driving its own planner chains the operator table:

```csharp
using Apache.Calcite.FullText.Sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.util;

var operatorTable = SqlOperatorTables.chain(
    SqlStdOperatorTable.instance(),
    FullTextOperatorTable.Instance());
```

Anyone else registers them on a schema, which works through the stock `jdbc:calcite:` driver with nothing chained and nothing subclassed:

```csharp
using Apache.Calcite.FullText.Schema;

FullTextSchema.AddTo(rootSchema);
```

An adapter that implements `Schema.getFunctions` itself merges `FullTextSchema.Functions()` into its own, so the operators arrive with its tables. Declare at **every level the connection might be rooted at** — an unqualified name resolves against the connection's default schema and the root, and never a subschema.

**Do one or the other, not both.** See *Registering both routes* below; with both in place, an `ARRAY` column stops working.

```sql
SELECT ID
FROM DOCS
WHERE CLR_FT_CONTAINS_ALL(BODY, 'steel', CLR_FT_FUZZY('bycycle', 2))
ORDER BY CLR_FT_SCORE(BODY, 'steel') DESC
FETCH FIRST 10 ROWS ONLY
```

## What is here

**Predicates and scores.**

| | |
| --- | --- |
| `CLR_FT_CONTAINS(searched, keyword)` | whether the keyword occurs |
| `CLR_FT_CONTAINS_ALL(searched, keyword, …)` | whether every keyword occurs |
| `CLR_FT_CONTAINS_ANY(searched, keyword, …)` | whether any keyword occurs |
| `CLR_FT_SCORE(searched, keyword, …)` | how well the keywords match |
| `CLR_FT_RRF(score, …)` | several scores fused by reciprocal rank fusion |

**Term constructors**, which stand where a keyword goes and say what kind of term it is.

| | |
| --- | --- |
| `CLR_FT_PHRASE(text)` | an ordered phrase |
| `CLR_FT_PREFIX(text)` | anything beginning with the text |
| `CLR_FT_FUZZY(text, edits)` | the text within a number of edits |

**A score modifier.**

| | |
| --- | --- |
| `CLR_FT_WEIGHT(score, weight)` | a score counting for more or less than the others fused with it |

The **searched** position takes anything — a character column, a document property typed `ANY`, an array of strings, a `ROW` of columns where the store searches several at once. What a store can actually search is the store's business; a shared package that narrowed this would be refusing a store rather than catching a mistake. Keywords are text, so `CLR_FT_CONTAINS(BODY, 42)` is refused at validation.

### Why term constructors rather than more functions

Because a bare multi-word keyword is not portable, and that is a correctness problem rather than an inconvenience. Cosmos reads `FullTextContains(c.text, "red bicycle")` as a **phrase**; PostgreSQL's `plainto_tsquery` reads the same two words as `red & bicycle`, which matches a document holding them paragraphs apart. The same query, two answers, no error. `CLR_FT_PHRASE` is what says which was meant.

The alternative everywhere else is to put the structure in the string — `to_tsquery('a:*')`, `'"a*"'`, `{"term": "red", "distance": 1}`. A shared package cannot adopt one store's grammar without making every other adapter parse it, and cannot invent its own without making every adapter parse that. As calls, the structure is in the plan an adapter already walks, and one it cannot render it declines by name.

Constructors also **mix**: `CLR_FT_CONTAINS_ALL(BODY, 'red', CLR_FT_FUZZY('bycycle', 2), CLR_FT_PREFIX('mount'))` carries an exact keyword, a fuzzy one and a prefix in one call, which a `CLR_FT_CONTAINS_ALL_FUZZY` could not.

## What each store spells these

Measured against each store's own reference, and the reason the vocabulary is nobody's in particular.

| | Cosmos DB | PostgreSQL | SQL Server | MySQL | SQLite FTS5 | Elasticsearch | Atlas Search |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `CLR_FT_CONTAINS` | `FullTextContains` | `@@ plainto_tsquery` | `CONTAINS` | `MATCH … AGAINST` | `MATCH` | `match` | `text` |
| `CLR_FT_CONTAINS_ALL` | `FullTextContainsAll` | `to_tsquery('a & b')` | `CONTAINS('a AND b')` | `+a +b` boolean | `a AND b` | `operator: and` | `compound.must` |
| `CLR_FT_CONTAINS_ANY` | `FullTextContainsAny` | `to_tsquery('a \| b')` | `CONTAINS('a OR b')` | natural language | `a OR b` | `operator: or` | `compound.should` |
| `CLR_FT_SCORE` | `FullTextScore` | `ts_rank`, `ts_rank_cd` | `CONTAINSTABLE.RANK` | `MATCH … AGAINST` | `bm25()`, `rank` | `_score` | `searchScore` |
| `CLR_FT_RRF` | `RRF` | — | — | — | — | rrf retriever | `$rankFusion` |
| `CLR_FT_PHRASE` | a multi-word term | `phraseto_tsquery`, `<->` | `'"a b"'` | `"a b"` boolean | `"a b"` | `match_phrase` | `phrase` |
| `CLR_FT_PREFIX` | — | `to_tsquery('a:*')` | `'"a*"'` | `a*` boolean | `a*` | `match_phrase_prefix` | `wildcard` |
| `CLR_FT_FUZZY` | `{"term": …, "distance": …}` | — | — | — | — | `fuzziness` | `fuzzy.maxEdits` |
| `CLR_FT_WEIGHT` | `RRF(…, [0.9, 0.1])` | `ts_rank` weights | `ISABOUT(… WEIGHT(0.9))` | `>` and `<` boolean | `bm25(t, 10.0)` | `boost` | `score.boost` |

**Everything Cosmos DB offers is expressible**, which is the bar this vocabulary was set.
`FullTextResolutionTests.ShouldExpressEveryCosmosFullTextConstruct` pins one statement per Cosmos construct, on both routes.

**Where a call is legal, how it renders, and what it costs are the adapter's.** Cosmos permits a score in an `ORDER BY RANK` clause and nowhere else, and rejects a projected one outright. SQL Server has **no scalar rank at all**: `CONTAINS` and `FREETEXT` are predicates, and the relevance value is a `RANK` column of the table `CONTAINSTABLE` returns, reached by joining it to the base table on the full text index's unique key — so `CLR_FT_SCORE` there becomes a join rather than an expression, and an adapter that cannot supply the key declines it. The scalar form is still the right thing to declare: it is what a query wants to write.

## There is no evaluator, and there will not be

Every operator here is declared with **no body**. A full text answer is the store's analyzer — tokenising, case folding, stemming, stopwords, per language — and which documents match is whatever that analyzer decides. An in-process approximation would answer differently from the store for the same query, which is worse than not answering: a predicate rechecked against it would discard rows the store correctly returned.

So a call that no rule pushed down is refused while the plan is turned into code, in words that say why:

```
CLR_FT_CONTAINS is evaluated by the store and has no in-process body. Which documents match is
decided by the store's analyzer, and approximating one would answer differently from the store for
the same query. This plan asks for the value somewhere the call could not be pushed down.
```

The declarations implement `ImplementableFunction` and throw, rather than declining the interface — which would leave Calcite to report `User defined function CLR_FT_SCORE must implement ImplementableFunction`, naming an interface rather than a reason and reading as a defect in the adapter. Same refusal, same moment, with the reason attached.

## The names

**`CLR_` is this repository's namespace**, and it is what makes the collision argument structural rather than empirical. A connection chains the operator table its `fun` property names *before* the catalog reader, and overload resolution takes the first candidate whose arity fits — so a name Calcite also used would shadow these silently, and only for hosts that set `fun`. No Calcite library will ever ship a `CLR_` function.

**`FT_` inside it is the family**, and it is borrowed knowingly. Full text *is* standardised: **ISO/IEC 13249-2, *SQL multimedia and application packages — Part 2: Full-Text***, the same 13249 series whose Part 3 is Spatial. It defines a `FullText` type whose `Contains` and `Score` methods take a structured pattern, plain SQL functions of those names beside them, and it prefixes its pattern type hierarchy `FT_` — `FT_Pattern`, `FT_WordOrPhrase`, `FT_StemmedWord`, `FT_Proxi`, `FT_Soundex`, `FT_Fuzzy`, `FT_IsAbout`.

**Its shape is not adopted, and its spelling is unavailable.** The standard's `Contains(doc, pattern)` is binary, with all the structure — `&`, `|`, `NOT`, `STEMMED FORM OF`, `SOUNDS LIKE`, `IN SAME SENTENCE AS`, `THESAURUS` — inside the pattern string, so adopting it puts a pattern parser in every adapter. And bare `CONTAINS` is taken twice over: `SqlStdOperatorTable.CONTAINS` is the SQL:2011 period predicate, and the parser reserves the word.

**Taking a store's spellings instead was considered**, that being what Calcite did for spatial — 69 of the 144 `ST_*` in its reference are PostGIS or H2GIS extensions rather than OpenGIS, and its acknowledgements name PostGIS's tests as a reference implementation. It works there because PostGIS's spatial surface already *is* a function family over one type Calcite models as `GEOMETRY`: only the names had to be borrowed. Full text has no such store. PostgreSQL spells it `tsvector @@ tsquery` — two types Calcite has no `SqlTypeName` for, plus a query grammar — and the rest agree with it and with each other on nothing.

**These may be supplanted by upstream Calcite**, and the namespace is what makes that survivable. If Calcite ever ships full text operators of its own, ours do not collide with them, are not shadowed by them, and can be mapped onto them or deprecated deliberately. `ShouldFindNoFullTextOperatorInCalcite` is the watch for it: it is not a guard against breakage — with `CLR_` nothing can break — but the test that will fail the day the question is worth revisiting.

## Simplifying a plan

`FullTextRules` is a pass a host sequences in front of whatever program it runs.

```csharp
using Apache.Calcite.FullText.Rel.Rules;

var config = Frameworks.newConfigBuilder()
    .defaultSchema(schema)
    .programs(Programs.sequence(FullTextRules.Program(), Programs.standard()))
    .build();
```

| | |
| --- | --- |
| `CLR_FT_CONTAINS_ALL(x, k)` → `CLR_FT_CONTAINS(x, k)` | and the same for `CONTAINS_ANY`. One keyword means the same thing either way, which `CLR_FT_CONTAINS`'s own declaration says |
| `CLR_FT_CONTAINS_ALL(x, 'a', 'b', 'a')` → `CLR_FT_CONTAINS_ALL(x, 'a', 'b')` | asking for every one of a list that names a keyword twice is asking for it once |
| `CLR_FT_FUZZY(t, 0)` → `t` | the edit count is Levenshtein in every store that has one, and zero edits is an exact match in all of them |
| `CLR_FT_WEIGHT(s, 1)` → `s` | which `CLR_FT_WEIGHT`'s own declaration says: a store with no weighting renders the inner score and declines only where the weight is not one |
| `CLR_FT_CONTAINS(x, a) AND CLR_FT_CONTAINS(x, b)` → `CLR_FT_CONTAINS_ALL(x, a, b)` | and a disjunction into `CONTAINS_ANY`. That is the form the stores with an all-of or any-of have — Cosmos's `FullTextContainsAll`, PostgreSQL's `to_tsquery('a & b')`, SQL Server's `CONTAINS('a AND b')` — so it is one index lookup where the conjunction was two |

**Every rewrite is an equality of values**, the merges included, and that is worth stating because it is not
obvious. These operators are nullable so that a store can have nothing to say about a row a plan keeps — an
outer join's unmatched side, a row outside the searched partition — and that is a property of the *row*
rather than of the keyword. So every call over one searched expression is null on the same rows, and
`CLR_FT_CONTAINS(x, a) AND CLR_FT_CONTAINS(x, b)` is null exactly where `CLR_FT_CONTAINS_ALL(x, a, b)` is.
The merge needs no filter context to be sound, and only merges calls whose searched expression is the same.

**A pass and not rules on a `VolcanoPlanner`.** `VolcanoCost.isLt` compares the row count and nothing else,
so a filter whose condition was simplified is never *cheaper* than the same filter unsimplified and the
planner keeps whichever it registered first. This is the same argument that keeps `Programs.calc` a hep
pass, and it was measured against the sibling geography package before either was written this way.

**`CLR_FT_PHRASE('steel')` is not unwrapped**, though a single-token phrase is the same search in every
store surveyed. Whether that text is one token is the analyzer's answer and not this package's —
`'red-bicycle'` is two tokens under some analyzers and one under others — and deciding it here is the
in-process approximation the package exists to refuse. Nor is `CLR_FT_RRF` of one score: reciprocal rank
fusion preserves an ordering and not a value.

**Nothing declares these operators strict.** A `Strong.Policy.ANY` would let `RexSimplify` rewrite
`CLR_FT_CONTAINS(BODY, 'a') IS NULL` into `BODY IS NULL`, which is a claim about the store — Cosmos answers
false for a missing property and PostgreSQL answers null. The package cannot know which, so it says
nothing. The sibling geography package does declare it, because there the bodies are its own.

## Known limitations

**Registering both routes.** Chaining the operator table *and* declaring on a schema works for everything except an `ARRAY` column, which then fails with `IllegalArgumentException: must contain type: ANY`. `SqlUtil.lookupSubjectRoutines` returns early at `if (list.size() < 2 || coerce)` before its type-precedence pass, so one route leaves one candidate and never reaches it; both leave two, it runs, and it compares each candidate's parameter type using the *argument's* precedence list — which for `ArraySqlType` accepts only another comparable `ARRAY` and throws otherwise. It is not the `ANY` parameter that causes it: any parameter type but a matching `ARRAY` fails identically, and one typed `ARRAY` would refuse every character column. A pass that throws where it should decline is Calcite's to fix.

**A nested term constructor validates.** `CLR_FT_FUZZY(CLR_FT_PHRASE('red bicycle'), 1)` type-checks, and cannot be made not to: a constructor is typed `ANY` so that it satisfies a `CHARACTER` keyword position, and `FamilyOperandTypeChecker` passes an `ANY` operand against every declared family. The rule that admits a constructor where a keyword goes is the rule that admits one inside another. It could be refused by hand on the operator, but not on the schema declaration — whose checker Calcite builds — and a divergence between the two routes is worse than a call the adapter declines.

**Variadic arity through a schema is bounded.** Calcite derives a function's operand count range from its parameter list, so a schema function is exactly as variadic as the parameters it declares, and each is declared once per arity up to `FullTextSchema.VariadicOperandLimit` (16). A query needing more keywords resolves against the chained operator table, whose checker has no bound.

**One declaration per arity, not one with optional parameters.** `SqlCallBinding.operands` pads a call out to the whole parameter list with `DEFAULT` where there is room under the count range, the position is optional, *and* the checker's parameters are fixed. No store renders `DEFAULT`, so every parameter here is required.

## What is deliberately not here

- **No pattern or query string.** A grammar in a shared package is a grammar every adapter has to parse.
- **No language or analyzer argument**, and the reason is structural: a leading configuration name and a keyword are both character strings, so `CLR_FT_CONTAINS_ALL(BODY, 'english', 'steel')` and `CLR_FT_CONTAINS_ALL(BODY, 'steel', 'frame')` would be the same call. PostgreSQL and SQL Server take one per call, each over a default; Cosmos puts it on the container's full text policy, MySQL on the column's collation, FTS5 on the tokenizer, Elasticsearch on the field mapping. An adapter that needs one takes it from its own configuration.
- **No proximity.** SQL Server's `NEAR((a, b), n)`, FTS5's `NEAR(a b, N)`, PostgreSQL's `<N>`, Elasticsearch's `slop` and Atlas's `near` all exist and do not agree on what the distance counts — tokens in some, positions in others, and SQL Server's `n` excludes the search terms themselves. A name that means something different per adapter is worse than no name.
- **No highlighting or snippets.** `ts_headline`, FTS5's `highlight()` and `snippet()`, and the Elasticsearch and Atlas highlighters return marked-up text rather than answering about a row, and Cosmos and SQL Server have nothing. Worth adding when two stores agree on a shape.
- **No `FREETEXT` mode.** SQL Server's `CONTAINS`/`FREETEXT` split and MySQL's boolean/natural-language modes distinguish exact matching from meaning; PostgreSQL, Cosmos and FTS5 stem unconditionally and have no such switch. A name that is a real distinction in two stores and a synonym in three needs a decision this package has not taken.
- **No vector search.** `CLR_FT_RRF` takes any numeric score, so an adapter's own vector-distance operator fuses with a full text one — which is how hybrid search is written — but a vector vocabulary is a different package.
