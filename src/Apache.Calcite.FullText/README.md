# Apache.Calcite.FullText

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.FullText)](https://www.nuget.org/packages/Apache.Calcite.FullText)

**Apache.Calcite.FullText** declares a set of `CLR_FT_*` full text search operators for [Apache Calcite](https://calcite.apache.org/): their names, arities, operand types and return types. A query can then be written against one vocabulary, and each store adapter translates it into the store's own full text syntax.

Calcite has no full text operators of its own, and every store spells full text differently: PostgreSQL has `@@` over `tsvector`, SQL Server `CONTAINS` and `FREETEXT`, MySQL `MATCH … AGAINST`, SQLite FTS5 `MATCH`, Cosmos DB `FullTextContains`. This package gives adapters a shared set of names to push down.

Targets **.NET 8**. It does not depend on any other package in this repository.

## Nothing here evaluates a search

These operators have **no in-process implementation, and never will**. Which documents match, and how they score, depends on the store's analyzer (tokenising, case folding, stemming, stopwords, language), and an approximation computed in .NET would give different answers from the store.

So a `CLR_FT_*` call only works when a store adapter pushes it down into the store's query. If a plan still contains a call when Calcite compiles it (because no adapter handles the table, or the adapter declined that call or that position), the statement fails. Through the schema route the failure explains itself:

```
CLR_FT_CONTAINS is evaluated by the store and has no in-process body. Which documents match is
decided by the store's analyzer, and approximating one would answer differently from the store for
the same query. This plan asks for the value somewhere the call could not be pushed down.
```

Through the operator table route it is Calcite's own error for an operator it cannot implement.

## Install

```sh
dotnet add package Apache.Calcite.FullText
```

## Registering the operators

There are two ways to make the names resolvable. **Use one or the other, never both** (see [Known limitations](#known-limitations)).

**Operator table**, for a host that builds its own validator or `Frameworks` configuration. Chain `FullTextOperatorTable` onto the tables you already use:

```csharp
using Apache.Calcite.FullText.Sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.util;

var operatorTable = SqlOperatorTables.chain(
    SqlStdOperatorTable.instance(),
    FullTextOperatorTable.Instance());
```

**Schema functions**, for everyone else, including a plain `jdbc:calcite:` connection, which cannot chain an operator table. Add the declarations to a schema:

```csharp
using Apache.Calcite.FullText.Schema;

FullTextSchema.AddTo(rootSchema);
```

An adapter that implements `Schema.getFunctions` itself can merge `FullTextSchema.Functions()` into what it returns, so the operators arrive with its tables.

An unqualified function name is looked up only in the connection's default schema and in the root schema, never in other subschemas, so declare the functions at every level a connection may use as its default.

## Writing queries

```sql
SELECT ID
FROM DOCS
WHERE CLR_FT_CONTAINS_ALL(BODY, 'steel', CLR_FT_FUZZY('bycycle', 2))
ORDER BY CLR_FT_SCORE(BODY, 'steel') DESC
FETCH FIRST 10 ROWS ONLY
```

**Predicates**, each answering a nullable `BOOLEAN`:

| | |
| --- | --- |
| `CLR_FT_CONTAINS(searched, keyword)` | whether the keyword occurs |
| `CLR_FT_CONTAINS_ALL(searched, keyword, …)` | whether every keyword occurs |
| `CLR_FT_CONTAINS_ANY(searched, keyword, …)` | whether any keyword occurs |

**Scores**, each answering a nullable `DOUBLE`:

| | |
| --- | --- |
| `CLR_FT_SCORE(searched, keyword, …)` | how well the keywords match |
| `CLR_FT_RRF(score, score, …)` | several scores fused by reciprocal rank fusion |
| `CLR_FT_WEIGHT(score, weight)` | a score weighted relative to the others it is fused with |

**Term constructors**, which stand where a keyword goes and say what kind of term it is:

| | |
| --- | --- |
| `CLR_FT_PHRASE(text)` | the words together, in order |
| `CLR_FT_PREFIX(text)` | any word beginning with the text |
| `CLR_FT_FUZZY(text, edits)` | the text within an integer number of edits |

Notes on the operands:

- The **searched** operand accepts any type: a character column, an untyped (`ANY`) document property, an array of strings, or a `ROW` of columns for a store that searches several at once. Which of these a store can search is its adapter's decision.
- **Keywords** are character values, passed as separate operands rather than as a query string in any store's grammar. A term constructor is accepted in any keyword position, and constructors mix with plain keywords: `CLR_FT_CONTAINS_ALL(BODY, 'red', CLR_FT_FUZZY('bycycle', 2), CLR_FT_PREFIX('mount'))`.
- Use `CLR_FT_PHRASE` for a multi-word search. Stores read a bare multi-word keyword differently: Cosmos DB treats `'red bicycle'` as a phrase, PostgreSQL's `plainto_tsquery` matches the two words anywhere in the document.
- A score's value is whatever the store's ranking computes and is not comparable across stores; its ordering is what a query can rely on. Where a score may appear depends on the store: Cosmos DB accepts one only in `ORDER BY RANK`, and SQL Server has no scalar rank at all.
- `CLR_FT_RRF` takes any numeric score, so a full text score can be fused with an adapter's own vector similarity for hybrid search.

## Pushing the operators down: what an adapter does

An adapter that supports full text search translates these calls into its store's query, in the rules that move filters, projections and sorts into its own convention.

1. **Recognise calls by name.** Use `FullTextOperatorTable.Matches(op, FullTextOperatorTable.ClrFtContains)`, `IsFullText`, `IsScoring` and `IsTerm`. Do not compare operators by reference: a call resolved through the schema route carries a `SqlUserDefinedFunction` that Calcite built, not the field in `FullTextOperatorTable`.
2. **Decline what the store cannot express, in the rule's match.** If an expression contains a `CLR_FT_*` call the store cannot render, or one in a position the store does not allow (a projected score, for example), do not push that node down. Every remaining call makes the statement fail when it is compiled, so declining is how an adapter reports "not supported".
3. **Read keyword positions.** For each operand after the searched one, `IsTerm` says whether it is a term constructor (`CLR_FT_PHRASE`, `CLR_FT_PREFIX`, `CLR_FT_FUZZY`) to be rendered as that kind of term, or a plain keyword. A constructor nested in another (`CLR_FT_FUZZY(CLR_FT_PHRASE(…), 1)`) passes validation, so decline it.
4. **Treat scores separately.** `IsScoring` identifies `CLR_FT_SCORE`, `CLR_FT_RRF` and `CLR_FT_WEIGHT`. A store that takes weights positionally builds them from the `CLR_FT_WEIGHT` wrappers; one with no weighting can render a weight of one and decline any other.
5. **Optionally simplify first.** `FullTextRules.Simplify(rexBuilder, expression)` returns an expression with the rewrites below applied, which reduces the number of shapes a renderer has to handle.

How each store might spell the operators:

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

A dash means the store has no equivalent, and its adapter declines the call. SQL Server's rank is a column of the `CONTAINSTABLE` result rather than a scalar, so an adapter there has to join to it on the full text key, or decline `CLR_FT_SCORE`.

## Simplifying a plan

`FullTextRules.Program()` is a `Programs.hep` pass to run ahead of the host's own program:

```csharp
using Apache.Calcite.FullText.Rel.Rules;

var config = Frameworks.newConfigBuilder()
    .defaultSchema(schema)
    .programs(Programs.sequence(FullTextRules.Program(), Programs.standard()))
    .build();
```

It rewrites filters, projections and join conditions:

| | |
| --- | --- |
| `CLR_FT_CONTAINS_ALL(x, 'a', 'b', 'a')` → `CLR_FT_CONTAINS_ALL(x, 'a', 'b')` | a repeated keyword is dropped; likewise for `CONTAINS_ANY` |
| `CLR_FT_CONTAINS_ALL(x, k)` → `CLR_FT_CONTAINS(x, k)` | likewise for `CONTAINS_ANY` |
| `CLR_FT_FUZZY(t, 0)` → `t` | zero edits is an exact match |
| `CLR_FT_WEIGHT(s, 1)` → `s` | only where `s` is already a nullable `DOUBLE` |
| `CLR_FT_CONTAINS(x, a) AND CLR_FT_CONTAINS(x, b)` → `CLR_FT_CONTAINS_ALL(x, a, b)` | and an `OR` into `CLR_FT_CONTAINS_ANY`; only for calls over the same searched expression |

Every rewrite preserves the value, nulls included. `CLR_FT_PHRASE` of a single word is not unwrapped, because whether text is one token is the analyzer's decision, and `CLR_FT_RRF` is never unwrapped, because fusion preserves an ordering rather than a value.

Run these as a separate pass rather than adding them to a `VolcanoPlanner`: Volcano compares plans by row count only, so a simplified filter is never cheaper than the original and the planner may keep either.

The operators are not declared strict (`Strong.Policy.ANY`), so `RexSimplify` does not rewrite `CLR_FT_CONTAINS(BODY, 'a') IS NULL` into `BODY IS NULL`; whether a store answers null or false for a missing value is the store's.

## Known limitations

- **Registering both routes breaks `ARRAY` columns.** With the operator table chained *and* the schema functions declared, a call whose searched operand is an `ARRAY` column fails with `IllegalArgumentException: must contain type: ANY`. With two candidate functions Calcite runs a type-precedence pass that throws for an `ARRAY` argument, rather than declining. Register one route only.
- **Variadic operators are bounded through a schema.** A schema function takes a fixed number of operands, so each variadic operator is declared once per arity up to `FullTextSchema.VariadicOperandLimit` (16 operands, the searched one included). A call with more resolves only through the operator table.
- **Every parameter is required.** There are no optional operands, because Calcite fills an omitted optional parameter with `DEFAULT`, which no store can render.
- **A nested term constructor validates.** `CLR_FT_FUZZY(CLR_FT_PHRASE('red bicycle'), 1)` passes validation on both routes, because a constructor is typed `ANY` and Calcite accepts `ANY` against any declared family. An adapter should decline it.

## Not supported

- **Query strings.** There is no operator taking a store's query grammar.
- **A language or analyzer argument.** It could not be told apart from a keyword, since both are character strings. An adapter that needs one takes it from its own configuration, as most stores configure it on the index or column anyway.
- **Proximity, highlighting and snippets.** Stores disagree on what they mean or do not offer them.
- **A `FREETEXT`-style mode** distinct from exact matching.
- **Vector search.** `CLR_FT_RRF` fuses any numeric score, but vector operators themselves are not part of this package.

## The names

`CLR_` is this repository's prefix, so no Calcite library function will share a name with these; a connection that sets `fun` chains its libraries ahead of the schema's functions, and a shared name would silently take their place. `FT_` follows ISO/IEC 13249-2 (SQL/MM Full-Text), which prefixes its full text types `FT_`. The bare name `CONTAINS` is unavailable: Calcite already uses it for the SQL:2011 period predicate, and the parser reserves it.
