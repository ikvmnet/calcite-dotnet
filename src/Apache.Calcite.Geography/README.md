# Apache.Calcite.Geography

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Geography)](https://www.nuget.org/packages/Apache.Calcite.Geography)

**Apache.Calcite.Geography** gives [Apache Calcite](https://calcite.apache.org/) a set of `CLR_ST_GEOG_*` operators that read coordinates as WGS84 and answer in metres.

Calcite has `GEOMETRY` and no `GEOGRAPHY`. Its spatial library is planar [JTS](https://github.com/locationtech/jts) over an unprojected coordinate system, answering in the units of that system. The stores that speak WGS84 — PostGIS `geography`, BigQuery, Snowflake, Elasticsearch `geo_shape`, MongoDB's 2dsphere — are geodesic, and answer in metres. The two disagree about what identically-named functions *mean*, and the disagreement is not a scale factor: the ratio varies with latitude and with bearing, so no conversion of a result recovers it and no transformation of the inputs does either. An ordering is not merely wrong, it is differently ordered.

This package is **optional** and nothing else in the repository depends on it. Reference it when you have geodesic data.

Targets **.NET 8**, and is verified on **.NET 8** and **.NET 10**.

## Install

```sh
dotnet add package Apache.Calcite.Geography
```

## Using it

The operators are declared in a `SqlOperatorTable`, which a host chains onto whatever it already has:

```csharp
using Apache.Calcite.Geography.Sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.util;

var operatorTable = SqlOperatorTables.chain(
    SqlStdOperatorTable.instance(),
    GeographyOperatorTable.Instance());

var config = Frameworks.newConfigBuilder()
    .defaultSchema(schema)
    .operatorTable(operatorTable)
    .build();
```

A column holds a geography by declaring `GeographyTypes.Of(typeFactory)` as its type:

```csharp
using Apache.Calcite.Geography.Rel.Type;

typeFactory.builder()
    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
    .add("LOCATION", GeographyTypes.Of(typeFactory))
    .build();
```

and the values in it are ordinary JTS `Geometry` objects — the same class Calcite's own spatial library carries, because it is the same type.

```sql
SELECT ID
FROM PLACES
WHERE CLR_ST_GEOG_DWITHIN(LOCATION, CLR_ST_GEOG_GEOMFROMTEXT('POINT(-0.1278 51.5074)'), 5000.0)
```

The operators can be reached two ways. A host driving its own planner chains `GeographyOperatorTable.Instance()`, as above. Anyone else registers them on a schema:

```csharp
using Apache.Calcite.Geography.Schema;

GeographySchema.AddTo(rootSchema);
```

which works through the stock `jdbc:calcite:` driver with nothing chained and nothing subclassed — an adapter can call it on the schema it builds, and its functions arrive with its tables. Registering on the root schema makes every operator visible unqualified everywhere on the connection, views in other schemas included. Do one or the other, not both: a name found twice resolves to whichever the lookup reaches first.

## The names

`CLR_` is this repository's namespace, carried by every SQL function it adds to Calcite, and it is what
makes the collision argument structural rather than empirical. A connection chains the operator table its
`fun` property names *ahead* of the catalog reader, and overload resolution takes the first candidate whose
arity fits — so a name Calcite also used would shadow these silently, and only for hosts that set `fun`.
That is not hypothetical here the way it is for full text: Calcite's spatial surface is already 144 names
and still growing out of PostGIS and H2GIS, and `ST_GEOG_` is a plausible thing for it to add. No Calcite
library will ever ship a `CLR_` function.

It also survives these being **supplanted upstream**. If Calcite grows a geodesic reading of its own, ours
does not collide with it, is not shadowed by it, and can be mapped onto it or deprecated deliberately.

`ST_GEOG_` inside the namespace is unchanged: `ST_` is the family every spatial function has carried since
OpenGIS Simple Feature Access, and `GEOG_` is what says the geodesic reading is meant. **These were
`ST_GEOG_*` before, and the rename is a breaking change taken deliberately while this package is
unpublished.**

## There is no GEOGRAPHY type

A geography and a geometry are one type — Calcite's `GEOMETRY`, over `org.locationtech.jts.geom.Geometry`. What says a value is to be read geodesically is the name of the operator applied to it, and nothing else.

**Nothing refuses a mixture.** `ST_DISTANCE(LOCATION, LOCATION)` over geodesic coordinates answers in degrees, and `CLR_ST_GEOG_DISTANCE` over projected ones answers metres as though they were degrees. Both validate, both run. Worse than either, an expression can be half of each: `CLR_ST_GEOG_DISTANCE(ST_BUFFER(LOCATION, 0.1), OTHER)` buffers in degrees and then measures in metres.

There is no run-time guard underneath, either. The SRID is a tag on the JTS instance, and Calcite's own spatial functions mostly drop it off a geometry they derive — `ST_Buffer`, `ST_Centroid`, `ST_Envelope` and `ST_Intersection` all return zero from an operand stamped 4326 (`SridPropagationTests` pins this). So a `CLR_ST_GEOG_` operator cannot refuse a value for want of a stamp: it may be geodesic and merely have passed through one of Calcite's functions.

**This was a choice, and the alternative was measured.** A `JavaType` subclass answering `SqlTypeName.OTHER` gives the whole guarantee back — Calcite's planar functions then refuse a geography at validation. It cannot be registered on a schema: `CalciteCatalogReader.toOp` builds a fixed-parameter operand checker for every schema function, that checker's parameter types go through the assignment rules, and `SqlTypeAssignmentRule` has no entry for `OTHER` — so routine resolution throws `AssertionError: No assign rules for OTHER defined` rather than rejecting. Nor can the type be added to `SqlTypeName`, which is a closed Java enum and the key to that table and several others.

Since a schema is the only way an adapter can bring its functions with it, and bringing them is the point, the type gave way to the registration.

## What is here

The names mirror Calcite's `ST_*` one for one, under this repository's `CLR_` namespace — `ST_DISTANCE` becomes `CLR_ST_GEOG_DISTANCE`. This is the first increment; Calcite's spatial library is about 130 names and every one of them needs a declaration, because Calcite's own read the plane.

**Constructors** — how a geography comes into existence in a query. The return type is `GEOMETRY`, and the result carries SRID 4326, though see above for how little that is worth once one of Calcite's own functions has touched it.

| | |
| --- | --- |
| `CLR_ST_GEOG_GEOMFROMTEXT(VARCHAR [, INTEGER])` | reads WKT |
| `CLR_ST_GEOG_GEOMFROMWKT(VARCHAR [, INTEGER])` | the same, under Calcite's other spelling |
| `CLR_ST_GEOG_GEOMFROMGEOJSON(VARCHAR)` | reads GeoJSON |

Both arities are Calcite's. The SRID a caller may name has to be 4326 and anything else is refused rather than ignored — a geography is WGS84 and there is no second reference system to reproject into, which is the same reason `ST_SETSRID` and `ST_TRANSFORM` have no counterpart at all.

**The crossing between the two readings.** Free at run time, since both sides are the same JTS object; explicit, because losing the geodesic reading should be something you wrote down.

| | |
| --- | --- |
| `CLR_ST_GEOG_ASGEOM(GEOMETRY)` | say the value is to be read as a plane from here on; converts nothing |
| `CLR_ST_GEOM_ASGEOG(GEOMETRY)` | say the value is to be read geodesically from here on; converts nothing |

**Relations.**

| | |
| --- | --- |
| `CLR_ST_GEOG_INTERSECTS` | any point in common |
| `CLR_ST_GEOG_DISJOINT` | none |
| `CLR_ST_GEOG_WITHIN`, `CLR_ST_GEOG_CONTAINS` | inside, with the interiors meeting |
| `CLR_ST_GEOG_COVEREDBY`, `CLR_ST_GEOG_COVERS` | inside, boundary allowed |
| `CLR_ST_GEOG_EQUALS` | the same set of places |
| `CLR_ST_GEOG_ENVELOPESINTERSECT` | bounding boxes meet |
| `CLR_ST_GEOG_ISVALID` | valid on the sphere |

`WITHIN` and `CONTAINS` are the DE-9IM relations JTS means by the words, not plain containment: a point on a polygon's boundary is covered by it and not within it.

**Measurements**, in metres and square metres rather than in degrees, on the WGS84 ellipsoid.

The two halves run on different engines, and deliberately. The predicates are S2's, which is a sphere; the measurements are [GeographicLib](https://geographiclib.sourceforge.io/)'s, which is the ellipsoid. S2 answers *which* points of two shapes are closest and GeographicLib answers *how far apart* they are. Measuring on the sphere was wrong by up to 0.56% against a live geodesic service — enough that an adapter could not recheck a predicate it had pushed down, since the recheck discards rows the store correctly returned. A degree of longitude at the equator is 111319.49 m and a degree of latitude is 110574.39 m; that these differ is the whole of what says the answers are ellipsoidal.

| | |
| --- | --- |
| `CLR_ST_GEOG_DISTANCE`, `CLR_ST_GEOG_DWITHIN` | the distance between two geographies |
| `CLR_ST_GEOG_MAXDISTANCE` | the greatest distance between a coordinate of one and a coordinate of the other |
| `CLR_ST_GEOG_CLOSESTCOORDINATE`, `CLR_ST_GEOG_FURTHESTCOORDINATE` | the coordinate of a geography nearest or furthest from a point |
| `CLR_ST_GEOG_CLOSESTPOINT` | the point of one geography nearest another, which may fall part way along an edge |
| `CLR_ST_GEOG_LONGESTLINE` | the line joining the pair `CLR_ST_GEOG_MAXDISTANCE` measures |
| `CLR_ST_GEOG_ENVELOPE`, `CLR_ST_GEOG_EXTENT` | the bounding rectangle, which wraps rather than spanning the globe when the shape crosses the antimeridian |
| `CLR_ST_GEOG_EXPAND` | that rectangle grown by a distance in metres |
| `CLR_ST_GEOG_DENSIFY` | vertices inserted along the geodesic so no edge exceeds a distance in metres |
| `CLR_ST_GEOG_PROJECTPOINT` | a point projected onto a line, landing on the geodesic |
| `CLR_ST_GEOG_INTERSECTION`, `CLR_ST_GEOG_DIFFERENCE`, `CLR_ST_GEOG_SYMDIFFERENCE`, `CLR_ST_GEOG_UNARYUNION` | the overlay set, over areas, bounded by geodesics |
| `CLR_ST_GEOG_CONVEXHULL` | convex on the sphere, so its edges bow poleward of a planar hull's |
| `CLR_ST_GEOG_SIMPLIFY` | vertices removed within a tolerance in metres |
| `CLR_ST_GEOG_CENTROID` | the centre as a direction from the Earth's centre, so it lands in the shape across the antimeridian |
| `CLR_ST_GEOG_BUFFER` | the region within a distance in metres, so it covers the same ground at every latitude |
| `CLR_ST_GEOG_ISSIMPLE`, `CLR_ST_GEOG_ISRING` | whether a shape touches itself, asked of geodesic edges |
| `CLR_ST_GEOG_BOUNDINGCIRCLE` | the smallest circle holding a shape, of constant distance rather than constant degrees |
| `CLR_ST_GEOG_LOCATEALONG` | a point a fraction along each segment, offset sideways in metres |
| `CLR_ST_GEOG_MINIMUMDIAMETER` | the narrowest way across, measured between great circles |
| `CLR_ST_GEOG_OFFSETCURVE` | a line drawn a distance in metres to one side, whichever way it runs |
| `CLR_ST_GEOG_MAKEELLIPSE` | an ellipse whose width and height are metres, so equal ones are round on the ground |
| `CLR_ST_GEOG_LENGTH`, `CLR_ST_GEOG_PERIMETER` | metres |
| `CLR_ST_GEOG_AREA` | square metres |

An area shows the difference plainly: a one-degree box at the equator is about 12,309 square kilometres, and it is a little *larger* than the region between the parallels through its corners, because its northern edge is a geodesic that runs north of the parallel joining its two northern corners.

**Reading a geography.** Accessors, which read or rearrange coordinates without interpreting the space between them, so each is a delegation to the very JTS method Calcite's `ST_*` of that name calls.

| | |
| --- | --- |
| ordinates | `CLR_ST_GEOG_X`, `CLR_ST_GEOG_Y`, `CLR_ST_GEOG_Z` |
| bounds | `CLR_ST_GEOG_XMIN`, `CLR_ST_GEOG_XMAX`, `CLR_ST_GEOG_YMIN`, `CLR_ST_GEOG_YMAX`, `CLR_ST_GEOG_ZMIN`, `CLR_ST_GEOG_ZMAX` |
| shape | `CLR_ST_GEOG_DIMENSION`, `CLR_ST_GEOG_COORDDIM`, `CLR_ST_GEOG_GEOMETRYTYPE`, `CLR_ST_GEOG_GEOMETRYTYPECODE`, `CLR_ST_GEOG_ISEMPTY`, `CLR_ST_GEOG_IS3D`, `CLR_ST_GEOG_ISCLOSED`, `CLR_ST_GEOG_SRID` |
| counts | `CLR_ST_GEOG_NPOINTS`, `CLR_ST_GEOG_NUMPOINTS`, `CLR_ST_GEOG_NUMGEOMETRIES`, `CLR_ST_GEOG_NUMINTERIORRING`, `CLR_ST_GEOG_NUMINTERIORRINGS` |
| parts | `CLR_ST_GEOG_STARTPOINT`, `CLR_ST_GEOG_ENDPOINT`, `CLR_ST_GEOG_POINTN`, `CLR_ST_GEOG_GEOMETRYN`, `CLR_ST_GEOG_EXTERIORRING`, `CLR_ST_GEOG_INTERIORRING`, `CLR_ST_GEOG_BOUNDARY`, `CLR_ST_GEOG_HOLES` |
| comparison | `CLR_ST_GEOG_ORDERINGEQUALS` |

`CLR_ST_GEOG_XMIN` and its four relatives are computed structurally and are wrong in the usual way for anything crossing the antimeridian, where the least longitude of a shape spanning the seam is not its westmost point. That is inherited from the planar reading rather than introduced here.

**Building one from parts.**

| | |
| --- | --- |
| places | `CLR_ST_GEOG_POINT`, `CLR_ST_GEOG_MAKEPOINT` — two ordinates or three |
| lines | `CLR_ST_GEOG_MAKELINE` — two to six places |
| polygons | `CLR_ST_GEOG_MAKEPOLYGON` — a shell and up to ten holes |

**Editing.** Rearranging coordinates without interpreting the space between them.

| | |
| --- | --- |
| coordinates | `CLR_ST_GEOG_ADDPOINT`, `CLR_ST_GEOG_REMOVEPOINT`, `CLR_ST_GEOG_ADDZ`, `CLR_ST_GEOG_REMOVEREPEATEDPOINTS` |
| order and form | `CLR_ST_GEOG_REVERSE`, `CLR_ST_GEOG_NORMALIZE`, `CLR_ST_GEOG_FLIPCOORDINATES` |
| ordinates | `CLR_ST_GEOG_FORCE2D`, `CLR_ST_GEOG_FORCE3D` |
| structure | `CLR_ST_GEOG_REMOVEHOLES`, `CLR_ST_GEOG_TOMULTILINE`, `CLR_ST_GEOG_TOMULTIPOINT`, `CLR_ST_GEOG_TOMULTISEGMENTS` |

Every geography one of these hands back is stamped WGS84, which is a small divergence: `ST_FORCE2D` answers something with an SRID of zero, because the transformer underneath builds through a geometry factory that carries none across. Calcite has no reference system to keep there and this package does.

**Every format, both ways.** A reader and a writer for each, plus a typed reader for each shape — `CLR_ST_GEOG_POINTFROMTEXT`, `CLR_ST_GEOG_LINEFROMTEXT`, `CLR_ST_GEOG_POLYFROMTEXT`, their `MULTI` counterparts and the three `FROMWKB` forms — each answering `NULL` for text that names a different shape.

| | reads | writes |
| --- | --- | --- |
| WKT | `CLR_ST_GEOG_GEOMFROMTEXT`, `CLR_ST_GEOG_GEOMFROMWKT` | `CLR_ST_GEOG_ASTEXT`, `CLR_ST_GEOG_ASWKT` |
| EWKT | `CLR_ST_GEOG_GEOMFROMEWKT` | `CLR_ST_GEOG_ASEWKT` |
| WKB | `CLR_ST_GEOG_GEOMFROMWKB` | `CLR_ST_GEOG_ASBINARY`, `CLR_ST_GEOG_ASWKB` |
| EWKB | `CLR_ST_GEOG_GEOMFROMEWKB` | `CLR_ST_GEOG_ASEWKB` |
| GeoJSON | `CLR_ST_GEOG_GEOMFROMGEOJSON` | `CLR_ST_GEOG_ASGEOJSON` |
| GML | `CLR_ST_GEOG_GEOMFROMGML` | `CLR_ST_GEOG_ASGML` |

Each answers `NULL` for a `NULL` argument.

What these *mean* is what Calcite means, with the plane swapped for the sphere: `ST_Within` is `geom1.within(geom2)`, so `CLR_ST_GEOG_WITHIN` is the DE-9IM relation and not containment — a point on a polygon's boundary is not within it. `GeographyDifferentialTests` runs every one of them against Calcite's over shapes small enough that the two models must agree, which is what holds them to that.

## The engine

[Google's S2](https://github.com/google/s2-geometry-library-java), consumed as a Java library the same way `calcite-core` is. It models the Earth as a **sphere** of radius 6,371,010 m and joins two vertices with a great-circle arc.

One model rather than two, deliberately. An ellipsoidal distance from GeographicLib next to a spherical containment from S2 would put two readings of the same coordinates in one plan; the sphere costs a few tenths of a percent against an ellipsoidal distance, and it is what BigQuery and Snowflake compute in. The choice is reversible.

The difference this makes is not a scale factor. The northern edge of `POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))` runs between two points at ten degrees north: as a straight line in longitude and latitude it stays on that parallel, and as a great circle it reaches about 10° 2′ at the midpoint. A point between the two is inside one polygon and outside the other — not a different distance, a different answer.

At the antimeridian the two readings are exact inversions. `POLYGON((179 -1, -179 -1, -179 1, 179 1, 179 -1))` is a two-degree box straddling the seam on the sphere, and in the plane it is the 358-degree band that is everything except that box: every point is inside one and outside the other. The same seam turns a fifth of a degree into 359.8, and near a pole the shortest way between opposite meridians runs over the pole rather than 180 degrees around.

The pairwise operations are quadratic in the vertex counts. S2 has an indexed form of these queries and this does not use it yet.

The relations are this package's own rather than a library's. `S2BooleanOperation` would settle `CLR_ST_GEOG_WITHIN` by construction, and it cannot be had: the S2 published to Maven Central is the 2021 release, which does not have it, and the current source is compiled to Java 11, which IKVM does not read. What stands in for it is the size of the oracle — `GeographyDifferentialTests` over hand-written shapes, and `GeographyRandomDifferentialTests` over thirty thousand generated pairs a run, both answered by Calcite. Four defects in the relations were found by the generated half after the hand-written half was green.

## Simplifying a plan

`GeographyRules` is a pass a host sequences in front of whatever program it runs.

```csharp
using Apache.Calcite.Geography.Rel.Rules;

var config = Frameworks.newConfigBuilder()
    .defaultSchema(schema)
    .executor(RexUtil.EXECUTOR)
    .programs(Programs.sequence(GeographyRules.Program(), Programs.standard()))
    .build();
```

Every rewrite is an equality of *values* rather than of truth under a filter, so each holds wherever an
expression can stand — a projection, a filter, a join condition — and each is an identity the
implementations state rather than one this package decided.

| | |
| --- | --- |
| `CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(x))` → `x` | both crossings are `return geography;`, so a crossing whose operand already has the call's type costs a dispatch per row and nothing else |
| `CLR_ST_GEOG_ASWKT` → `CLR_ST_GEOG_ASTEXT` | and `ASWKB`→`ASBINARY`, `GEOMFROMWKT`→`GEOMFROMTEXT`, `NPOINTS`→`NUMPOINTS`, `NUMINTERIORRINGS`→`NUMINTERIORRING`, `MAKEPOINT`→`POINT`, `EXTENT`→`ENVELOPE`. Each pair is one function under two names — `ST_AsText` *is* `return ST_AsWKT(geom);` — and the canonical one is the OGC spelling |
| `NOT CLR_ST_GEOG_DISJOINT(a, b)` → `CLR_ST_GEOG_INTERSECTS(a, b)` | and the other way round: `Disjoint` is `Intersects` negated, and both answer null on a null argument, so the rewrite is exact under three-valued logic too |
| `CLR_ST_GEOG_CONTAINS(a, b)` → `CLR_ST_GEOG_WITHIN(b, a)` | and `COVEREDBY`→`COVERS`. `Contains(a, b)` *is* `Within(b, a)`, so canonical is the one the other delegates to |
| `CLR_ST_GEOG_DISTANCE(a, b) <= d` → `CLR_ST_GEOG_DWITHIN(a, b, d)` | `DWithin` *is* `Distance(a, b) <= d`. Not cheaper in process; the point is that a geodesic store has a within-distance predicate its index can answer and a scalar distance it cannot |

**`CLR_ST_GEOG_ASEWKB` is deliberately not in the alias list**, though today it answers the same bytes as
`ASBINARY`: Calcite's `ST_AsEWKB` is `return ST_AsWKB(geometry);` and writes no SRID, which is an oversight
rather than a declared synonym — `ST_AsEWKT` has a body of its own and does write one. An alias rule may
rest on two names meaning one thing and not on two things being equal by a defect.

**A pass and not rules on a `VolcanoPlanner`, and the difference is not a preference.** Measured: with these
registered on the planner, one of the five rewrites takes effect and four do not. `VolcanoCost.isLt`
compares the row count and nothing else, so a filter whose condition was simplified is never *cheaper* than
the same filter unsimplified and the planner keeps whichever it registered first — the original. The one
that does take effect wins for a reason unrelated to being better: `RelMdUtil.guessSelectivity` guesses 0.5
for a comparison and 0.25 for any other call, so the `DWITHIN` form carries a smaller row count. This is
the same argument that keeps `Programs.calc` a hep pass.

### Two things the pass does not do, and one a caller has to

**Constant folding is Calcite's and already works.** `CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')` in a
predicate reduces to a `GEOMETRY` literal and a wholly constant predicate reduces to nothing at all, under
`CoreRules.FILTER_REDUCE_EXPRESSIONS` — which `RelOptUtil.registerDefaultRules` already registers. It needs
an **executor**, and `ReduceExpressionsRule` gives up without one silently, saying in a comment that there
is no mechanism for a warning. A `jdbc:calcite:` connection always has one, because `CalcitePrepareImpl`
sets it; a `Frameworks` config has whatever it was given, which is nothing. **Without one the WKT is parsed
once per row** — measured. `.executor(RexUtil.EXECUTOR)` is the whole of the fix.

**Strictness and symmetry are declared on the operators**, not rewritten by the pass, and Calcite's own
machinery reads them with no rule involved. Every `CLR_ST_GEOG_` predicate and measurement is null exactly
when an argument is null, which is `Strong.Policy.ANY`; `RelOptUtil.simplifyJoin` reads it to turn a
`LEFT JOIN` whose `WHERE` holds one of these into an `INNER JOIN`, measured to remove both sorts and a
merge join from the plan. And `CLR_ST_GEOG_DISTANCE`, `MAXDISTANCE`, `INTERSECTS`, `DISJOINT`, `EQUALS` and
`ENVELOPESINTERSECT` are symmetrical, so `RexNormalize` gives `f(a, b)` and `f(b, a)` one digest and the
calc evaluates one of them.

**Most of the surface is not strict and does not say it is.** `Strong.Policy.ANY` is read in *both*
directions — `RexSimplify.simplifyIsNull` turns `f(a) IS NULL` into `a IS NULL` — and a reader answers null
over a non-null argument all the time: `CLR_ST_GEOG_POINTFROMTEXT` of text naming another shape,
`CLR_ST_GEOG_X` of anything but a point, `POINTN` past the end.

**These declarations live on the operator object, so only the chained route carries them.** A name resolved
through `GeographySchema` arrives as something `CalciteCatalogReader.toOp` built around the bare function.
The pass puts this package's operator back — matched by name and confirmed by the body being the same
object, so a host that declared a `CLR_ST_GEOG_` name over a body of its own is left alone — which is why
the join rewrite above happens on that route only with the pass in front.

## What is not here

- The rest of the mapping in [the design issue](https://github.com/ikvmnet/calcite-dotnet/issues/86) — the constructed-geometry group (buffer, the boolean overlay set, hulls, simplification, triangulation, grids), the point-returning measurements, `CLR_ST_GEOG_RELATE`, and the aggregates and table functions, which need machinery this package does not have.
- **`CLR_ST_GEOG_CROSSES`, `CLR_ST_GEOG_TOUCHES`, `CLR_ST_GEOG_OVERLAPS` and `CLR_ST_GEOG_CONTAINSPROPERLY`.** All four turn on whether the interiors of two geographies meet, and where a line comes back and touches itself that question has no answer without a node graph over both geometries: the place is the end of the whole line and the middle of one of its own edges at once, so it is boundary by the rule that counts ends and interior by the rule that reads the curve. Both rules were tried and each is wrong somewhere. That is the same machinery `S2BooleanOperation` would have brought.
- `ST_SETSRID` and `ST_TRANSFORM` have no counterpart, and will not: geography is WGS84 by definition and there is no second reference system to reproject into. Nor do the planar affine transforms — `ST_ROTATE`, `ST_SCALE`, `ST_TRANSLATE` — or precision reduction, which snaps to a planar grid.
- **Rechecking a pushed-down predicate.** These implementations must not be wired into a filter-recheck path until their agreement with each live store has been measured at the boundaries — a point on a polygon edge, an antimeridian crossing, the poles, a distance sitting on a threshold. A recheck that disagrees discards rows the store returned.
