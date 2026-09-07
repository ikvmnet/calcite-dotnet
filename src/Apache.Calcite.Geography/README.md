# Apache.Calcite.Geography

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Geography)](https://www.nuget.org/packages/Apache.Calcite.Geography)

**Apache.Calcite.Geography** gives [Apache Calcite](https://calcite.apache.org/) a set of `ST_GEOG_*` operators that read coordinates as WGS84 and answer in metres.

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
WHERE ST_GEOG_DWITHIN(LOCATION, ST_GEOG_GEOMFROMTEXT('POINT(-0.1278 51.5074)'), 5000.0)
```

The operators can be reached two ways. A host driving its own planner chains `GeographyOperatorTable.Instance()`, as above. Anyone else registers them on a schema:

```csharp
using Apache.Calcite.Geography.Schema;

GeographySchema.AddTo(rootSchema);
```

which works through the stock `jdbc:calcite:` driver with nothing chained and nothing subclassed — an adapter can call it on the schema it builds, and its functions arrive with its tables. Registering on the root schema makes every operator visible unqualified everywhere on the connection, views in other schemas included. Do one or the other, not both: a name found twice resolves to whichever the lookup reaches first.

## There is no GEOGRAPHY type

A geography and a geometry are one type — Calcite's `GEOMETRY`, over `org.locationtech.jts.geom.Geometry`. What says a value is to be read geodesically is the name of the operator applied to it, and nothing else.

**Nothing refuses a mixture.** `ST_DISTANCE(LOCATION, LOCATION)` over geodesic coordinates answers in degrees, and `ST_GEOG_DISTANCE` over projected ones answers metres as though they were degrees. Both validate, both run. Worse than either, an expression can be half of each: `ST_GEOG_DISTANCE(ST_BUFFER(LOCATION, 0.1), OTHER)` buffers in degrees and then measures in metres.

There is no run-time guard underneath, either. The SRID is a tag on the JTS instance, and Calcite's own spatial functions mostly drop it off a geometry they derive — `ST_Buffer`, `ST_Centroid`, `ST_Envelope` and `ST_Intersection` all return zero from an operand stamped 4326 (`SridPropagationTests` pins this). So an `ST_GEOG_` operator cannot refuse a value for want of a stamp: it may be geodesic and merely have passed through one of Calcite's functions.

**This was a choice, and the alternative was measured.** A `JavaType` subclass answering `SqlTypeName.OTHER` gives the whole guarantee back — Calcite's planar functions then refuse a geography at validation. It cannot be registered on a schema: `CalciteCatalogReader.toOp` builds a fixed-parameter operand checker for every schema function, that checker's parameter types go through the assignment rules, and `SqlTypeAssignmentRule` has no entry for `OTHER` — so routine resolution throws `AssertionError: No assign rules for OTHER defined` rather than rejecting. Nor can the type be added to `SqlTypeName`, which is a closed Java enum and the key to that table and several others.

Since a schema is the only way an adapter can bring its functions with it, and bringing them is the point, the type gave way to the registration.

## What is here

The names mirror Calcite's `ST_*` one for one with an `ST_GEOG_` prefix. This is the first increment; Calcite's spatial library is about 130 names and every one of them needs a declaration, because Calcite's own reject the type.

**Constructors** — how a geography comes into existence in a query. The return type is `GEOMETRY`, and the result carries SRID 4326, though see above for how little that is worth once one of Calcite's own functions has touched it.

| | |
| --- | --- |
| `ST_GEOG_GEOMFROMTEXT(VARCHAR [, INTEGER])` | reads WKT |
| `ST_GEOG_GEOMFROMWKT(VARCHAR [, INTEGER])` | the same, under Calcite's other spelling |
| `ST_GEOG_GEOMFROMGEOJSON(VARCHAR)` | reads GeoJSON |

Both arities are Calcite's. The SRID a caller may name has to be 4326 and anything else is refused rather than ignored — a geography is WGS84 and there is no second reference system to reproject into, which is the same reason `ST_SETSRID` and `ST_TRANSFORM` have no counterpart at all.

**The crossing between the two readings.** Free at run time, since both sides are the same JTS object; explicit, because losing the geodesic reading should be something you wrote down.

| | |
| --- | --- |
| `ST_GEOG_ASGEOM(GEOMETRY)` | say the value is to be read as a plane from here on; converts nothing |
| `ST_GEOM_ASGEOG(GEOMETRY)` | say the value is to be read geodesically from here on; converts nothing |

**Relations.**

| | |
| --- | --- |
| `ST_GEOG_INTERSECTS` | any point in common |
| `ST_GEOG_DISJOINT` | none |
| `ST_GEOG_WITHIN`, `ST_GEOG_CONTAINS` | inside, with the interiors meeting |
| `ST_GEOG_COVEREDBY`, `ST_GEOG_COVERS` | inside, boundary allowed |
| `ST_GEOG_EQUALS` | the same set of places |
| `ST_GEOG_ENVELOPESINTERSECT` | bounding boxes meet |
| `ST_GEOG_ISVALID` | valid on the sphere |

`WITHIN` and `CONTAINS` are the DE-9IM relations JTS means by the words, not plain containment: a point on a polygon's boundary is covered by it and not within it.

**Measurements**, in metres and square metres rather than in degrees, on the WGS84 ellipsoid.

The two halves run on different engines, and deliberately. The predicates are S2's, which is a sphere; the measurements are [GeographicLib](https://geographiclib.sourceforge.io/)'s, which is the ellipsoid. S2 answers *which* points of two shapes are closest and GeographicLib answers *how far apart* they are. Measuring on the sphere was wrong by up to 0.56% against a live geodesic service — enough that an adapter could not recheck a predicate it had pushed down, since the recheck discards rows the store correctly returned. A degree of longitude at the equator is 111319.49 m and a degree of latitude is 110574.39 m; that these differ is the whole of what says the answers are ellipsoidal.

| | |
| --- | --- |
| `ST_GEOG_DISTANCE`, `ST_GEOG_DWITHIN` | the distance between two geographies |
| `ST_GEOG_MAXDISTANCE` | the greatest distance between a coordinate of one and a coordinate of the other |
| `ST_GEOG_CLOSESTCOORDINATE`, `ST_GEOG_FURTHESTCOORDINATE` | the coordinate of a geography nearest or furthest from a point |
| `ST_GEOG_CLOSESTPOINT` | the point of one geography nearest another, which may fall part way along an edge |
| `ST_GEOG_LONGESTLINE` | the line joining the pair `ST_GEOG_MAXDISTANCE` measures |
| `ST_GEOG_ENVELOPE`, `ST_GEOG_EXTENT` | the bounding rectangle, which wraps rather than spanning the globe when the shape crosses the antimeridian |
| `ST_GEOG_EXPAND` | that rectangle grown by a distance in metres |
| `ST_GEOG_DENSIFY` | vertices inserted along the geodesic so no edge exceeds a distance in metres |
| `ST_GEOG_PROJECTPOINT` | a point projected onto a line, landing on the geodesic |
| `ST_GEOG_INTERSECTION`, `ST_GEOG_DIFFERENCE`, `ST_GEOG_SYMDIFFERENCE`, `ST_GEOG_UNARYUNION` | the overlay set, over areas, bounded by geodesics |
| `ST_GEOG_CONVEXHULL` | convex on the sphere, so its edges bow poleward of a planar hull's |
| `ST_GEOG_SIMPLIFY` | vertices removed within a tolerance in metres |
| `ST_GEOG_CENTROID` | the centre as a direction from the Earth's centre, so it lands in the shape across the antimeridian |
| `ST_GEOG_LENGTH`, `ST_GEOG_PERIMETER` | metres |
| `ST_GEOG_AREA` | square metres |

An area shows the difference plainly: a one-degree box at the equator is about 12,309 square kilometres, and it is a little *larger* than the region between the parallels through its corners, because its northern edge is a geodesic that runs north of the parallel joining its two northern corners.

**Reading a geography.** Accessors, which read or rearrange coordinates without interpreting the space between them, so each is a delegation to the very JTS method Calcite's `ST_*` of that name calls.

| | |
| --- | --- |
| ordinates | `ST_GEOG_X`, `ST_GEOG_Y`, `ST_GEOG_Z` |
| bounds | `ST_GEOG_XMIN`, `ST_GEOG_XMAX`, `ST_GEOG_YMIN`, `ST_GEOG_YMAX`, `ST_GEOG_ZMIN`, `ST_GEOG_ZMAX` |
| shape | `ST_GEOG_DIMENSION`, `ST_GEOG_COORDDIM`, `ST_GEOG_GEOMETRYTYPE`, `ST_GEOG_GEOMETRYTYPECODE`, `ST_GEOG_ISEMPTY`, `ST_GEOG_IS3D`, `ST_GEOG_ISCLOSED`, `ST_GEOG_SRID` |
| counts | `ST_GEOG_NPOINTS`, `ST_GEOG_NUMPOINTS`, `ST_GEOG_NUMGEOMETRIES`, `ST_GEOG_NUMINTERIORRING`, `ST_GEOG_NUMINTERIORRINGS` |
| parts | `ST_GEOG_STARTPOINT`, `ST_GEOG_ENDPOINT`, `ST_GEOG_POINTN`, `ST_GEOG_GEOMETRYN`, `ST_GEOG_EXTERIORRING`, `ST_GEOG_INTERIORRING`, `ST_GEOG_BOUNDARY`, `ST_GEOG_HOLES` |
| comparison | `ST_GEOG_ORDERINGEQUALS` |

`ST_GEOG_XMIN` and its four relatives are computed structurally and are wrong in the usual way for anything crossing the antimeridian, where the least longitude of a shape spanning the seam is not its westmost point. That is inherited from the planar reading rather than introduced here.

**Building one from parts.**

| | |
| --- | --- |
| places | `ST_GEOG_POINT`, `ST_GEOG_MAKEPOINT` — two ordinates or three |
| lines | `ST_GEOG_MAKELINE` — two to six places |
| polygons | `ST_GEOG_MAKEPOLYGON` — a shell and up to ten holes |

**Editing.** Rearranging coordinates without interpreting the space between them.

| | |
| --- | --- |
| coordinates | `ST_GEOG_ADDPOINT`, `ST_GEOG_REMOVEPOINT`, `ST_GEOG_ADDZ`, `ST_GEOG_REMOVEREPEATEDPOINTS` |
| order and form | `ST_GEOG_REVERSE`, `ST_GEOG_NORMALIZE`, `ST_GEOG_FLIPCOORDINATES` |
| ordinates | `ST_GEOG_FORCE2D`, `ST_GEOG_FORCE3D` |
| structure | `ST_GEOG_REMOVEHOLES`, `ST_GEOG_TOMULTILINE`, `ST_GEOG_TOMULTIPOINT`, `ST_GEOG_TOMULTISEGMENTS` |

Every geography one of these hands back is stamped WGS84, which is a small divergence: `ST_FORCE2D` answers something with an SRID of zero, because the transformer underneath builds through a geometry factory that carries none across. Calcite has no reference system to keep there and this package does.

**Every format, both ways.** A reader and a writer for each, plus a typed reader for each shape — `ST_GEOG_POINTFROMTEXT`, `ST_GEOG_LINEFROMTEXT`, `ST_GEOG_POLYFROMTEXT`, their `MULTI` counterparts and the three `FROMWKB` forms — each answering `NULL` for text that names a different shape.

| | reads | writes |
| --- | --- | --- |
| WKT | `ST_GEOG_GEOMFROMTEXT`, `ST_GEOG_GEOMFROMWKT` | `ST_GEOG_ASTEXT`, `ST_GEOG_ASWKT` |
| EWKT | `ST_GEOG_GEOMFROMEWKT` | `ST_GEOG_ASEWKT` |
| WKB | `ST_GEOG_GEOMFROMWKB` | `ST_GEOG_ASBINARY`, `ST_GEOG_ASWKB` |
| EWKB | `ST_GEOG_GEOMFROMEWKB` | `ST_GEOG_ASEWKB` |
| GeoJSON | `ST_GEOG_GEOMFROMGEOJSON` | `ST_GEOG_ASGEOJSON` |
| GML | `ST_GEOG_GEOMFROMGML` | `ST_GEOG_ASGML` |

Each answers `NULL` for a `NULL` argument.

What these *mean* is what Calcite means, with the plane swapped for the sphere: `ST_Within` is `geom1.within(geom2)`, so `ST_GEOG_WITHIN` is the DE-9IM relation and not containment — a point on a polygon's boundary is not within it. `GeographyDifferentialTests` runs every one of them against Calcite's over shapes small enough that the two models must agree, which is what holds them to that.

## The engine

[Google's S2](https://github.com/google/s2-geometry-library-java), consumed as a Java library the same way `calcite-core` is. It models the Earth as a **sphere** of radius 6,371,010 m and joins two vertices with a great-circle arc.

One model rather than two, deliberately. An ellipsoidal distance from GeographicLib next to a spherical containment from S2 would put two readings of the same coordinates in one plan; the sphere costs a few tenths of a percent against an ellipsoidal distance, and it is what BigQuery and Snowflake compute in. The choice is reversible.

The difference this makes is not a scale factor. The northern edge of `POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))` runs between two points at ten degrees north: as a straight line in longitude and latitude it stays on that parallel, and as a great circle it reaches about 10° 2′ at the midpoint. A point between the two is inside one polygon and outside the other — not a different distance, a different answer.

At the antimeridian the two readings are exact inversions. `POLYGON((179 -1, -179 -1, -179 1, 179 1, 179 -1))` is a two-degree box straddling the seam on the sphere, and in the plane it is the 358-degree band that is everything except that box: every point is inside one and outside the other. The same seam turns a fifth of a degree into 359.8, and near a pole the shortest way between opposite meridians runs over the pole rather than 180 degrees around.

The pairwise operations are quadratic in the vertex counts. S2 has an indexed form of these queries and this does not use it yet.

The relations are this package's own rather than a library's. `S2BooleanOperation` would settle `ST_GEOG_WITHIN` by construction, and it cannot be had: the S2 published to Maven Central is the 2021 release, which does not have it, and the current source is compiled to Java 11, which IKVM does not read. What stands in for it is the size of the oracle — `GeographyDifferentialTests` over hand-written shapes, and `GeographyRandomDifferentialTests` over thirty thousand generated pairs a run, both answered by Calcite. Four defects in the relations were found by the generated half after the hand-written half was green.

## What is not here

- The rest of the mapping in [the design issue](https://github.com/ikvmnet/calcite-dotnet/issues/86) — the constructed-geometry group (buffer, the boolean overlay set, hulls, simplification, triangulation, grids), the point-returning measurements, `ST_GEOG_RELATE`, and the aggregates and table functions, which need machinery this package does not have.
- **`ST_GEOG_CROSSES`, `ST_GEOG_TOUCHES`, `ST_GEOG_OVERLAPS` and `ST_GEOG_CONTAINSPROPERLY`.** All four turn on whether the interiors of two geographies meet, and where a line comes back and touches itself that question has no answer without a node graph over both geometries: the place is the end of the whole line and the middle of one of its own edges at once, so it is boundary by the rule that counts ends and interior by the rule that reads the curve. Both rules were tried and each is wrong somewhere. That is the same machinery `S2BooleanOperation` would have brought.
- `ST_SETSRID` and `ST_TRANSFORM` have no counterpart, and will not: geography is WGS84 by definition and there is no second reference system to reproject into. Nor do the planar affine transforms — `ST_ROTATE`, `ST_SCALE`, `ST_TRANSLATE` — or precision reduction, which snaps to a planar grid.
- **Rechecking a pushed-down predicate.** These implementations must not be wired into a filter-recheck path until their agreement with each live store has been measured at the boundaries — a point on a polygon edge, an antimeridian crossing, the poles, a distance sitting on a threshold. A recheck that disagrees discards rows the store returned.
