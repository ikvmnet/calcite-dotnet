# Apache.Calcite.Geography

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Geography)](https://www.nuget.org/packages/Apache.Calcite.Geography)

**Apache.Calcite.Geography** gives [Apache Calcite](https://calcite.apache.org/) a `GEOGRAPHY` type and a set of `ST_GEOG_*` operators that read coordinates as WGS84 and answer in metres.

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

and the values in it are ordinary JTS `Geometry` objects — the same class Calcite's own spatial library carries.

**A geography column has to be nullable.** Saying `.nullable(false)` on the field — the ordinary thing an adapter does for a `NOT NULL` column — silently gives you an ordinary geometry, and Calcite's planar `ST_*` will then take it. `RelDataTypeFactoryImpl.copySimpleType` answers any change of a `JavaType`'s nullability with a plain `new JavaType(clazz, nullable)`, which is not this subclass; it is private, and a type factory of one's own cannot be put in front of Calcite, since `PlannerImpl` and `CalciteConnectionImpl` each build a `JavaTypeFactoryImpl` outright. Declaring the row not-nullable is a different call and is fine — only the field-level one degrades.

```sql
SELECT ID
FROM PLACES
WHERE ST_GEOG_DWITHIN(LOCATION, ST_GEOG_GEOMFROMTEXT('POINT(-0.1278 51.5074)'), 5000.0)
```

There is no other way in. A function declared through a schema cannot take a geography parameter at all — routine resolution runs an assignability check keyed on the parameter's `SqlTypeName` and throws `AssertionError: No assign rules for OTHER defined` — so something has to chain the table for the caller: the host, or a provider on their behalf.

## What the type is

A `RelDataTypeFactoryImpl.JavaType` over `org.locationtech.jts.geom.Geometry` that answers a different name.

| | |
| --- | --- |
| `getSqlTypeName()` | `OTHER` — the base would answer `GEOMETRY` |
| `getFullTypeString()` | `GEOGRAPHY` — a digest nothing else produces |
| `getJavaClass()` | `org.locationtech.jts.geom.Geometry` |

So the runtime carrier stays a plain JTS geometry — no new class, nothing new for code generation to name, and nothing to convert at a boundary — while the type system sees something else entirely.

**Calcite's planar functions refuse it at validation.** `ST_DISTANCE(LOCATION, LOCATION)` over a geography column fails to resolve. That is the property that matters most and the one no naming scheme provides: without it, a geodesic value would answer in degrees, in a different ordering, with no error anywhere. It is not special to `ST_DISTANCE` — every function in Calcite's spatial library is a reflective binding over `Geometry`, so all of them refuse it, the harmless accessors included.

The marking exists only in the type system, so a geography and a geometry are indistinguishable at run time. Anywhere the type is erased — a value on an `ANY` path, a third-party function declared over `Geometry` — the geodesic reading is silently lost. That is the same guarantee PostGIS gives, where both are the same bytes and only the declared type keeps them apart.

## What is here

The names mirror Calcite's `ST_*` one for one with an `ST_GEOG_` prefix. This is the first increment; Calcite's spatial library is about 130 names and every one of them needs a declaration, because Calcite's own reject the type.

**Constructors** — the only way a geography comes into existence in a query. The return type is `GEOGRAPHY`, and the result carries SRID 4326.

| | |
| --- | --- |
| `ST_GEOG_GEOMFROMTEXT(VARCHAR [, INTEGER])` | reads WKT |
| `ST_GEOG_GEOMFROMWKT(VARCHAR [, INTEGER])` | the same, under Calcite's other spelling |
| `ST_GEOG_GEOMFROMGEOJSON(VARCHAR)` | reads GeoJSON |

Both arities are Calcite's. The SRID a caller may name has to be 4326 and anything else is refused rather than ignored — a geography is WGS84 and there is no second reference system to reproject into, which is the same reason `ST_SETSRID` and `ST_TRANSFORM` have no counterpart at all.

**The crossing between the two readings.** Free at run time, since both sides are the same JTS object; explicit, because losing the geodesic reading should be something you wrote down.

| | |
| --- | --- |
| `ST_GEOG_ASGEOM(GEOGRAPHY)` | read a geography as a geometry |
| `ST_GEOM_ASGEOG(GEOMETRY)` | read a geometry as a geography |

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

**Measurements**, in metres and square metres rather than in degrees.

| | |
| --- | --- |
| `ST_GEOG_DISTANCE`, `ST_GEOG_DWITHIN` | the distance between two geographies |
| `ST_GEOG_MAXDISTANCE` | the greatest distance between a coordinate of one and a coordinate of the other |
| `ST_GEOG_LENGTH`, `ST_GEOG_PERIMETER` | metres |
| `ST_GEOG_AREA` | square metres |

An area shows the difference plainly: a one-degree box at the equator is about 12,364 square kilometres, and it is a little *larger* than the region between the parallels through its corners, because its northern edge is a great circle that runs north of the parallel joining its two northern corners.

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
