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
| `ST_GEOG_GEOMFROMTEXT(VARCHAR)` | reads WKT |
| `ST_GEOG_GEOMFROMWKT(VARCHAR)` | the same, under Calcite's other spelling |
| `ST_GEOG_GEOMFROMGEOJSON(VARCHAR)` | reads GeoJSON |

**The crossing between the two readings.** Free at run time, since both sides are the same JTS object; explicit, because losing the geodesic reading should be something you wrote down.

| | |
| --- | --- |
| `ST_GEOG_ASGEOM(GEOGRAPHY)` | read a geography as a geometry |
| `ST_GEOM_ASGEOG(GEOMETRY)` | read a geometry as a geography |

**The operations backends actually push.**

| | |
| --- | --- |
| `ST_GEOG_DISTANCE(GEOGRAPHY, GEOGRAPHY)` | metres |
| `ST_GEOG_DWITHIN(GEOGRAPHY, GEOGRAPHY, NUMERIC)` | within a distance in metres |
| `ST_GEOG_WITHIN(GEOGRAPHY, GEOGRAPHY)` | containment |
| `ST_GEOG_INTERSECTS(GEOGRAPHY, GEOGRAPHY)` | any point in common |
| `ST_GEOG_ISVALID(GEOGRAPHY)` | valid on the sphere |

Each answers `NULL` for a `NULL` argument.

## The engine

[Google's S2](https://github.com/google/s2-geometry-library-java), consumed as a Java library the same way `calcite-core` is. It models the Earth as a **sphere** of radius 6,371,010 m and joins two vertices with a great-circle arc.

One model rather than two, deliberately. An ellipsoidal distance from GeographicLib next to a spherical containment from S2 would put two readings of the same coordinates in one plan; the sphere costs a few tenths of a percent against an ellipsoidal distance, and it is what BigQuery and Snowflake compute in. The choice is reversible.

The difference this makes is not a scale factor. The northern edge of `POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))` runs between two points at ten degrees north: as a straight line in longitude and latitude it stays on that parallel, and as a great circle it reaches about 10° 2′ at the midpoint. A point between the two is inside one polygon and outside the other — not a different distance, a different answer.

The pairwise operations are quadratic in the vertex counts. S2 has an indexed form of these queries and this does not use it yet.

## What is not here

- The remaining ~120 declarations. The full mapping is in [the design issue](https://github.com/ikvmnet/calcite-dotnet/issues/86).
- `ST_SETSRID` and `ST_TRANSFORM` have no counterpart, and will not: geography is WGS84 by definition and there is no second reference system to reproject into. Nor do the planar affine transforms — `ST_ROTATE`, `ST_SCALE`, `ST_TRANSLATE` — or precision reduction, which snaps to a planar grid.
- **Rechecking a pushed-down predicate.** These implementations must not be wired into a filter-recheck path until their agreement with each live store has been measured at the boundaries — a point on a polygon edge, an antimeridian crossing, the poles, a distance sitting on a threshold. A recheck that disagrees discards rows the store returned.
