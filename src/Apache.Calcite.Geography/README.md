# Apache.Calcite.Geography

[![NuGet](https://img.shields.io/nuget/v/Apache.Calcite.Geography)](https://www.nuget.org/packages/Apache.Calcite.Geography)

**Apache.Calcite.Geography** adds `CLR_ST_GEOG_*` functions to [Apache Calcite](https://calcite.apache.org/)
that read coordinates as WGS84 longitude and latitude and answer in metres.

Calcite's own `ST_*` functions are planar [JTS](https://github.com/locationtech/jts): they treat longitude and
latitude as x and y on a flat plane and answer in degrees. That is not a scale factor away from the geodesic
answer — how far a degree reaches depends on latitude and direction, so planar distances do not even rank
places in the same order, and a shape crossing the antimeridian means something entirely different on the plane.
If your data is WGS84 and you want the answers PostGIS `geography`, BigQuery, Snowflake or a 2dsphere index
would give, use these functions.

The package is optional; nothing else in this repository depends on it. It targets .NET 8.

## Install

```sh
dotnet add package Apache.Calcite.Geography
```

## Registering the functions

There are two ways to make the functions visible to SQL. Use one, not both: a name found twice resolves to
whichever the lookup reaches first.

**Chain the operator table**, if you configure the validator yourself (for example through `Frameworks`):

```csharp
using Apache.Calcite.Geography.Sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.sql.util;
using org.apache.calcite.tools;

var operatorTable = SqlOperatorTables.chain(
    SqlStdOperatorTable.instance(),
    GeographyOperatorTable.Instance());

var config = Frameworks.newConfigBuilder()
    .defaultSchema(schema)
    .operatorTable(operatorTable)
    .build();
```

**Or declare them on a schema**, which needs no validator configuration and so works through Calcite's
`jdbc:calcite:` driver, or from an adapter that wants its functions to arrive with its tables:

```csharp
using Apache.Calcite.Geography.Schema;

GeographySchema.AddTo(rootSchema);
```

Functions added to the root schema are visible unqualified everywhere on the connection, including in views
defined in other schemas.

## Declaring a geography column

There is no `GEOGRAPHY` SQL type. A geography is Calcite's `GEOMETRY`, and its values are ordinary JTS
`org.locationtech.jts.geom.Geometry` objects. Declare a column with `GeographyTypes.Of`:

```csharp
using Apache.Calcite.Geography.Rel.Type;

typeFactory.builder()
    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
    .add("LOCATION", GeographyTypes.Of(typeFactory))
    .build();
```

Coordinates are `(x, y)` = `(longitude, latitude)` in degrees, the order WKT and GeoJSON use:

```sql
SELECT ID
FROM PLACES
WHERE CLR_ST_GEOG_DWITHIN(LOCATION, CLR_ST_GEOG_GEOMFROMTEXT('POINT(-0.1278 51.5074)'), 5000)
```

Because a geography and a geometry share one type, **nothing stops you mixing the two readings**. The name of
the function is the only thing that says a value is read geodesically. `ST_DISTANCE(LOCATION, OTHER)` validates
and answers in degrees, and `CLR_ST_GEOG_DISTANCE(ST_BUFFER(LOCATION, 0.1), OTHER)` buffers by 0.1 degrees
and then measures in metres. The SRID cannot be relied on either: the constructors here stamp 4326, but Calcite's
own spatial functions generally return results with an SRID of 0. `CLR_ST_GEOG_ASGEOM` and `CLR_ST_GEOM_ASGEOG`
convert nothing; use them to make the point where a query changes reading explicit.

## Conventions

- Every function returns `NULL` when any argument is `NULL`.
- An argument shown as `DOUBLE` accepts any numeric type, so `5000` and `5000.0` both work without a `CAST`.
- Distances are in metres and areas in square metres, measured on the WGS84 ellipsoid.
- Functions that take an SRID accept only 4326 and throw for anything else, since no reprojection takes place.
  EWKT and EWKB input naming an SRID other than 0 or 4326 is refused the same way.
- Where a function mirrors a Calcite `ST_*` function, it means the same thing with the plane replaced by the
  Earth. `CLR_ST_GEOG_WITHIN` is JTS `within`, the DE-9IM relation, so a point on a polygon's boundary is covered
  by the polygon but not within it.

## Functions

### Reading and writing

| format | read | write |
| --- | --- | --- |
| WKT | `CLR_ST_GEOG_GEOMFROMTEXT(VARCHAR [, INTEGER srid])`, alias `CLR_ST_GEOG_GEOMFROMWKT` | `CLR_ST_GEOG_ASTEXT`, alias `CLR_ST_GEOG_ASWKT` |
| EWKT | `CLR_ST_GEOG_GEOMFROMEWKT(VARCHAR)` | `CLR_ST_GEOG_ASEWKT` |
| WKB | `CLR_ST_GEOG_GEOMFROMWKB(VARBINARY [, INTEGER srid])` | `CLR_ST_GEOG_ASBINARY`, alias `CLR_ST_GEOG_ASWKB` |
| EWKB | `CLR_ST_GEOG_GEOMFROMEWKB(VARBINARY)` | `CLR_ST_GEOG_ASEWKB` (the same bytes as WKB, as Calcite's `ST_ASEWKB` is) |
| GeoJSON | `CLR_ST_GEOG_GEOMFROMGEOJSON(VARCHAR)` | `CLR_ST_GEOG_ASGEOJSON` |
| GML | `CLR_ST_GEOG_GEOMFROMGML(VARCHAR [, INTEGER srid])` | `CLR_ST_GEOG_ASGML` |

Typed readers return `NULL` when the input describes a different kind of shape: `CLR_ST_GEOG_POINTFROMTEXT`,
`CLR_ST_GEOG_LINEFROMTEXT`, `CLR_ST_GEOG_POLYFROMTEXT`, `CLR_ST_GEOG_MPOINTFROMTEXT`,
`CLR_ST_GEOG_MLINEFROMTEXT`, `CLR_ST_GEOG_MPOLYFROMTEXT`, `CLR_ST_GEOG_POINTFROMWKB`, `CLR_ST_GEOG_LINEFROMWKB`
and `CLR_ST_GEOG_POLYFROMWKB`, each with an optional SRID.

### Building

| function | returns |
| --- | --- |
| `CLR_ST_GEOG_POINT(DOUBLE lng, DOUBLE lat [, DOUBLE z])`, alias `CLR_ST_GEOG_MAKEPOINT` | a point |
| `CLR_ST_GEOG_MAKELINE(GEOMETRY, …)` | the line through two to six points |
| `CLR_ST_GEOG_MAKEPOLYGON(GEOMETRY shell, …)` | a polygon from a closed line and up to ten holes |
| `CLR_ST_GEOG_MAKEELLIPSE(GEOMETRY point, DOUBLE width, DOUBLE height)` | a 32-sided ellipse whose width and height are metres, so equal ones are round on the ground |
| `CLR_ST_GEOG_ASGEOM(GEOMETRY)`, `CLR_ST_GEOM_ASGEOG(GEOMETRY)` | the argument unchanged; see above |

### Relations

All return `BOOLEAN`. `INTERSECTS`, `WITHIN`, `CONTAINS`, `COVERS`, `COVEREDBY`, `EQUALS` and
`ENVELOPESINTERSECT` are `FALSE` when either argument is empty, and `DISJOINT` is then `TRUE`; `DWITHIN` is
`TRUE` for any non-negative distance, because the distance to an empty geography is 0.

| function | true when |
| --- | --- |
| `CLR_ST_GEOG_INTERSECTS(a, b)` | the two have any point in common |
| `CLR_ST_GEOG_DISJOINT(a, b)` | they have none |
| `CLR_ST_GEOG_WITHIN(a, b)`, `CLR_ST_GEOG_CONTAINS(b, a)` | every point of `a` is in `b` and their interiors meet |
| `CLR_ST_GEOG_COVEREDBY(a, b)`, `CLR_ST_GEOG_COVERS(b, a)` | every point of `a` is in `b`, boundary included |
| `CLR_ST_GEOG_EQUALS(a, b)` | they are the same set of places (a line equals its reverse) |
| `CLR_ST_GEOG_ORDERINGEQUALS(a, b)` | they have the same coordinates in the same order |
| `CLR_ST_GEOG_DWITHIN(a, b, DOUBLE d)` | `CLR_ST_GEOG_DISTANCE(a, b) <= d` |
| `CLR_ST_GEOG_ENVELOPESINTERSECT(a, b)` | their bounding rectangles (as `CLR_ST_GEOG_ENVELOPE`) meet |
| `CLR_ST_GEOG_ISVALID(g)` | every coordinate is a valid latitude and longitude and every ring is a valid loop on the sphere; this is not JTS's planar validity |
| `CLR_ST_GEOG_ISSIMPLE(g)`, `CLR_ST_GEOG_ISRING(g)` | JTS's rules, applied to geodesic edges |

### Measurements

| function | returns |
| --- | --- |
| `CLR_ST_GEOG_DISTANCE(a, b)` | the shortest distance in metres; 0 if they intersect, one encloses the other, or either is empty |
| `CLR_ST_GEOG_MAXDISTANCE(a, b)` | the greatest distance between a coordinate of one and a coordinate of the other |
| `CLR_ST_GEOG_LENGTH(g)` | the length of every edge, polygon rings included |
| `CLR_ST_GEOG_PERIMETER(g)` | the length of the polygon rings only |
| `CLR_ST_GEOG_AREA(g)` | the area in square metres; 0 for points and lines |

### Deriving shapes

Every result is stamped with SRID 4326.

| function | returns |
| --- | --- |
| `CLR_ST_GEOG_ENVELOPE(g)`, alias `CLR_ST_GEOG_EXTENT` | the smallest latitude-longitude rectangle; a shape crossing the antimeridian gets a narrow rectangle split into two halves, not one spanning the globe |
| `CLR_ST_GEOG_EXPAND(g, DOUBLE d)` | that rectangle grown by `d` metres |
| `CLR_ST_GEOG_BUFFER(g, DOUBLE d)` | an approximation of the region within `d` metres (see below) |
| `CLR_ST_GEOG_CENTROID(g)` | the centre, computed on the sphere; an empty point where there is none |
| `CLR_ST_GEOG_CONVEXHULL(g)` | the smallest region convex on the sphere, whose edges bow poleward of a planar hull's |
| `CLR_ST_GEOG_BOUNDINGCIRCLE(g)` | a 32-sided polygon around a circle of constant ground distance that contains the shape; close to, and never smaller than, the smallest such circle |
| `CLR_ST_GEOG_MINIMUMDIAMETER(g)` | the shortest line across the shape's width, measured between great circles |
| `CLR_ST_GEOG_INTERSECTION(a, b)`, `CLR_ST_GEOG_DIFFERENCE(a, b)`, `CLR_ST_GEOG_SYMDIFFERENCE(a, b)`, `CLR_ST_GEOG_UNARYUNION(g)` | the overlay of the polygons, bounded by geodesics; `NULL` if an argument has no polygon, and points and lines are ignored |
| `CLR_ST_GEOG_SIMPLIFY(g, DOUBLE d)` | the polygons with vertices removed that move the boundary by at most `d` metres; `NULL` if there is no polygon |
| `CLR_ST_GEOG_DENSIFY(g, DOUBLE d)` | vertices inserted along each geodesic edge so none is longer than `d` metres |
| `CLR_ST_GEOG_OFFSETCURVE(line, DOUBLE d)` | the line moved `d` metres to its left (negative for right); `NULL` for anything but a line |
| `CLR_ST_GEOG_LOCATEALONG(g, DOUBLE fraction, DOUBLE offset)` | a multi-point with one point a fraction along each segment, offset sideways in metres |
| `CLR_ST_GEOG_PROJECTPOINT(point, line)` | the point on the line nearest the given point; `NULL` if `line` is a polygon |
| `CLR_ST_GEOG_CLOSESTPOINT(a, b)` | the point of `a` nearest `b`, which may lie part way along an edge |
| `CLR_ST_GEOG_CLOSESTCOORDINATE(point, g)`, `CLR_ST_GEOG_FURTHESTCOORDINATE(point, g)` | the coordinate(s) of `g` nearest or furthest from the point, as a multi-point if tied |
| `CLR_ST_GEOG_LONGESTLINE(a, b)` | the line joining the two coordinates `CLR_ST_GEOG_MAXDISTANCE` measures |

`CLR_ST_GEOG_BUFFER` is built rather than exact: it unions 32-sided rings drawn at exactly `d` metres around
every vertex and around points spaced along each edge. The rings are inscribed, so the result lies slightly inside
the true buffer, and for a shape that is long compared with `d` the spacing is widened to bound the work, which
reduces accuracy.

### Accessors and editing

These read or rearrange coordinates without interpreting the space between them, so each calls the Calcite
`ST_*` function of the same name. Those in the editing group stamp their result with SRID 4326.

| | |
| --- | --- |
| ordinates | `CLR_ST_GEOG_X` (longitude), `CLR_ST_GEOG_Y` (latitude), `CLR_ST_GEOG_Z` — `NULL` for anything but a point |
| bounds | `CLR_ST_GEOG_XMIN`, `CLR_ST_GEOG_XMAX`, `CLR_ST_GEOG_YMIN`, `CLR_ST_GEOG_YMAX`, `CLR_ST_GEOG_ZMIN`, `CLR_ST_GEOG_ZMAX` |
| shape | `CLR_ST_GEOG_DIMENSION`, `CLR_ST_GEOG_COORDDIM`, `CLR_ST_GEOG_GEOMETRYTYPE`, `CLR_ST_GEOG_GEOMETRYTYPECODE`, `CLR_ST_GEOG_ISEMPTY`, `CLR_ST_GEOG_IS3D`, `CLR_ST_GEOG_ISCLOSED`, `CLR_ST_GEOG_SRID` |
| counts | `CLR_ST_GEOG_NUMPOINTS` (alias `CLR_ST_GEOG_NPOINTS`), `CLR_ST_GEOG_NUMGEOMETRIES`, `CLR_ST_GEOG_NUMINTERIORRING` (alias `CLR_ST_GEOG_NUMINTERIORRINGS`) |
| parts | `CLR_ST_GEOG_STARTPOINT`, `CLR_ST_GEOG_ENDPOINT`, `CLR_ST_GEOG_POINTN`, `CLR_ST_GEOG_GEOMETRYN`, `CLR_ST_GEOG_EXTERIORRING`, `CLR_ST_GEOG_INTERIORRING`, `CLR_ST_GEOG_BOUNDARY`, `CLR_ST_GEOG_HOLES` |
| editing | `CLR_ST_GEOG_ADDPOINT`, `CLR_ST_GEOG_REMOVEPOINT`, `CLR_ST_GEOG_ADDZ`, `CLR_ST_GEOG_REMOVEREPEATEDPOINTS`, `CLR_ST_GEOG_REVERSE`, `CLR_ST_GEOG_NORMALIZE`, `CLR_ST_GEOG_FLIPCOORDINATES`, `CLR_ST_GEOG_FORCE2D`, `CLR_ST_GEOG_FORCE3D`, `CLR_ST_GEOG_REMOVEHOLES`, `CLR_ST_GEOG_TOMULTILINE`, `CLR_ST_GEOG_TOMULTIPOINT`, `CLR_ST_GEOG_TOMULTISEGMENTS` |

The bounds functions compare coordinates as numbers, so for a shape crossing the antimeridian `XMIN` is not its
western edge; use `CLR_ST_GEOG_ENVELOPE` for that. The tolerance of `CLR_ST_GEOG_REMOVEREPEATEDPOINTS` is in
degrees, not metres, because it is passed to Calcite unchanged. Typed accessors return `NULL` where Calcite's do:
`POINTN` and `INTERIORRING` out of range, `STARTPOINT` of a polygon.

## What is not provided

- `CLR_ST_GEOG_CROSSES`, `TOUCHES`, `OVERLAPS` and `CONTAINSPROPERLY`. Each depends on classifying where a line
  touches itself as boundary or interior, which needs a node graph over both shapes that is not available.
- `CLR_ST_GEOG_RELATE`, a two-argument union, and the spatial aggregates and table functions.
- `CLR_ST_GEOG_SIMPLIFYPRESERVETOPOLOGY`, since the simplification used does not guarantee what the JTS one does.
- `ST_SETSRID` and `ST_TRANSFORM` equivalents: a geography is always WGS84. Nor the planar affine transforms
  (`ST_ROTATE`, `ST_SCALE`, `ST_TRANSLATE`) or precision reduction.
- The overlay functions and `CLR_ST_GEOG_SIMPLIFY` work on polygons only.
- `CLR_ST_GEOG_OFFSETCURVE` takes two arguments; Calcite's third, a JTS buffer style, has no meaning here.

Any `ST_*` function without a `CLR_ST_GEOG_` counterpart listed above is not provided.

## How answers are computed

Topology — whether shapes meet, which contains which, and which pair of points is closest — is decided by
[Google's S2](https://github.com/google/s2-geometry-library-java), which models the Earth as a sphere and joins
vertices with great-circle arcs. Lengths, distances and areas are then measured on the WGS84 ellipsoid with
[GeographicLib](https://geographiclib.sourceforge.io/). A spherical distance can differ from the ellipsoidal one
by about half a percent; at the equator a degree of longitude is 111,319.49 m and a degree of latitude
110,574.39 m.

Edges are geodesics, not straight lines in degrees. The northern edge of
`POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))` bows north of the 10° parallel, so a point just north of that parallel
can be inside the geography and outside the planar polygon. `POLYGON((179 -1, -179 -1, -179 1, 179 1, 179 -1))`
is a two-degree box across the antimeridian here and a 358-degree band on the plane.

The pairwise operations compare every edge of one shape with every edge of the other, so their cost grows with
the product of the vertex counts.

Results on a boundary — a point exactly on an edge, a distance exactly equal to a threshold — can differ from a
particular store's. An adapter that pushes a predicate down should not re-evaluate it in process with these
functions without first checking that they agree with that store at such cases, since a disagreeing recheck
discards rows the store correctly returned.

## Simplifying plans

`GeographyRules.Program()` is a rewrite pass to run ahead of your own program:

```csharp
using Apache.Calcite.Geography.Rel.Rules;
using org.apache.calcite.rex;
using org.apache.calcite.tools;

var config = Frameworks.newConfigBuilder()
    .defaultSchema(schema)
    .executor(RexUtil.EXECUTOR)
    .programs(Programs.sequence(GeographyRules.Program(), Programs.standard()))
    .build();
```

Each rewrite replaces an expression with one of equal value, including under three-valued logic, so it applies in
projections, filters and join conditions alike:

| before | after |
| --- | --- |
| `CLR_ST_GEOG_ASGEOM(x)` or `CLR_ST_GEOM_ASGEOG(x)` where `x` already has the call's type | `x` |
| an alias: `ASWKT`, `ASWKB`, `GEOMFROMWKT`, `NPOINTS`, `NUMINTERIORRINGS`, `MAKEPOINT`, `EXTENT` | `ASTEXT`, `ASBINARY`, `GEOMFROMTEXT`, `NUMPOINTS`, `NUMINTERIORRING`, `POINT`, `ENVELOPE` |
| `CLR_ST_GEOG_CONTAINS(a, b)`, `CLR_ST_GEOG_COVEREDBY(a, b)` | `CLR_ST_GEOG_WITHIN(b, a)`, `CLR_ST_GEOG_COVERS(b, a)` |
| `NOT CLR_ST_GEOG_DISJOINT(a, b)`, `NOT CLR_ST_GEOG_INTERSECTS(a, b)` | `CLR_ST_GEOG_INTERSECTS(a, b)`, `CLR_ST_GEOG_DISJOINT(a, b)` |
| `CLR_ST_GEOG_DISTANCE(a, b) <= d`, `d >= CLR_ST_GEOG_DISTANCE(a, b)` | `CLR_ST_GEOG_DWITHIN(a, b, d)` |

The last is not cheaper to evaluate in process; it gives an adapter a within-distance predicate a geodesic index
can answer. `GeographyRules.Simplify` applies the same rewrites to a single expression, for an adapter that wants
the canonical form before matching function names.

Run it as a separate pass, not as rules on a `VolcanoPlanner`: that planner compares row counts only, so it
generally keeps the unrewritten expression.

**Give the planner an executor.** Constant folding — reducing `CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')` to a
literal — is Calcite's own `ReduceExpressionsRule`, which silently does nothing without an executor. A
`jdbc:calcite:` connection has one; a `Frameworks` configuration has one only if you set it as above. Without it
a WKT literal is parsed once per row.

**Null handling and symmetry are declared on the operators.** The measurements, the relations and the
`IS*`/count functions are declared to return `NULL` exactly when an argument is `NULL`, which lets Calcite's own
simplifications, for example, turn a `LEFT JOIN` filtered on one of them into an `INNER JOIN`. `DISTANCE`,
`MAXDISTANCE`, `INTERSECTS`, `DISJOINT`, `EQUALS` and `ENVELOPESINTERSECT` are declared symmetrical, so
`f(a, b)` and `f(b, a)` are recognised as one expression. These declarations are on the operator objects of
`GeographyOperatorTable`. A function resolved through a schema reaches the plan as an operator Calcite builds
itself, without them; the rewrite pass puts this package's operators back, so on the schema route run the pass
to get them.
