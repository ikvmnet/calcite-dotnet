using System;

using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Runtime
{

    /// <summary>
    /// A geography: coordinates read as WGS84 latitude and longitude rather than as a plane.
    /// </summary>
    /// <remarks>
    /// The marking lives here, in the value, rather than in the type system, and that is the whole design.
    /// <c>SqlTypeName</c> is a closed enum, so a type of this package's own has to impersonate one of
    /// Calcite's; and every table that would make such a type behave — the assign rules, the java class
    /// mapping, the nullability copy — is keyed by that name. A type distinct from <c>GEOMETRY</c> is
    /// therefore not expressible. A distinct <em>class</em> is, and the type system carries it faithfully:
    /// <c>createJavaType(Geography.class)</c> is an ordinary <c>RelDataTypeFactoryImpl.JavaType</c>, and
    /// <c>copySimpleType</c> answers a change of nullability on one with
    /// <c>new JavaType(Primitive.box(clazz), nullable)</c> — where <c>Primitive.box</c> returns a
    /// non-primitive class unchanged. So the type survives every nullability change on a stock
    /// <c>JavaTypeFactoryImpl</c>, with no factory of ours in front of Calcite.
    ///
    /// <para>The consequence worth having is that the marking cannot be erased. A geography on an
    /// <c>ANY</c> path, or through a third-party function declared over <c>Object</c>, is still a
    /// <see cref="Geography"/> when it arrives. A JTS geometry marked only by its type would have been
    /// indistinguishable from a planar one at that point, which is the failure PostGIS answers with a
    /// second type and Calcite gives no way to declare.</para>
    ///
    /// <para>The SRID is carried rather than read off <see cref="Geometry"/> because JTS propagates its own
    /// inconsistently — an operation that builds a new geometry takes the SRID of the factory that built it,
    /// not of the operand. Holding it here means the value answers for itself.</para>
    /// </remarks>
    public sealed class Geography
    {

        /// <summary>
        /// The reference system a geography is in, always.
        /// </summary>
        /// <remarks>
        /// There is no second one to reproject into, which is why <c>ST_SETSRID</c> and <c>ST_TRANSFORM</c>
        /// have no <c>ST_GEOG_</c> counterpart.
        /// </remarks>
        public const int Wgs84 = 4326;

        /// <summary>
        /// Returns the given geometry read as a geography, or <see langword="null"/> for a null geometry.
        /// </summary>
        /// <param name="geometry"></param>
        /// <returns></returns>
        /// <remarks>
        /// The geometry is stamped with <see cref="Wgs84"/> as well as wrapped, so that a caller who unwraps
        /// one — <c>ST_GEOG_ASGEOM</c>, or a reader taking the value out — gets a geometry that says what
        /// reference system its coordinates are in.
        /// </remarks>
        public static Geography? Of(Geometry? geometry)
        {
            if (geometry is null)
                return null;

            geometry.setSRID(Wgs84);
            return new Geography(geometry, Wgs84);
        }

        readonly Geometry _geometry;
        readonly int _srid;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="geometry"></param>
        /// <param name="srid"></param>
        public Geography(Geometry geometry, int srid)
        {
            _geometry = geometry ?? throw new ArgumentNullException(nameof(geometry));
            _srid = srid;
        }

        /// <summary>
        /// Gets the coordinates.
        /// </summary>
        public Geometry Geometry => _geometry;

        /// <summary>
        /// Gets the reference system the coordinates are in.
        /// </summary>
        public int Srid => _srid;

        /// <inheritdoc />
        /// <remarks>
        /// Calcite compares values with <c>equals</c> and groups them by <c>hashCode</c> — a hash join, a
        /// <c>DISTINCT</c>, a <c>GROUP BY</c> over a geography column all reach these — so both delegate to
        /// the geometry. JTS answers <c>equals</c> structurally, which is the comparison a geography wants.
        /// </remarks>
        public override bool Equals(object? obj)
        {
            return obj is Geography other && _srid == other._srid && _geometry.equals(other._geometry);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return HashCode.Combine(_geometry.hashCode(), _srid);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return _geometry.toString();
        }

    }

}
