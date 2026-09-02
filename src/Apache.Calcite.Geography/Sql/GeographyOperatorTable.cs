using System;

using Apache.Calcite.Geography.Runtime;
using Apache.Calcite.Geography.Sql.Type;

using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Geography.Sql
{

    /// <summary>
    /// The <c>ST_GEOG_*</c> operators.
    /// </summary>
    /// <remarks>
    /// Chained by a host onto whatever it already has:
    ///
    /// <code>
    /// SqlOperatorTables.chain(SqlStdOperatorTable.instance(), GeographyOperatorTable.Instance())
    /// </code>
    ///
    /// <para>There is no other way in. A function declared through a schema — <c>SchemaPlus.add(name,
    /// Function)</c> — cannot take a geography parameter at all; see
    /// <see cref="GeographyOperandTypeChecker"/> for the assertion it dies on. So a caller reaching Calcite
    /// through a plain connection and a model file cannot resolve these names by themselves: something has to
    /// chain this table for them.</para>
    ///
    /// <para>Each operator is a <c>SqlUserDefinedFunction</c> over a <c>ScalarFunctionImpl</c>, which is what
    /// gives the call an implementor: <c>RexImpTable.get</c> answers a user-defined function by asking its
    /// <c>Function</c> for one, and its own map is private with no <c>RexImplementorTable</c> to add to until
    /// 1.43. The body is a .NET method and no class name is written out — this convention's translator holds
    /// the method itself, and Calcite's own engine reaches a CLR class through the class-loader stamp
    /// <c>IKVM.Maven.Sdk</c> puts on <c>calcite-core</c>.</para>
    ///
    /// <para>Each operator is also a public field, which <c>SqlSpatialTypeOperatorTable</c> is not, and the
    /// reason is that Calcite has a handle we cannot have. Its own spatial pushdown rules recognise a call by
    /// <c>SqlKind</c> — <c>SpatialRules</c> matches <c>SqlKind.ST_DWITHIN</c> and <c>SqlKind.ST_CONTAINS</c>,
    /// which reach the operator from a <c>@Hints({"SqlKind:ST_DWITHIN"})</c> annotation on the method that
    /// <c>CalciteCatalogReader.toOp</c> reads. <c>SqlKind</c> is a closed enum, so <c>ST_GEOG_DWITHIN</c>
    /// cannot be added to it, and taking <c>ST_DWITHIN</c> would be worse than having nothing: Calcite's own
    /// rules would then match a geodesic call and plan it as a planar one. So an adapter that wants to push
    /// one of these down has the operator itself or its name, and the field is the exact one.</para>
    ///
    /// <para>This is the first increment of the surface. Calcite's spatial library is about 130 names and
    /// every one of them needs an <c>ST_GEOG_</c> declaration, because Calcite's own reject the type. What is
    /// here is the type's constructors, the two crossings, and the five operations a geodesic store actually
    /// pushes.</para>
    /// </remarks>
    public sealed class GeographyOperatorTable : SqlOperatorTable
    {

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMGEOJSON(VARCHAR)</c>. Reads a geography from GeoJSON.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromGeoJson =
            Function("ST_GEOG_GEOMFROMGEOJSON", nameof(GeographyFunctions.FromGeoJson), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["geoJson"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMTEXT(VARCHAR)</c>. Reads a geography from WKT.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromText =
            Function("ST_GEOG_GEOMFROMTEXT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKT(VARCHAR)</c>. Reads a geography from WKT; Calcite's alias for the same
        /// thing, mirrored.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromWkt =
            Function("ST_GEOG_GEOMFROMWKT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character], ["wkt"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMTEXT(VARCHAR, INTEGER)</c>. Reads a geography from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromTextWithSrid =
            Function("ST_GEOG_GEOMFROMTEXT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_GEOMFROMWKT(VARCHAR, INTEGER)</c>. Reads a geography from WKT; the SRID must be 4326.
        /// </summary>
        public static readonly SqlFunction StGeogGeomFromWktWithSrid =
            Function("ST_GEOG_GEOMFROMWKT", nameof(GeographyFunctions.FromWkt), GeographyReturnTypes.Geography,
                [GeographyOperand.Character, GeographyOperand.Numeric], ["wkt", "srid"]);

        /// <summary>
        /// <c>ST_GEOG_ASGEOM(GEOGRAPHY)</c>. Reads a geography as a geometry.
        /// </summary>
        public static readonly SqlFunction StGeogAsGeom =
            Function("ST_GEOG_ASGEOM", nameof(GeographyFunctions.AsGeometry), GeographyReturnTypes.Geometry,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// <c>ST_GEOM_ASGEOG(GEOMETRY)</c>. Reads a geometry as a geography.
        /// </summary>
        public static readonly SqlFunction StGeomAsGeog =
            Function("ST_GEOM_ASGEOG", nameof(GeographyFunctions.AsGeography), GeographyReturnTypes.Geography,
                [GeographyOperand.Geometry], ["geom"]);

        /// <summary>
        /// <c>ST_GEOG_DISTANCE(GEOGRAPHY, GEOGRAPHY)</c>. The distance between two geographies, in metres.
        /// </summary>
        public static readonly SqlFunction StGeogDistance =
            Function("ST_GEOG_DISTANCE", nameof(GeographyFunctions.Distance), ReturnTypes.DOUBLE_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_DWITHIN(GEOGRAPHY, GEOGRAPHY, DOUBLE)</c>. Whether two geographies are within the given
        /// distance in metres.
        /// </summary>
        public static readonly SqlFunction StGeogDWithin =
            Function("ST_GEOG_DWITHIN", nameof(GeographyFunctions.DWithin), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography, GeographyOperand.Numeric], ["geog1", "geog2", "distance"]);

        /// <summary>
        /// <c>ST_GEOG_WITHIN(GEOGRAPHY, GEOGRAPHY)</c>. Whether the first geography lies within the second.
        /// </summary>
        public static readonly SqlFunction StGeogWithin =
            Function("ST_GEOG_WITHIN", nameof(GeographyFunctions.Within), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_INTERSECTS(GEOGRAPHY, GEOGRAPHY)</c>. Whether two geographies have any point in common.
        /// </summary>
        public static readonly SqlFunction StGeogIntersects =
            Function("ST_GEOG_INTERSECTS", nameof(GeographyFunctions.Intersects), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography, GeographyOperand.Geography], ["geog1", "geog2"]);

        /// <summary>
        /// <c>ST_GEOG_ISVALID(GEOGRAPHY)</c>. Whether the geography is valid on the sphere.
        /// </summary>
        public static readonly SqlFunction StGeogIsValid =
            Function("ST_GEOG_ISVALID", nameof(GeographyFunctions.IsValid), ReturnTypes.BOOLEAN_NULLABLE,
                [GeographyOperand.Geography], ["geog"]);

        /// <summary>
        /// Declares one operator.
        /// </summary>
        /// <param name="name">The SQL name.</param>
        /// <param name="method">The name of the method on <see cref="GeographyFunctions"/> that implements
        /// it.</param>
        /// <param name="returnType"></param>
        /// <param name="operands">What each position takes.</param>
        /// <param name="names">The name of each position.</param>
        /// <returns></returns>
        static SqlFunction Function(string name, string method, SqlReturnTypeInference returnType, GeographyOperand[] operands, string[] names)
        {
            // by signature and not by name. ScalarFunctionImpl.create(class, name) goes through
            // ReflectiveFunctionBase.findMethod, which answers the first method of that name and would pick
            // between two arities of a constructor at random. Deriving the signature from the operands is
            // also what keeps the declaration and the body from drifting apart: a position that takes a
            // geography is a Geometry parameter, and there is one place that says so.
            var parameters = new java.lang.Class[operands.Length];
            for (var i = 0; i < operands.Length; i++)
                parameters[i] = ClassOf(operands[i]);

            var found = ((java.lang.Class)typeof(GeographyFunctions)).getMethod(method, parameters) ??
                throw new InvalidOperationException($"No method '{method}' on '{nameof(GeographyFunctions)}'.");

            return new SqlUserDefinedFunction(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                SqlKind.OTHER_FUNCTION,
                returnType,
                null,
                new GeographyOperandTypeChecker(operands, names),
                ScalarFunctionImpl.create(found));
        }

        /// <summary>
        /// The parameter class a position of the given kind is passed as.
        /// </summary>
        /// <param name="operand"></param>
        /// <returns></returns>
        /// <remarks>
        /// A number is a <c>Number</c> rather than a <c>Double</c> because a literal arrives as whatever type
        /// it had, and <c>2.0</c> is a <c>BigDecimal</c>; see <c>GeographyFunctions.DWithin</c>.
        /// </remarks>
        static java.lang.Class ClassOf(GeographyOperand operand)
        {
            return operand switch
            {
                GeographyOperand.Geography => (java.lang.Class)typeof(org.locationtech.jts.geom.Geometry),
                GeographyOperand.Geometry => (java.lang.Class)typeof(org.locationtech.jts.geom.Geometry),
                GeographyOperand.Character => (java.lang.Class)typeof(string),
                GeographyOperand.Numeric => (java.lang.Class)typeof(java.lang.Number),
                _ => throw new NotSupportedException($"No parameter class for '{operand}'."),
            };
        }

        /// <summary>
        /// The one instance.
        /// </summary>
        /// <remarks>
        /// Declared after the operators deliberately: a static field initializer runs in textual order, and
        /// one placed above them builds the table out of ten nulls.
        /// </remarks>
        static readonly GeographyOperatorTable instance = new();

        /// <summary>
        /// Returns the operator table.
        /// </summary>
        /// <returns></returns>
        public static GeographyOperatorTable Instance()
        {
            return instance;
        }

        readonly SqlOperatorTable operators;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        GeographyOperatorTable()
        {
            operators = SqlOperatorTables.of([
                StGeogGeomFromGeoJson,
                StGeogGeomFromText,
                StGeogGeomFromWkt,
                StGeogGeomFromTextWithSrid,
                StGeogGeomFromWktWithSrid,
                StGeogAsGeom,
                StGeomAsGeog,
                StGeogDistance,
                StGeogDWithin,
                StGeogWithin,
                StGeogIntersects,
                StGeogIsValid,
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
