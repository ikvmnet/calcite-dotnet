using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Rel.Type;
using Apache.Calcite.Geography.Runtime;
using Apache.Calcite.Geography.Schema;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using DataContext = org.apache.calcite.DataContext;
using Geometry = org.locationtech.jts.geom.Geometry;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// The operators reached through the stock Calcite JDBC driver, with nothing chained and nothing
    /// subclassed.
    /// </summary>
    /// <remarks>
    /// This is the whole point of typing them over <c>GEOMETRY</c>. Everything else in this suite drives a
    /// planner by hand and hands it an operator table, which only a host embedding Calcite can do. Here the
    /// connection is <c>jdbc:calcite:</c>, the schema is registered the way an adapter would register one,
    /// and the SQL goes through <c>Statement.executeQuery</c> — the path a consumer who has never heard of
    /// this package takes.
    /// </remarks>
    [TestClass]
    public class GeographySchemaTests
    {

        static List<object?[]> Run(string sql)
        {
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:fun=spatial");
            var calcite = (CalciteConnection)connection.unwrap((java.lang.Class)typeof(CalciteConnection));
            var root = calcite.getRootSchema();

            root.add("GEO", new PlacesTable());
            GeographySchema.AddTo(root);

            using var statement = connection.createStatement();
            var results = statement.executeQuery(sql);
            var count = results.getMetaData().getColumnCount();

            var rows = new List<object?[]>();

            while (results.next())
            {
                var row = new object?[count];
                for (var i = 0; i < count; i++)
                    row[i] = results.getObject(i + 1);

                rows.Add(row);
            }

            return rows;
        }

        [TestMethod]
        public void ShouldRunAConstructorAndAMeasurement()
        {
            var rows = Run("SELECT ST_GEOG_DISTANCE(ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), ST_GEOG_GEOMFROMTEXT('POINT(1 0)'))");

            rows.Should().HaveCount(1);
            ((java.lang.Number)rows[0][0]!).doubleValue().Should().BeApproximately(6371010.0 * Math.PI / 180, 0.001);
        }

        [TestMethod]
        public void ShouldReadAGeographyColumn()
        {
            var rows = Run("SELECT ID, ST_GEOG_ASTEXT(GEOG) FROM GEO ORDER BY ID");

            rows.Should().HaveCount(2);
            rows[0][1].Should().Be("POINT (0.5 0)");
        }

        [TestMethod]
        public void ShouldRunAnAccessorOverAColumn()
        {
            var rows = Run("SELECT ID, ST_GEOG_X(GEOG) FROM GEO ORDER BY ID");

            rows.Should().HaveCount(2);
            ((java.lang.Number)rows[0][1]!).doubleValue().Should().Be(0.5);
        }

        [TestMethod]
        public void ShouldRunAPredicateOverAColumn()
        {
            var rows = Run("SELECT ID FROM GEO WHERE ST_GEOG_DWITHIN(GEOG, ST_GEOG_GEOMFROMTEXT('POINT(0 0)'), 200000.0)");

            rows.Should().HaveCount(1);
            ((java.lang.Number)rows[0][0]!).intValue().Should().Be(1);
        }

        /// <summary>
        /// Calcite's own planar operators take the same column, which is the cost of there being one type.
        /// </summary>
        [TestMethod]
        public void ShouldLetCalcitesOwnOperatorsTakeTheSameColumn()
        {
            var rows = Run("SELECT ST_DISTANCE(GEOG, ST_GEOMFROMTEXT('POINT(0 0)')) FROM GEO WHERE ID = 1");

            rows.Should().HaveCount(1);
            ((java.lang.Number)rows[0][0]!).doubleValue().Should().BeApproximately(0.5, 1e-9);
        }

        /// <summary>
        /// Two rows, a degree apart on the equator, in a table an adapter could have built.
        /// </summary>
        sealed class PlacesTable : AbstractTable, ScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("GEOG", GeographyTypes.Of(typeFactory))
                    .build();
            }

            public Enumerable scan(DataContext root)
            {
                return Linq4j.asEnumerable(java.util.Arrays.asList([
                    new object[] { java.lang.Integer.valueOf(1), Geog("POINT(0.5 0)") },
                    new object[] { java.lang.Integer.valueOf(2), Geog("POINT(20 0)") },
                ]));
            }

            static Geometry Geog(string wkt)
            {
                var g = org.apache.calcite.runtime.SpatialTypeUtils.fromWkt(wkt);
                g.setSRID(GeographyFunctions.Wgs84);
                return g;
            }

        }

    }

}
