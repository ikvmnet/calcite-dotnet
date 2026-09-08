using System;
using System.Collections.Generic;

using Apache.Calcite.Data;
using Apache.Calcite.Data.Types;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// A CLR type mapping registered on an adapter schema, and reached by a table scan.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The mapping is the <i>schema's</i> and not a connection's. A schema outlives every connection that
    /// reads it and its tables are read the same way for all of them; which .NET type a caller finally
    /// sees is a separate question, asked of the connection at the ADO.NET surface. So it arrives the way
    /// the data source does — as an operand where a model names one, as an argument where an application
    /// builds the schema itself — and a plan fetches it off the schema at run time through
    /// <c>Schemas.unwrap</c>.
    /// </para>
    /// <para>
    /// Both routes are here because they are compiled by different things and only one of them was ever
    /// believed able to carry a mapping. A statement over <c>jdbc:calcite:</c>, and one over this
    /// provider's own connection in its default mode, cross through <see cref="AdoToEnumerableConverter"/>
    /// — Java source, compiled by Janino. <c>Synchronous=true</c> converts straight into
    /// <c>ClrEnumerableConvention</c> through <see cref="AdoToClrEnumerableConverter"/>, an expression
    /// tree. The value the resolver produces is a thousand higher than the column holds, which nothing but
    /// a resolver reaching the scan could do.
    /// </para>
    /// </remarks>
    [TestClass]
    public class AdoSchemaTypeMappingTests
    {

        /// <summary>
        /// Carries a BIGINT into a plan a thousand higher than the provider produced it.
        /// </summary>
        /// <remarks>
        /// BIGINT and not INTEGER because SQLite's <c>INTEGER</c> is sixty-four bits, which the adapter
        /// reads as <see cref="System.Data.DbType.Int64"/> and types accordingly.
        /// </remarks>
        sealed class OffsetResolver : IClrTypeResolver
        {

            readonly ClrTypeMappingCollection _mappings = new();

            public OffsetResolver()
            {
                _mappings.Add(
                    typeof(long),
                    SqlTypeName.BIGINT,
                    static (v, _) => java.lang.Long.valueOf(Convert.ToInt64(v) + 1000),
                    static (v, t) => CalciteValues.ToClr(v, t));
            }

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context) => _mappings.GetMapping(clrType, relType, context);

        }

        static ClrTypeMapper Mapper() => new ClrTypeMapper().Prepend(new OffsetResolver());

        SqliteFixture _sqlite = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();
        }

        [TestCleanup]
        public void Cleanup()
        {
            _sqlite?.Dispose();
        }

        /// <summary>
        /// The provider's own connection, in the mode asked for.
        /// </summary>
        CalciteConnection OpenConnection(bool synchronous, ClrTypeMapper? mapper)
        {
            return new CalciteDataSourceBuilder(new CalciteConnectionStringBuilder
            {
                Lex = "JAVA",
                CaseSensitive = false,
                Synchronous = synchronous ? true : null,
            }.ToString())
                .ConfigureRootSchema(root => root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null, mapper)))
                .Build()
                .OpenConnection();
        }

        static List<string> Rows(CalciteConnection connection, string sql)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;

            var rows = new List<string>();
            using var r = cmd.ExecuteReader();
            while (r.Read())
                rows.Add(r.IsDBNull(0) ? "null" : r.GetValue(0).ToString()!);

            return rows;
        }

        [TestMethod]
        public void AMappingOnTheSchemaReachesTheClrConverter()
        {
            using var connection = OpenConnection(synchronous: true, Mapper());

            CollectionAssert.AreEqual(
                new[] { "1001", "1002", "1003", "1004", "1005" },
                Rows(connection, "SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO"));
        }

        [TestMethod]
        public void AMappingOnTheSchemaReachesTheGeneratedReader()
        {
            using var connection = OpenConnection(synchronous: false, Mapper());

            CollectionAssert.AreEqual(
                new[] { "1001", "1002", "1003", "1004", "1005" },
                Rows(connection, "SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO"));
        }

        [TestMethod]
        public void AnUnmappedSchemaIsUnaffected()
        {
            using var connection = OpenConnection(synchronous: true, null);

            CollectionAssert.AreEqual(
                new[] { "1", "2", "3", "4", "5" },
                Rows(connection, "SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO"));
        }

        /// <summary>
        /// And through Calcite's own connection, which is the only place the generated reader is all
        /// there is — no Clr convention anywhere in the plan.
        /// </summary>
        [TestMethod]
        public void AMappingOnTheSchemaReachesCalcitesOwnConnection()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            using var connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);
            var root = ((org.apache.calcite.jdbc.CalciteConnection)connection).getRootSchema();
            root.add("ADO", AdoSchema.Create(root, "ADO", _sqlite.DataSource, null, null, Mapper()));

            using var statement = connection.createStatement();
            var results = statement.executeQuery("SELECT EMPNO FROM ADO.EMPS ORDER BY EMPNO");

            var rows = new List<string>();
            while (results.next())
                rows.Add(results.getObject(1)?.ToString() ?? "null");

            CollectionAssert.AreEqual(new[] { "1001", "1002", "1003", "1004", "1005" }, rows);
        }

    }

}
