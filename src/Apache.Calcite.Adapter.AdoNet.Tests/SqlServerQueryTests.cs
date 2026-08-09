using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the adapter against a real SQL Server, which is the only thing that exercises the information
    /// schema path and the SQL Server type mapping.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every query against SQL Server failed with <c>InvalidCastException: Unable to cast object of type
    /// 'System.Byte' to type 'System.Int32'</c> and nothing here said so, because the suite had only ever
    /// been pointed at SQLite, whose metadata comes from <c>PRAGMA table_xinfo</c> rather than from
    /// <c>INFORMATION_SCHEMA</c>. A provider's own description of itself is not something one provider can
    /// stand in for.
    /// </para>
    /// <para>
    /// LocalDB is Windows only, and a Windows machine need not have it either, so these skip where there is
    /// no server to talk to.
    /// </para>
    /// </remarks>
    [TestClass]
    public class SqlServerQueryTests
    {

        static SqlServerQueryTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        SqlServerFixture _server = null!;
        java.sql.Connection _connection = null!;
        AdoSchema _schema = null!;
        JavaTypeFactoryImpl _types = null!;

        [TestInitialize]
        public void Setup()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Inconclusive("No SQL Server LocalDB instance is reachable on this machine.");

            _server = SqlServerFixture.Shared;
            _types = new JavaTypeFactoryImpl();

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            _connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);

            var calcite = (CalciteConnection)_connection;
            var root = calcite.getRootSchema();
            _schema = AdoSchema.Create(root, "ADO", _server.DataSource, null, "dbo");
            root.add("ADO", _schema);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _connection?.close();
        }

        /// <summary>
        /// Runs a query and returns its rows as strings, so a comparison does not depend on which numeric
        /// type a provider chose.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        List<string> Rows(string sql)
        {
            using var statement = _connection.createStatement();
            var results = statement.executeQuery(sql);

            var rows = new List<string>();
            var columns = results.getMetaData().getColumnCount();

            while (results.next())
            {
                var values = new string[columns];
                for (int i = 0; i < columns; i++)
                    values[i] = results.getObject(i + 1)?.ToString() ?? "NULL";

                rows.Add(string.Join("|", values));
            }

            return rows;
        }

        /// <summary>
        /// Runs a query and returns the single value it produces.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        string Scalar(string sql)
        {
            var rows = Rows(sql);
            Assert.AreEqual(1, rows.Count, $"expected one row from: {sql}");
            return rows[0];
        }

        /// <summary>
        /// Returns a table by name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        org.apache.calcite.schema.Table Table(string tableName)
        {
            return (org.apache.calcite.schema.Table?)_schema.tables().get(tableName)
                ?? throw new AssertFailedException($"no table {tableName}; found {string.Join(", ", TableNames())}");
        }

        /// <summary>
        /// Returns every table name the schema exposes.
        /// </summary>
        /// <returns></returns>
        List<string> TableNames()
        {
            var names = _schema.tables().getNames(org.apache.calcite.schema.lookup.LikePattern.any());

            var result = new List<string>();
            for (var i = names.iterator(); i.hasNext();)
                result.Add((string)i.next());

            return result;
        }

        /// <summary>
        /// Returns the fields of a table's row type, by name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        Dictionary<string, RelDataType> Fields(string tableName)
        {
            var fields = Table(tableName).getRowType(_types).getFieldList();

            var result = new Dictionary<string, RelDataType>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < fields.size(); i++)
                result[((RelDataTypeField)fields.get(i)).getName()] = ((RelDataTypeField)fields.get(i)).getType();

            return result;
        }

        /// <summary>
        /// Returns the Calcite type name of a named column of a table.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        string TypeOf(string tableName, string columnName)
        {
            if (Fields(tableName).TryGetValue(columnName, out var type) == false)
                throw new AssertFailedException($"no column {columnName} on {tableName}");

            return type.getSqlTypeName().name();
        }

        #region The report

        /// <summary>
        /// The query from the report, verbatim in shape: three columns, one of them the <c>INT</c> whose
        /// <c>tinyint</c> precision in the information schema was read as an <c>int</c> and threw.
        /// </summary>
        [TestMethod]
        public void ScanningATableWithAnIntegerColumnReturnsItsRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Widget|Acme|3", "Gadget|Globex|10", "Doohickey|Initech|1" },
                Rows("SELECT * FROM ADO.SUPPLIERS"));
        }

        /// <summary>
        /// Projecting the two <c>VARCHAR</c> columns failed the same way, because the row type is derived
        /// from every column of the table whatever the query asks for.
        /// </summary>
        [TestMethod]
        public void ProjectingOnlyTheCharacterColumnsAlsoWorks()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Widget|Acme", "Gadget|Globex", "Doohickey|Initech" },
                Rows("SELECT PRODUCT, SUPPLIER FROM ADO.SUPPLIERS"));
        }

        #endregion

        #region Discovery

        [TestMethod]
        public void ASqlServerConnectionSelectsTheSqlServerMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.DataSource);
            Assert.AreEqual("SqlServerDatabaseMetadata", metadata.GetType().Name);
        }

        [TestMethod]
        public void TheSchemaFindsTheTables()
        {
            var names = TableNames();

            CollectionAssert.Contains(names, "SUPPLIERS");
            CollectionAssert.Contains(names, "EMPS");
            CollectionAssert.Contains(names, "DEPTS");
        }

        [TestMethod]
        public void NullabilityIsCarriedOntoTheType()
        {
            var fields = Fields("EMPS");

            Assert.IsFalse(fields["EMPNO"].isNullable(), "EMPNO is declared NOT NULL");
            Assert.IsTrue(fields["DEPTNO"].isNullable(), "DEPTNO is declared NULL");
        }

        #endregion

        #region Types

        /// <summary>
        /// Every column of the fixture's wide table has to reach a Calcite type: a name the mapping does not
        /// know throws, and takes the whole table with it.
        /// </summary>
        [TestMethod]
        public void EveryColumnTypeIsMapped()
        {
            Assert.AreEqual(26, Fields("TYPES").Count);
        }

        [TestMethod]
        [DataRow("C_BIT", nameof(SqlTypeName.BOOLEAN))]
        // the server's tinyint is unsigned 0..255 and Calcite's TINYINT is signed, so UTINYINT is what holds it
        [DataRow("C_TINYINT", nameof(SqlTypeName.UTINYINT))]
        [DataRow("C_SMALLINT", nameof(SqlTypeName.SMALLINT))]
        [DataRow("C_BIGINT", nameof(SqlTypeName.BIGINT))]
        [DataRow("C_DECIMAL", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_NUMERIC", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_MONEY", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_SMALLMONEY", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_FLOAT", nameof(SqlTypeName.DOUBLE))]
        [DataRow("C_REAL", nameof(SqlTypeName.REAL))]
        [DataRow("C_CHAR", nameof(SqlTypeName.CHAR))]
        [DataRow("C_VARCHAR", nameof(SqlTypeName.VARCHAR))]
        [DataRow("C_NCHAR", nameof(SqlTypeName.CHAR))]
        [DataRow("C_NVARCHAR", nameof(SqlTypeName.VARCHAR))]
        [DataRow("C_DATE", nameof(SqlTypeName.DATE))]
        [DataRow("C_TIME", nameof(SqlTypeName.TIME))]
        [DataRow("C_DATETIME", nameof(SqlTypeName.TIMESTAMP))]
        [DataRow("C_SMALLDATETIME", nameof(SqlTypeName.TIMESTAMP))]
        [DataRow("C_DATETIME2", nameof(SqlTypeName.TIMESTAMP))]
        [DataRow("C_DATETIMEOFFSET", nameof(SqlTypeName.TIMESTAMP_TZ))]
        [DataRow("C_BINARY", nameof(SqlTypeName.VARBINARY))]
        [DataRow("C_VARBINARY", nameof(SqlTypeName.VARBINARY))]
        [DataRow("C_GUID", nameof(SqlTypeName.CHAR))]
        [DataRow("C_XML", nameof(SqlTypeName.VARCHAR))]
        public void AColumnGetsItsCalciteType(string columnName, string expected)
        {
            Assert.AreEqual(expected, TypeOf("TYPES", columnName));
        }

        /// <summary>
        /// Reading the wide table is a separate claim from typing it: the reader picks its accessor from the
        /// Calcite type, and a mapping that types a column plausibly can still refuse to read one.
        /// </summary>
        [TestMethod]
        public void EveryColumnTypeCanBeRead()
        {
            Assert.AreEqual(2, Rows("SELECT * FROM ADO.TYPES").Count);
        }

        [TestMethod]
        public void ANullOfEveryTypeComesBackNull()
        {
            var row = Scalar("SELECT * FROM ADO.TYPES WHERE ID = 2");
            var values = row.Split('|');

            Assert.AreEqual("2", values[0]);
            for (int i = 1; i < values.Length; i++)
                Assert.AreEqual("NULL", values[i], $"column {i} of the all-null row");
        }

        [TestMethod]
        public void ADecimalKeepsItsScale()
        {
            Assert.AreEqual("123456789.125", Scalar("SELECT C_DECIMAL FROM ADO.TYPES WHERE ID = 1"));
        }

        [TestMethod]
        public void AVarcharMaxIsReadable()
        {
            Assert.AreEqual("unbounded", Scalar("SELECT C_VARCHARMAX FROM ADO.TYPES WHERE ID = 1"));
        }

        /// <summary>
        /// A <c>uniqueidentifier</c> is a <see cref="Guid"/> to the provider and a <c>CHAR(36)</c> to
        /// Calcite, so what the reader hands over has to be the text.
        /// </summary>
        [TestMethod]
        public void AUniqueIdentifierComesBackAsItsText()
        {
            Assert.AreEqual("3f2504e0-4f89-11d3-9a0c-0305e82c3301", Scalar("SELECT C_GUID FROM ADO.TYPES WHERE ID = 1"));
        }

        [TestMethod]
        [DataRow("C_BIT", "true")]
        [DataRow("C_TINYINT", "200")]
        [DataRow("C_SMALLINT", "-300")]
        [DataRow("C_BIGINT", "9000000000")]
        [DataRow("C_FLOAT", "1.5")]
        [DataRow("C_REAL", "2.5")]
        [DataRow("C_CHAR", "abcd")]
        [DataRow("C_NCHAR", "wxyz")]
        [DataRow("C_NVARCHAR", "nvarchar")]
        public void AScalarValueComesBackAsWritten(string columnName, string expected)
        {
            Assert.AreEqual(expected, Scalar($"SELECT {columnName} FROM ADO.TYPES WHERE ID = 1"));
        }

        #endregion

        #region Query

        /// <summary>
        /// A filter on the unsigned column, which the planner pushes down as a comparison against a literal
        /// typed <c>UTINYINT</c>: the value 200 has to survive planning without wrapping to -56, and the
        /// dialect has to unparse the literal as something the server parses.
        /// </summary>
        [TestMethod]
        public void AFilterOnATinyIntComparesUnsigned()
        {
            Assert.AreEqual("1", Scalar("SELECT ID FROM ADO.TYPES WHERE C_TINYINT = 200"));
            Assert.AreEqual("1", Scalar("SELECT ID FROM ADO.TYPES WHERE C_TINYINT > 100"));
            Assert.AreEqual(0, Rows("SELECT ID FROM ADO.TYPES WHERE C_TINYINT < 0").Count);
        }

        [TestMethod]
        public void AFilterIsApplied()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Gadget" },
                Rows("SELECT PRODUCT FROM ADO.SUPPLIERS WHERE LEAD_DAYS > 5"));
        }

        [TestMethod]
        public void AnAggregateIsComputed()
        {
            Assert.AreEqual("14", Scalar("SELECT SUM(LEAD_DAYS) FROM ADO.SUPPLIERS"));
        }

        [TestMethod]
        public void AJoinAcrossTwoTablesReturnsTheMatchedRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering" },
                Rows("SELECT E.NAME, D.DNAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO"));
        }

        [TestMethod]
        public void AnOrderByIsHonoured()
        {
            CollectionAssert.AreEqual(
                new[] { "Doohickey", "Widget", "Gadget" },
                Rows("SELECT PRODUCT FROM ADO.SUPPLIERS ORDER BY LEAD_DAYS"));
        }

        #endregion

    }

}
