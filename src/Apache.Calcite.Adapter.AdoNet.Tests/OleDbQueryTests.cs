using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using System;
using System.Collections.Generic;
using System.Data.OleDb;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the adapter over an <see cref="OleDbConnection"/>, against the same LocalDB database
    /// <see cref="SqlServerQueryTests"/> uses.
    /// </summary>
    /// <remarks>
    /// OLE DB's schema rowsets borrow the information schema's column names without its types — a numeric
    /// <c>DATA_TYPE</c>, a <see cref="bool"/> <c>IS_NULLABLE</c>, a <see cref="decimal"/>
    /// <c>CHARACTER_MAXIMUM_LENGTH</c> — which is close enough to look like it should have worked and did
    /// not: the whole of <see cref="Metadata.OleDbDatabaseMetadata"/> threw
    /// <see cref="NotImplementedException"/>.
    /// </remarks>
    [TestClass]
    public class OleDbQueryTests
    {

        static OleDbQueryTests()
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
            if (SqlServerFixture.OleDbProvider is null)
                Assert.Inconclusive("No SQL Server OLE DB provider is registered for this process architecture.");

            _server = SqlServerFixture.Shared;
            _types = new JavaTypeFactoryImpl();

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            _connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);

            var calcite = (CalciteConnection)_connection;
            var root = calcite.getRootSchema();
            _schema = AdoSchema.Create(root, "ADO", _server.OleDbDataSource, null, "dbo");
            root.add("ADO", _schema);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _connection?.close();
        }

        /// <summary>
        /// Runs a query and returns its rows as strings.
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
        /// Returns the fields of a table's row type, by name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        Dictionary<string, RelDataType> Fields(string tableName)
        {
            var table = (org.apache.calcite.schema.Table?)_schema.tables().get(tableName)
                ?? throw new AssertFailedException($"no table {tableName}");

            var fields = table.getRowType(_types).getFieldList();

            var result = new Dictionary<string, RelDataType>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < fields.size(); i++)
                result[((RelDataTypeField)fields.get(i)).getName()] = ((RelDataTypeField)fields.get(i)).getType();

            return result;
        }

        #region Discovery

        [TestMethod]
        public void AnOleDbConnectionSelectsTheOleDbMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OleDbDataSource);
            Assert.AreEqual("OleDbDatabaseMetadata", metadata.GetType().Name);
        }

        /// <summary>
        /// That the version came with it is <see cref="AnOffsetIsHonoured"/>.
        /// </summary>
        [TestMethod]
        public void TheDialectIsTheOneTheProviderReports()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OleDbDataSource);

            Assert.IsInstanceOfType<org.apache.calcite.sql.dialect.MssqlSqlDialect>(metadata.Dialect);
        }

        [TestMethod]
        public void TheSchemaFindsTheTables()
        {
            var names = new List<string>();
            var found = _schema.tables().getNames(org.apache.calcite.schema.lookup.LikePattern.any());
            for (var i = found.iterator(); i.hasNext();)
                names.Add((string)i.next());

            CollectionAssert.Contains(names, "SUPPLIERS");
            CollectionAssert.Contains(names, "EMPS");
            CollectionAssert.Contains(names, "DEPTS");
        }

        /// <summary>
        /// OLE DB states nullability as a <see cref="bool"/>, where the information schema and ODBC both
        /// state it otherwise.
        /// </summary>
        [TestMethod]
        public void NullabilityIsCarriedOntoTheType()
        {
            var fields = Fields("EMPS");

            Assert.IsFalse(fields["EMPNO"].isNullable(), "EMPNO is declared NOT NULL");
            Assert.IsTrue(fields["DEPTNO"].isNullable(), "DEPTNO is declared NULL");
        }

        #endregion

        #region Types

        [TestMethod]
        [DataRow("C_BIT", nameof(SqlTypeName.BOOLEAN))]
        [DataRow("C_TINYINT", nameof(SqlTypeName.UTINYINT))]
        [DataRow("C_SMALLINT", nameof(SqlTypeName.SMALLINT))]
        [DataRow("C_BIGINT", nameof(SqlTypeName.BIGINT))]
        [DataRow("C_DECIMAL", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_NUMERIC", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_MONEY", nameof(SqlTypeName.DECIMAL))]
        [DataRow("C_FLOAT", nameof(SqlTypeName.DOUBLE))]
        [DataRow("C_REAL", nameof(SqlTypeName.REAL))]
        [DataRow("C_CHAR", nameof(SqlTypeName.CHAR))]
        [DataRow("C_VARCHAR", nameof(SqlTypeName.VARCHAR))]
        [DataRow("C_NCHAR", nameof(SqlTypeName.CHAR))]
        [DataRow("C_NVARCHAR", nameof(SqlTypeName.VARCHAR))]
        [DataRow("C_DATE", nameof(SqlTypeName.DATE))]
        [DataRow("C_TIME", nameof(SqlTypeName.TIME))]
        [DataRow("C_DATETIME", nameof(SqlTypeName.TIMESTAMP))]
        [DataRow("C_DATETIME2", nameof(SqlTypeName.TIMESTAMP))]
        [DataRow("C_DATETIMEOFFSET", nameof(SqlTypeName.TIMESTAMP_TZ))]
        [DataRow("C_BINARY", nameof(SqlTypeName.VARBINARY))]
        [DataRow("C_VARBINARY", nameof(SqlTypeName.VARBINARY))]
        [DataRow("C_GUID", nameof(SqlTypeName.CHAR))]
        [DataRow("C_XML", nameof(SqlTypeName.VARCHAR))]
        public void AColumnGetsItsCalciteType(string columnName, string expected)
        {
            Assert.AreEqual(expected, Fields("TYPES")[columnName].getSqlTypeName().name());
        }

        /// <summary>
        /// A <c>DBTYPE</c> says <c>DBTYPE_STR</c> for both <c>char</c> and <c>varchar</c>; only
        /// <c>DBCOLUMNFLAGS_ISFIXEDLENGTH</c> tells them apart, and Calcite pads a <c>CHAR</c>.
        /// </summary>
        [TestMethod]
        public void AFixedLengthColumnIsDistinguishedFromAVaryingOne()
        {
            Assert.AreEqual(nameof(SqlTypeName.CHAR), Fields("TYPES")["C_CHAR"].getSqlTypeName().name());
            Assert.AreEqual(nameof(SqlTypeName.VARCHAR), Fields("TYPES")["C_VARCHAR"].getSqlTypeName().name());
            Assert.AreEqual(nameof(SqlTypeName.CHAR), Fields("TYPES")["C_NCHAR"].getSqlTypeName().name());
            Assert.AreEqual(nameof(SqlTypeName.VARCHAR), Fields("TYPES")["C_NVARCHAR"].getSqlTypeName().name());
        }

        [TestMethod]
        public void EveryColumnTypeIsMapped()
        {
            Assert.AreEqual(26, Fields("TYPES").Count);
        }

        [TestMethod]
        public void EveryColumnTypeCanBeRead()
        {
            Assert.AreEqual(2, Rows("SELECT * FROM ADO.TYPES").Count);
        }

        [TestMethod]
        [DataRow("C_BIT", "true")]
        [DataRow("C_TINYINT", "200")]
        [DataRow("C_SMALLINT", "-300")]
        [DataRow("C_BIGINT", "9000000000")]
        [DataRow("C_DECIMAL", "123456789.125")]
        [DataRow("C_FLOAT", "1.5")]
        [DataRow("C_REAL", "2.5")]
        [DataRow("C_CHAR", "abcd")]
        [DataRow("C_VARCHAR", "varchar")]
        [DataRow("C_VARCHARMAX", "unbounded")]
        [DataRow("C_NCHAR", "wxyz")]
        [DataRow("C_NVARCHAR", "nvarchar")]
        [DataRow("C_GUID", "3f2504e0-4f89-11d3-9a0c-0305e82c3301")]
        public void AScalarValueComesBackAsWritten(string columnName, string expected)
        {
            Assert.AreEqual(expected, Scalar($"SELECT {columnName} FROM ADO.TYPES WHERE ID = 1"));
        }

        #endregion

        #region Query

        [TestMethod]
        public void ScanningATableReturnsItsRows()
        {
            CollectionAssert.AreEquivalent(
                new[] { "Widget|Acme|3", "Gadget|Globex|10", "Doohickey|Initech|1" },
                Rows("SELECT * FROM ADO.SUPPLIERS"));
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
        public void AnOffsetIsHonoured()
        {
            Assert.AreEqual("Widget", Scalar("SELECT PRODUCT FROM ADO.SUPPLIERS ORDER BY LEAD_DAYS OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY"));
        }

        #endregion

    }

}
