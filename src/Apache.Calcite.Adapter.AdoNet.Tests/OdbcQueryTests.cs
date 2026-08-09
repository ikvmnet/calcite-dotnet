using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using System;
using System.Collections.Generic;
using System.Data.Odbc;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the adapter over an <see cref="OdbcConnection"/>, against the same LocalDB database
    /// <see cref="SqlServerQueryTests"/> uses.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The ODBC catalog is not the information schema — <c>TABLE_CAT</c>, <c>TABLE_SCHEM</c>, a numeric
    /// <c>DATA_TYPE</c>, no <c>NUMERIC_PRECISION</c> — so nothing about the SQL Server metadata provider
    /// carries over, and the whole of <see cref="Metadata.OdbcDatabaseMetadata"/> threw
    /// <see cref="NotImplementedException"/> until it was written against the shape the driver actually
    /// returns.
    /// </para>
    /// <para>
    /// Pointing it at the same database as the SqlClient suite is the point: the answers have to be the
    /// same, and a metadata provider that has misread its own driver's collections says something different.
    /// </para>
    /// </remarks>
    [TestClass]
    public class OdbcQueryTests
    {

        static OdbcQueryTests()
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
            if (SqlServerFixture.OdbcDriver is null)
                Assert.Inconclusive("No SQL Server ODBC driver is installed on this machine.");

            _server = SqlServerFixture.Shared;
            _types = new JavaTypeFactoryImpl();

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");

            _connection = java.sql.DriverManager.getConnection("jdbc:calcite:", properties);

            var calcite = (CalciteConnection)_connection;
            var root = calcite.getRootSchema();
            _schema = AdoSchema.Create(root, "ADO", _server.OdbcDataSource, null, "dbo");
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
        public void AnOdbcConnectionSelectsTheOdbcMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OdbcDataSource);
            Assert.AreEqual("OdbcDatabaseMetadata", metadata.GetType().Name);
        }

        /// <summary>
        /// ODBC fronts anything, so the dialect can only come from what the driver says is behind it, which
        /// here is SQL Server. That the version came with it is <see cref="AnOffsetIsHonoured"/>.
        /// </summary>
        [TestMethod]
        public void TheDialectIsTheOneTheDriverReports()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OdbcDataSource);

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
        /// The ODBC catalog spells nullability as <c>NULLABLE</c>, an integer, where the information schema
        /// spells it <c>IS_NULLABLE</c> and <c>YES</c>.
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
        // ODBC reports money as SQL_DECIMAL with its precision and scale, so it arrives as the decimal it is
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

        [TestMethod]
        public void EveryColumnTypeIsMapped()
        {
            Assert.AreEqual(26, Fields("TYPES").Count);
        }

        /// <summary>
        /// Every column the driver can read, read. <c>C_TIME</c> and <c>C_DATETIMEOFFSET</c> are left out:
        /// see <see cref="TheDriverCannotReadSqlServersOwnTimeTypes"/>.
        /// </summary>
        [TestMethod]
        public void EveryReadableColumnTypeCanBeRead()
        {
            Assert.AreEqual(2, Rows("""
                SELECT ID, C_BIT, C_TINYINT, C_SMALLINT, C_BIGINT, C_DECIMAL, C_NUMERIC, C_MONEY, C_SMALLMONEY,
                       C_FLOAT, C_REAL, C_CHAR, C_VARCHAR, C_VARCHARMAX, C_NCHAR, C_NVARCHAR, C_DATE,
                       C_DATETIME, C_SMALLDATETIME, C_DATETIME2, C_BINARY, C_VARBINARY, C_GUID, C_XML
                FROM ADO.TYPES
                """).Count);
        }

        /// <summary>
        /// A limitation of the driver rather than of the adapter, pinned so that it is a stated fact rather
        /// than a surprise: <c>System.Data.Odbc</c> has no mapping for <c>SQL_SS_TIME2</c> or
        /// <c>SQL_SS_TIMESTAMPOFFSET</c>, and <c>TypeMap.FromSqlType</c> throws on either. The columns are
        /// still typed — the metadata comes from the catalog, not from a reader — and only reading one
        /// fails.
        /// </summary>
        [TestMethod]
        public void TheDriverCannotReadSqlServersOwnTimeTypes()
        {
            Assert.AreEqual(nameof(SqlTypeName.TIME), Fields("TYPES")["C_TIME"].getSqlTypeName().name());

            var thrown = Assert.ThrowsException<ArgumentException>(() => Rows("SELECT C_TIME FROM ADO.TYPES"));
            StringAssert.Contains(thrown.Message, "SS_TIME_EX");
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

        /// <summary>
        /// The dialect has to know the server is past 2012 for this to be right: under that,
        /// <c>MssqlSqlDialect</c> writes <c>TOP(1)</c> and drops the offset, and the answer is the first row
        /// rather than the second.
        /// </summary>
        [TestMethod]
        public void AnOffsetIsHonoured()
        {
            Assert.AreEqual("Widget", Scalar("SELECT PRODUCT FROM ADO.SUPPLIERS ORDER BY LEAD_DAYS OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY"));
        }

        #endregion

    }

}
