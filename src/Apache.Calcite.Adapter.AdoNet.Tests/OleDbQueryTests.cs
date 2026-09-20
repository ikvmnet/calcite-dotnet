using System;
using System.Collections.Generic;
using System.Data.OleDb;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;
using Xunit.Sdk;

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
    public class OleDbQueryTests : IDisposable
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

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public OleDbQueryTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");
            if (SqlServerFixture.OleDbProvider is null)
                Assert.Skip("No SQL Server OLE DB provider is registered for this process architecture.");

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

        /// <inheritdoc />
        public void Dispose()
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
            rows.Count.Should().Be(1, $"expected one row from: {sql}");
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
                ?? throw new XunitException($"no table {tableName}");

            var fields = table.getRowType(_types).getFieldList();

            var result = new Dictionary<string, RelDataType>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < fields.size(); i++)
                result[((RelDataTypeField)fields.get(i)).getName()] = ((RelDataTypeField)fields.get(i)).getType();

            return result;
        }

        #region Discovery

        [Fact]
        public void AnOleDbConnectionSelectsTheOleDbMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OleDbDataSource);
            Assert.Equal("OleDbDatabaseMetadata", metadata.GetType().Name);
        }

        /// <summary>
        /// That the version came with it is <see cref="AnOffsetIsHonoured"/>.
        /// </summary>
        [Fact]
        public void TheDialectIsTheOneTheProviderReports()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OleDbDataSource);

            Assert.IsAssignableFrom<org.apache.calcite.sql.dialect.MssqlSqlDialect>(metadata.Dialect);
        }

        [Fact]
        public void TheSchemaFindsTheTables()
        {
            var names = new List<string>();
            var found = _schema.tables().getNames(org.apache.calcite.schema.lookup.LikePattern.any());
            for (var i = found.iterator(); i.hasNext();)
                names.Add((string)i.next());

            Assert.Contains("SUPPLIERS", names);
            Assert.Contains("EMPS", names);
            Assert.Contains("DEPTS", names);
        }

        /// <summary>
        /// OLE DB states nullability as a <see cref="bool"/>, where the information schema and ODBC both
        /// state it otherwise.
        /// </summary>
        [Fact]
        public void NullabilityIsCarriedOntoTheType()
        {
            var fields = Fields("EMPS");

            Assert.False(fields["EMPNO"].isNullable(), "EMPNO is declared NOT NULL");
            Assert.True(fields["DEPTNO"].isNullable(), "DEPTNO is declared NULL");
        }

        #endregion

        #region Types

        [Theory]
        [InlineData("C_BIT", nameof(SqlTypeName.BOOLEAN))]
        [InlineData("C_TINYINT", nameof(SqlTypeName.UTINYINT))]
        [InlineData("C_SMALLINT", nameof(SqlTypeName.SMALLINT))]
        [InlineData("C_BIGINT", nameof(SqlTypeName.BIGINT))]
        [InlineData("C_DECIMAL", nameof(SqlTypeName.DECIMAL))]
        [InlineData("C_NUMERIC", nameof(SqlTypeName.DECIMAL))]
        [InlineData("C_MONEY", nameof(SqlTypeName.DECIMAL))]
        [InlineData("C_FLOAT", nameof(SqlTypeName.DOUBLE))]
        [InlineData("C_REAL", nameof(SqlTypeName.REAL))]
        [InlineData("C_CHAR", nameof(SqlTypeName.CHAR))]
        [InlineData("C_VARCHAR", nameof(SqlTypeName.VARCHAR))]
        [InlineData("C_NCHAR", nameof(SqlTypeName.CHAR))]
        [InlineData("C_NVARCHAR", nameof(SqlTypeName.VARCHAR))]
        [InlineData("C_DATE", nameof(SqlTypeName.DATE))]
        [InlineData("C_TIME", nameof(SqlTypeName.TIME))]
        [InlineData("C_DATETIME", nameof(SqlTypeName.TIMESTAMP))]
        [InlineData("C_DATETIME2", nameof(SqlTypeName.TIMESTAMP))]
        [InlineData("C_DATETIMEOFFSET", nameof(SqlTypeName.TIMESTAMP_TZ))]
        [InlineData("C_BINARY", nameof(SqlTypeName.VARBINARY))]
        [InlineData("C_VARBINARY", nameof(SqlTypeName.VARBINARY))]
        [InlineData("C_GUID", nameof(SqlTypeName.UUID))]
        [InlineData("C_XML", nameof(SqlTypeName.VARCHAR))]
        public void AColumnGetsItsCalciteType(string columnName, string expected)
        {
            Assert.Equal(expected, Fields("TYPES")[columnName].getSqlTypeName().name());
        }

        /// <summary>
        /// A <c>DBTYPE</c> says <c>DBTYPE_STR</c> for both <c>char</c> and <c>varchar</c>; only
        /// <c>DBCOLUMNFLAGS_ISFIXEDLENGTH</c> tells them apart, and Calcite pads a <c>CHAR</c>.
        /// </summary>
        [Fact]
        public void AFixedLengthColumnIsDistinguishedFromAVaryingOne()
        {
            Assert.Equal(nameof(SqlTypeName.CHAR), Fields("TYPES")["C_CHAR"].getSqlTypeName().name());
            Assert.Equal(nameof(SqlTypeName.VARCHAR), Fields("TYPES")["C_VARCHAR"].getSqlTypeName().name());
            Assert.Equal(nameof(SqlTypeName.CHAR), Fields("TYPES")["C_NCHAR"].getSqlTypeName().name());
            Assert.Equal(nameof(SqlTypeName.VARCHAR), Fields("TYPES")["C_NVARCHAR"].getSqlTypeName().name());
        }

        [Fact]
        public void EveryColumnTypeIsMapped()
        {
            Assert.Equal(26, Fields("TYPES").Count);
        }

        [Fact]
        public void EveryColumnTypeCanBeRead()
        {
            Assert.Equal(2, Rows("SELECT * FROM ADO.TYPES").Count);
        }

        [Theory]
        [InlineData("C_BIT", "true")]
        [InlineData("C_TINYINT", "200")]
        [InlineData("C_SMALLINT", "-300")]
        [InlineData("C_BIGINT", "9000000000")]
        [InlineData("C_DECIMAL", "123456789.125")]
        [InlineData("C_FLOAT", "1.5")]
        [InlineData("C_REAL", "2.5")]
        [InlineData("C_CHAR", "abcd")]
        [InlineData("C_VARCHAR", "varchar")]
        [InlineData("C_VARCHARMAX", "unbounded")]
        [InlineData("C_NCHAR", "wxyz")]
        [InlineData("C_NVARCHAR", "nvarchar")]
        [InlineData("C_GUID", "3f2504e0-4f89-11d3-9a0c-0305e82c3301")]
        public void AScalarValueComesBackAsWritten(string columnName, string expected)
        {
            Assert.Equal(expected, Scalar($"SELECT {columnName} FROM ADO.TYPES WHERE ID = 1"));
        }

        #endregion

        #region Query

        [Fact]
        public void ScanningATableReturnsItsRows()
        {
            Assert.Equivalent(
                new[] { "Widget|Acme|3", "Gadget|Globex|10", "Doohickey|Initech|1" },
                Rows("SELECT * FROM ADO.SUPPLIERS"), strict: true);
        }

        [Fact]
        public void AFilterIsApplied()
        {
            Assert.Equivalent(
                new[] { "Gadget" },
                Rows("SELECT PRODUCT FROM ADO.SUPPLIERS WHERE LEAD_DAYS > 5"), strict: true);
        }

        [Fact]
        public void AnAggregateIsComputed()
        {
            Assert.Equal("14", Scalar("SELECT SUM(LEAD_DAYS) FROM ADO.SUPPLIERS"));
        }

        [Fact]
        public void AJoinAcrossTwoTablesReturnsTheMatchedRows()
        {
            Assert.Equivalent(
                new[] { "Alice|Sales", "Bob|Sales", "Carol|Engineering", "Dave|Engineering" },
                Rows("SELECT E.NAME, D.DNAME FROM ADO.EMPS E JOIN ADO.DEPTS D ON E.DEPTNO = D.DEPTNO"), strict: true);
        }

        [Fact]
        public void AnOffsetIsHonoured()
        {
            Assert.Equal("Widget", Scalar("SELECT PRODUCT FROM ADO.SUPPLIERS ORDER BY LEAD_DAYS OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY"));
        }

        #endregion

    }

}
