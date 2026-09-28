using System;
using System.Collections.Generic;
using System.Data.Odbc;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;
using Xunit.Sdk;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests the adapter over an <see cref="OdbcConnection"/>, against the same LocalDB database
    /// <see cref="SqlServerQueryTests"/> uses.
    /// </summary>
    /// <remarks>
    /// The ODBC catalog differs from the information schema (<c>TABLE_CAT</c>, <c>TABLE_SCHEM</c>, a numeric
    /// <c>DATA_TYPE</c>, no <c>NUMERIC_PRECISION</c>), so <see cref="Metadata.OdbcDatabaseMetadata"/> reads it
    /// separately. Using the same database as the SqlClient suite means the answers must agree.
    /// </remarks>
    public class OdbcQueryTests : IDisposable
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

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public OdbcQueryTests()
        {
            if (SqlServerFixture.IsAvailable == false)
                Assert.Skip("No SQL Server LocalDB instance is reachable on this machine.");
            if (SqlServerFixture.OdbcDriver is null)
                Assert.Skip("No SQL Server ODBC driver is installed on this machine.");

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

        /// <inheritdoc />
        public void Dispose()
        {
            _connection?.close();
        }

        /// <summary>
        /// Runs a query and returns each row's values as strings joined by a pipe.
        /// </summary>
        /// <param name="sql">The statement to run through Calcite over the ODBC data source.</param>
        /// <returns>One string per row, its values joined by a pipe with <c>NULL</c> for a null.</returns>
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
        /// Runs a query that must return one row and returns that row as <see cref="Rows"/> formats it.
        /// </summary>
        /// <param name="sql">A statement expected to produce exactly one row.</param>
        /// <returns>The single row, its values joined by a pipe.</returns>
        string Scalar(string sql)
        {
            var rows = Rows(sql);
            rows.Count.Should().Be(1, $"expected one row from: {sql}");
            return rows[0];
        }

        /// <summary>
        /// Returns the fields of a table's row type, keyed case-insensitively by name.
        /// </summary>
        /// <param name="tableName">The name of a table in the mounted schema, exactly as the schema exposes
        /// it.</param>
        /// <returns>Each column's Calcite type, keyed by column name without regard to case.</returns>
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
        public void AnOdbcConnectionSelectsTheOdbcMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OdbcDataSource);
            Assert.Equal("OdbcDatabaseMetadata", metadata.GetType().Name);
        }

        /// <summary>
        /// The dialect comes from the product name the driver reports, here SQL Server. That the version is
        /// also picked up is covered by <see cref="AnOffsetIsHonoured"/>.
        /// </summary>
        [Fact]
        public void TheDialectIsTheOneTheDriverReports()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_server.OdbcDataSource);

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
        /// The ODBC catalog spells nullability as <c>NULLABLE</c>, an integer, where the information schema
        /// spells it <c>IS_NULLABLE</c> and <c>YES</c>.
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
        // ODBC reports money as SQL_DECIMAL with its precision and scale
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

        [Fact]
        public void EveryColumnTypeIsMapped()
        {
            Assert.Equal(26, Fields("TYPES").Count);
        }

        /// <summary>
        /// Every column the driver can read is read. <c>C_TIME</c> and <c>C_DATETIMEOFFSET</c> are left out; see
        /// <see cref="TheDriverCannotReadSqlServersOwnTimeTypes"/>.
        /// </summary>
        [Fact]
        public void EveryReadableColumnTypeCanBeRead()
        {
            Assert.Equal(2, Rows("""
                SELECT ID, C_BIT, C_TINYINT, C_SMALLINT, C_BIGINT, C_DECIMAL, C_NUMERIC, C_MONEY, C_SMALLMONEY,
                       C_FLOAT, C_REAL, C_CHAR, C_VARCHAR, C_VARCHARMAX, C_NCHAR, C_NVARCHAR, C_DATE,
                       C_DATETIME, C_SMALLDATETIME, C_DATETIME2, C_BINARY, C_VARBINARY, C_GUID, C_XML
                FROM ADO.TYPES
                """).Count);
        }

        /// <summary>
        /// A driver limitation: <c>System.Data.Odbc</c> has no mapping for <c>SQL_SS_TIME2</c> or
        /// <c>SQL_SS_TIMESTAMPOFFSET</c>, and <c>TypeMap.FromSqlType</c> throws on either. The columns are still
        /// typed, because the metadata comes from the catalog; only reading one fails.
        /// </summary>
        [Fact]
        public void TheDriverCannotReadSqlServersOwnTimeTypes()
        {
            Assert.Equal(nameof(SqlTypeName.TIME), Fields("TYPES")["C_TIME"].getSqlTypeName().name());

            var thrown = Assert.ThrowsAny<ArgumentException>(() => Rows("SELECT C_TIME FROM ADO.TYPES"));
            Assert.Contains("SS_TIME_EX", thrown.Message);
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

        /// <summary>
        /// Depends on the dialect knowing the server version: below SQL Server 2012 <c>MssqlSqlDialect</c>
        /// writes <c>TOP(1)</c> and drops the offset, which would return the first row rather than the second.
        /// </summary>
        [Fact]
        public void AnOffsetIsHonoured()
        {
            Assert.Equal("Widget", Scalar("SELECT PRODUCT FROM ADO.SUPPLIERS ORDER BY LEAD_DAYS OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY"));
        }

        #endregion

    }

}
