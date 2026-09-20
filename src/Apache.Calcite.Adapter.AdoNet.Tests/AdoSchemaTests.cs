using System;
using System.Collections.Generic;
using System.Linq;

using FluentAssertions;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;
using Xunit.Sdk;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers what the adapter discovers about a database, and the Calcite types it derives from it.
    /// </summary>
    public class AdoSchemaTests : IDisposable
    {

        SqliteFixture _sqlite = null!;
        org.apache.calcite.schema.SchemaPlus _root = null!;
        AdoSchema _schema = null!;
        JavaTypeFactoryImpl _types = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public AdoSchemaTests()
        {
            _sqlite = new SqliteFixture();
            _types = new JavaTypeFactoryImpl();

            // a real parent: AdoSchema derives its own expression from one, and cannot be given null
            _root = org.apache.calcite.tools.Frameworks.createRootSchema(true);
            _schema = AdoSchema.Create(_root, "ADO", _sqlite.DataSource, null, null);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Returns the names of the columns of a table, in order.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        List<string> Columns(string tableName)
        {
            var fields = Table(tableName).getRowType(_types).getFieldList();

            var names = new List<string>(fields.size());
            for (int i = 0; i < fields.size(); i++)
                names.Add(((RelDataTypeField)fields.get(i)).getName());

            return names;
        }

        /// <summary>
        /// Returns a table by name.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        org.apache.calcite.schema.Table Table(string tableName)
        {
            return (org.apache.calcite.schema.Table?)_schema.tables().get(tableName)
                ?? throw new XunitException($"no table {tableName}; found {string.Join(", ", TableNames())}");
        }

        /// <summary>
        /// Returns every table name the schema exposes.
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> TableNames()
        {
            var names = _schema.tables().getNames(org.apache.calcite.schema.lookup.LikePattern.any());

            var result = new List<string>();
            for (var i = names.iterator(); i.hasNext();)
                result.Add((string)i.next());

            return result;
        }

        /// <summary>
        /// Returns the Calcite type of a named column.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        RelDataType TypeOf(string tableName, string columnName)
        {
            var fields = Table(tableName).getRowType(_types).getFieldList();

            for (int i = 0; i < fields.size(); i++)
                if (((RelDataTypeField)fields.get(i)).getName().Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    return ((RelDataTypeField)fields.get(i)).getType();

            throw new XunitException($"no column {columnName} on {tableName}");
        }

        #region Discovery

        [Fact]
        public void TheSchemaFindsTheTables()
        {
            var names = TableNames().ToList();

            Assert.Contains("EMPS", names);
            Assert.Contains("DEPTS", names);
        }

        [Fact]
        public void AMissingTableIsAbsentRatherThanEmpty()
        {
            Assert.Null(_schema.tables().get("NO_SUCH_TABLE"));
        }

        [Fact]
        public void ATableIsAnAdoTable()
        {
            Assert.IsAssignableFrom<AdoTable>(Table("EMPS"));
        }

        [Fact]
        public void ATableKnowsItsDataSource()
        {
            Assert.NotNull(((AdoTable)Table("EMPS")).Schema.DataSource);
        }

        #endregion

        #region Row types

        [Fact]
        public void ColumnsAreDiscoveredInOrder()
        {
            Assert.Equal(
                new[] { "EMPNO", "NAME", "DEPTNO", "SALARY", "HIREDATE" },
                Columns("EMPS"));
        }

        [Fact]
        public void DeptsHasItsOwnColumns()
        {
            Assert.Equal(new[] { "DEPTNO", "DNAME" }, Columns("DEPTS"));
        }

        [Fact]
        public void AnIntegerColumnBecomesAnIntegralSqlType()
        {
            var name = TypeOf("EMPS", "EMPNO").getSqlTypeName().name();

            new[] { nameof(SqlTypeName.INTEGER), nameof(SqlTypeName.BIGINT), nameof(SqlTypeName.SMALLINT) }.Should().Contain(
                name,
                $"EMPNO came back as {name}");
        }

        [Fact]
        public void ATextColumnBecomesACharacterSqlType()
        {
            var name = TypeOf("EMPS", "NAME").getSqlTypeName().name();

            new[] { nameof(SqlTypeName.VARCHAR), nameof(SqlTypeName.CHAR) }.Should().Contain(
                name,
                $"NAME came back as {name}");
        }

        [Fact]
        public void ARealColumnBecomesAnApproximateSqlType()
        {
            var name = TypeOf("EMPS", "SALARY").getSqlTypeName().name();

            new[] { nameof(SqlTypeName.DOUBLE), nameof(SqlTypeName.REAL), nameof(SqlTypeName.FLOAT), nameof(SqlTypeName.DECIMAL) }.Should().Contain(
                name,
                $"SALARY came back as {name}");
        }

        /// <summary>
        /// Nullability is carried onto the Calcite type, which is what lets the planner reason about null
        /// rather than discovering one at runtime.
        /// </summary>
        [Fact]
        public void NullabilityIsCarriedOntoTheType()
        {
            Assert.False(TypeOf("EMPS", "EMPNO").isNullable(), "EMPNO is declared NOT NULL");
            Assert.True(TypeOf("EMPS", "DEPTNO").isNullable(), "DEPTNO is declared NULL");
        }

        [Fact]
        public void TheRowTypeIsStableAcrossCalls()
        {
            var first = Table("EMPS").getRowType(_types);
            var second = Table("EMPS").getRowType(_types);

            Assert.Equal(first.getFullTypeString(), second.getFullTypeString());
        }

        #endregion

        #region Metadata selection

        /// <summary>
        /// The factory picks a metadata provider from the connection type, which is what lets one adapter
        /// serve providers that describe themselves differently.
        /// </summary>
        [Fact]
        public void ASqliteConnectionSelectsTheSqliteMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_sqlite.DataSource);
            Assert.Equal("SqliteDatabaseMetadata", metadata.GetType().Name);
        }

        [Fact]
        public void TheMetadataOffersADialect()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_sqlite.DataSource);
            Assert.NotNull(metadata.Dialect);
        }

        #endregion

        #region Change

        /// <summary>
        /// A table created after the schema was built is still reachable: a schema that cached its first
        /// answer forever would make a long-lived connection wrong.
        /// </summary>
        [Fact]
        public void ATableAddedLaterCanBeFound()
        {
            _sqlite.Execute("CREATE TABLE LATE (ID INTEGER NOT NULL)");

            _schema = AdoSchema.Create(_root, "ADO", _sqlite.DataSource, null, null);
            Assert.Contains("LATE", TableNames().ToList());
        }

        #endregion

    }

}
