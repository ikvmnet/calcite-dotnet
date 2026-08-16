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
    /// Covers what the adapter discovers about a database, and the Calcite types it derives from it.
    /// </summary>
    [TestClass]
    public class AdoSchemaTests
    {

        SqliteFixture _sqlite = null!;
        org.apache.calcite.schema.SchemaPlus _root = null!;
        AdoSchema _schema = null!;
        JavaTypeFactoryImpl _types = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();
            _types = new JavaTypeFactoryImpl();

            // a real parent: AdoSchema derives its own expression from one, and cannot be given null
            _root = org.apache.calcite.tools.Frameworks.createRootSchema(true);
            _schema = AdoSchema.Create(_root, "ADO", _sqlite.DataSource, null, null);
        }

        [TestCleanup]
        public void Cleanup()
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
                ?? throw new AssertFailedException($"no table {tableName}; found {string.Join(", ", TableNames())}");
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

            throw new AssertFailedException($"no column {columnName} on {tableName}");
        }

        #region Discovery

        [TestMethod]
        public void TheSchemaFindsTheTables()
        {
            var names = TableNames().ToList();

            CollectionAssert.Contains(names, "EMPS");
            CollectionAssert.Contains(names, "DEPTS");
        }

        [TestMethod]
        public void AMissingTableIsAbsentRatherThanEmpty()
        {
            Assert.IsNull(_schema.tables().get("NO_SUCH_TABLE"));
        }

        [TestMethod]
        public void ATableIsAnAdoTable()
        {
            Assert.IsInstanceOfType<AdoTable>(Table("EMPS"));
        }

        [TestMethod]
        public void ATableKnowsItsDataSource()
        {
            Assert.IsNotNull(((AdoTable)Table("EMPS")).Schema.DataSource);
        }

        #endregion

        #region Row types

        [TestMethod]
        public void ColumnsAreDiscoveredInOrder()
        {
            CollectionAssert.AreEqual(
                new[] { "EMPNO", "NAME", "DEPTNO", "SALARY", "HIREDATE" },
                Columns("EMPS"));
        }

        [TestMethod]
        public void DeptsHasItsOwnColumns()
        {
            CollectionAssert.AreEqual(new[] { "DEPTNO", "DNAME" }, Columns("DEPTS"));
        }

        [TestMethod]
        public void AnIntegerColumnBecomesAnIntegralSqlType()
        {
            var name = TypeOf("EMPS", "EMPNO").getSqlTypeName().name();

            CollectionAssert.Contains(
                new[] { nameof(SqlTypeName.INTEGER), nameof(SqlTypeName.BIGINT), nameof(SqlTypeName.SMALLINT) },
                name,
                $"EMPNO came back as {name}");
        }

        [TestMethod]
        public void ATextColumnBecomesACharacterSqlType()
        {
            var name = TypeOf("EMPS", "NAME").getSqlTypeName().name();

            CollectionAssert.Contains(
                new[] { nameof(SqlTypeName.VARCHAR), nameof(SqlTypeName.CHAR) },
                name,
                $"NAME came back as {name}");
        }

        [TestMethod]
        public void ARealColumnBecomesAnApproximateSqlType()
        {
            var name = TypeOf("EMPS", "SALARY").getSqlTypeName().name();

            CollectionAssert.Contains(
                new[] { nameof(SqlTypeName.DOUBLE), nameof(SqlTypeName.REAL), nameof(SqlTypeName.FLOAT), nameof(SqlTypeName.DECIMAL) },
                name,
                $"SALARY came back as {name}");
        }

        /// <summary>
        /// Nullability is carried onto the Calcite type, which is what lets the planner reason about null
        /// rather than discovering one at runtime.
        /// </summary>
        [TestMethod]
        public void NullabilityIsCarriedOntoTheType()
        {
            Assert.IsFalse(TypeOf("EMPS", "EMPNO").isNullable(), "EMPNO is declared NOT NULL");
            Assert.IsTrue(TypeOf("EMPS", "DEPTNO").isNullable(), "DEPTNO is declared NULL");
        }

        [TestMethod]
        public void TheRowTypeIsStableAcrossCalls()
        {
            var first = Table("EMPS").getRowType(_types);
            var second = Table("EMPS").getRowType(_types);

            Assert.AreEqual(first.getFullTypeString(), second.getFullTypeString());
        }

        #endregion

        #region Metadata selection

        /// <summary>
        /// The factory picks a metadata provider from the connection type, which is what lets one adapter
        /// serve providers that describe themselves differently.
        /// </summary>
        [TestMethod]
        public void ASqliteConnectionSelectsTheSqliteMetadata()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_sqlite.DataSource);
            Assert.AreEqual("SqliteDatabaseMetadata", metadata.GetType().Name);
        }

        [TestMethod]
        public void TheMetadataOffersADialect()
        {
            var metadata = Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(_sqlite.DataSource);
            Assert.IsNotNull(metadata.Dialect);
        }

        #endregion

        #region Change

        /// <summary>
        /// A table created after the schema was built is still reachable: a schema that cached its first
        /// answer forever would make a long-lived connection wrong.
        /// </summary>
        [TestMethod]
        public void ATableAddedLaterCanBeFound()
        {
            _sqlite.Execute("CREATE TABLE LATE (ID INTEGER NOT NULL)");

            _schema = AdoSchema.Create(_root, "ADO", _sqlite.DataSource, null, null);
            CollectionAssert.Contains(TableNames().ToList(), "LATE");
        }

        #endregion

    }

}
