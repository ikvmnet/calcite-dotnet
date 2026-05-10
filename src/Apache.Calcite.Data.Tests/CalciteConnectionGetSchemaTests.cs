using System;
using System.Data;
using System.Linq;

using Apache.Calcite.Data.Internal;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteConnectionGetSchemaTests
    {

        [Fact]
        public void GetSchema_with_no_args_should_return_metadata_collections()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema();

            Assert.Equal(CalciteSchemaInfo.MetaDataCollections, t.TableName);
            var names = t.Rows.Cast<DataRow>().Select(r => (string)r["CollectionName"]).ToArray();
            Assert.Contains(CalciteSchemaInfo.MetaDataCollections, names);
            Assert.Contains(CalciteSchemaInfo.Restrictions, names);
            Assert.Contains(CalciteSchemaInfo.DataSourceInformation, names);
            Assert.Contains(CalciteSchemaInfo.DataTypes, names);
            Assert.Contains(CalciteSchemaInfo.ReservedWords, names);
            Assert.Contains(CalciteSchemaInfo.Schemas, names);
            Assert.Contains(CalciteSchemaInfo.Tables, names);
            Assert.Contains(CalciteSchemaInfo.Columns, names);
        }

        [Fact]
        public void GetSchema_should_return_restrictions()
        {
            using var c = new CalciteConnection();
            var t = c.GetSchema(CalciteSchemaInfo.Restrictions);

            Assert.Equal(CalciteSchemaInfo.Restrictions, t.TableName);
            Assert.Contains(t.Rows.Cast<DataRow>(),
                r => (string)r["CollectionName"] == CalciteSchemaInfo.Tables &&
                     (string)r["RestrictionName"] == "Schema");
        }

        [Fact]
        public void GetSchema_should_return_data_source_information()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.DataSourceInformation);
            Assert.Single(t.Rows);
            var row = t.Rows[0];
            Assert.Equal("Apache Calcite", row["DataSourceProductName"]);
            Assert.Equal("?", row["ParameterMarkerFormat"]);
        }

        [Fact]
        public void GetSchema_should_return_data_types()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.DataTypes);
            var names = t.Rows.Cast<DataRow>().Select(r => (string)r["TypeName"]).ToArray();
            Assert.Contains("INTEGER", names);
            Assert.Contains("VARCHAR", names);
            Assert.Contains("TIMESTAMP", names);
            Assert.Contains("DECIMAL", names);
        }

        [Fact]
        public void GetSchema_should_return_reserved_words()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.ReservedWords);
            var words = t.Rows.Cast<DataRow>().Select(r => (string)r["ReservedWord"]).ToArray();
            Assert.Contains("SELECT", words);
            Assert.Contains("FROM", words);
            Assert.Contains("WHERE", words);
        }

        [Fact]
        public void GetSchema_should_return_schemas()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.Schemas);
            var schemas = t.Rows.Cast<DataRow>().Select(r => (string)r["SchemaName"]).ToArray();
            Assert.Contains(schemas, s => string.Equals(s, "adhoc", StringComparison.OrdinalIgnoreCase));
        }

        [Fact]
        public void GetSchema_schemas_should_apply_restriction_filter()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.Schemas, new string?[] { "does_not_exist" });
            Assert.Empty(t.Rows);
        }

        [Fact]
        public void GetSchema_tables_and_columns_should_enumerate_user_schema()
        {
            // The engine always exposes a built-in "metadata" schema containing real Table
            // instances (COLUMNS, TABLES, ...). It exercises GetSchema(Tables/Columns/Views)
            // without requiring the JDBC driver (which JSON view macros would).
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var schemas = c.GetSchema(CalciteSchemaInfo.Schemas).Rows.Cast<DataRow>()
                .Select(r => (string)r["SchemaName"]).ToArray();
            var schemaName = schemas.First(s => string.Equals(s, "metadata", StringComparison.OrdinalIgnoreCase));

            var allTables = c.GetSchema(CalciteSchemaInfo.Tables);
            var tableNames = allTables.Rows.Cast<DataRow>()
                .Where(r => string.Equals((string)r["TableSchema"], schemaName, StringComparison.Ordinal))
                .Select(r => (string)r["TableName"]).ToArray();
            Assert.Contains(tableNames, n => string.Equals(n, "COLUMNS", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(tableNames, n => string.Equals(n, "TABLES", StringComparison.OrdinalIgnoreCase));

            var tableName = tableNames.First(n => string.Equals(n, "COLUMNS", StringComparison.OrdinalIgnoreCase));
            var columns = c.GetSchema(CalciteSchemaInfo.Columns,
                new string?[] { null, schemaName, tableName, null });
            Assert.NotEmpty(columns.Rows);
            var columnNames = columns.Rows.Cast<DataRow>()
                .Select(r => ((string)r["ColumnName"]).ToUpperInvariant()).ToArray();
            Assert.Contains("TABLENAME", columnNames);
            Assert.Contains("COLUMNNAME", columnNames);

            // Single-column restriction should narrow the result to that column only.
            var single = c.GetSchema(CalciteSchemaInfo.Columns,
                new string?[] { null, schemaName, tableName, columns.Rows[0]["ColumnName"]!.ToString() });
            Assert.Single(single.Rows);
        }

        [Fact]
        public void GetSchema_should_throw_for_unknown_collection()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<ArgumentException>(() => c.GetSchema("not_a_real_collection"));
        }

        [Fact]
        public void GetSchema_should_throw_when_collection_name_is_null()
        {
            using var c = new CalciteConnection();
            Assert.Throws<ArgumentNullException>(() => c.GetSchema(null!));
        }

    }

}
