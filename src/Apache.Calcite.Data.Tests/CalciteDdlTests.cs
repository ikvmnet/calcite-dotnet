using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Tests that verify DDL execution via the ADO.NET surface when <c>serverDdl</c> is enabled.
    /// </summary>
    public class CalciteDdlTests
    {

        static CalciteDdlTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.server.ServerDdlExecutor).Assembly);
        }

        static readonly string ServerDdlConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            Schema = "adhoc",
        }.ConnectionString;

        [Fact]
        public void CreateSchema_IfNotExists_should_succeed_when_serverDdl_is_enabled()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"myschema\"";
            var affected = cmd.ExecuteNonQuery();
            Assert.True(affected >= 0);
        }

        [Fact]
        public void CreateSchema_IfNotExists_should_be_idempotent()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"myschema\"";
            cmd.ExecuteNonQuery();

            // Running the same statement a second time should not throw.
            var ex = Record.Exception(() => cmd.ExecuteNonQuery());
            Assert.Null(ex);
        }

        [Fact]
        public void ExecuteReader_after_ddl_should_return_empty_result_set()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"readertest\"";
            using var r = cmd.ExecuteReader();

            Assert.Equal(0, r.FieldCount);
            Assert.False(r.Read());
        }

        [Fact]
        public void ExecuteReaderAsync_after_ddl_should_return_empty_result_set()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "CREATE SCHEMA IF NOT EXISTS \"readertest2\"";
            using var r = cmd.ExecuteReaderAsync().GetAwaiter().GetResult();

            Assert.Equal(0, r.FieldCount);
            Assert.False(r.Read());
        }

        [Fact]
        public void ExecuteNonQuery_insert_should_return_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"dmltest\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"dmltest\" VALUES (2)";
            var affected = cmd.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        static readonly string SchemaPropertyOnlyConnectionString = new CalciteConnectionStringBuilder
        {
            // Model has NO defaultSchema — the connection-string Schema property is the sole source.
            Model = "inline:{\"version\":\"1.0\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            Schema = "adhoc",
        }.ConnectionString;

        static readonly string ModelDefaultSchemaOnlyConnectionString = new CalciteConnectionStringBuilder
        {
            // Model carries defaultSchema — no Schema property on the connection string.
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
        }.ConnectionString;

        [Fact]
        public void CreateTable_with_connection_string_schema_property_should_land_in_that_schema()
        {
            using var c = new CalciteConnection(SchemaPropertyOnlyConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"cs_schema_tbl\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            var t = c.GetSchema(CalciteSchemaInfo.Tables);
            foreach (System.Data.DataRow row in t.Rows)
            {
                if ((string)row["TABLE_SCHEMA"] == "adhoc" &&
                    (string)row["TABLE_NAME"] == "cs_schema_tbl")
                    return;
            }

            Assert.Fail("Expected table created via connection-string Schema to appear in the 'adhoc' schema.");
        }

        [Fact]
        public void CreateTable_with_model_defaultSchema_should_land_in_that_schema()
        {
            using var c = new CalciteConnection(ModelDefaultSchemaOnlyConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"model_schema_tbl\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            var t = c.GetSchema(CalciteSchemaInfo.Tables);
            foreach (System.Data.DataRow row in t.Rows)
            {
                if ((string)row["TABLE_SCHEMA"] == "adhoc" &&
                    (string)row["TABLE_NAME"] == "model_schema_tbl")
                    return;
            }

            Assert.Fail("Expected table created via model defaultSchema to appear in the 'adhoc' schema.");
        }

        [Fact]
        public void CreateTable_should_be_visible_in_get_schema_tables()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"schema_table_test\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            var t = c.GetSchema(CalciteSchemaInfo.Tables);
            foreach (System.Data.DataRow row in t.Rows)
            {
                if ((string)row["TABLE_SCHEMA"] == "adhoc" &&
                    (string)row["TABLE_NAME"] == "schema_table_test" &&
                    (string)row["TABLE_TYPE"] == "TABLE")
                    return;
            }

            Assert.Fail("Expected dynamically created table to appear in GetSchema(\"Tables\").");
        }

        [Fact]
        public void CreateTable_should_be_visible_in_get_schema_columns_with_expected_values()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"schema_columns_test\" (\"amount\" DECIMAL(18, 4) NOT NULL, \"name\" VARCHAR(100))";
            cmd.ExecuteNonQuery();

            var t = c.GetSchema(CalciteSchemaInfo.Columns, [null, "adhoc", "schema_columns_test", null]);

            System.Data.DataRow? amountRow = null;
            System.Data.DataRow? nameRow = null;

            foreach (System.Data.DataRow row in t.Rows)
            {
                if ((string)row["COLUMN_NAME"] == "amount")
                    amountRow = row;
                else if ((string)row["COLUMN_NAME"] == "name")
                    nameRow = row;
            }

            Assert.NotNull(amountRow);
            Assert.NotNull(nameRow);

            Assert.Equal("adhoc", amountRow!["TABLE_SCHEMA"]);
            Assert.Equal("schema_columns_test", amountRow["TABLE_NAME"]);
            Assert.Equal(1, amountRow["ORDINAL_POSITION"]);
            Assert.Equal("NO", amountRow["IS_NULLABLE"]);
            Assert.Equal("DECIMAL", amountRow["DATA_TYPE"]);
            Assert.Equal(System.DBNull.Value, amountRow["CHARACTER_MAXIMUM_LENGTH"]);
            Assert.Equal(18, amountRow["NUMERIC_PRECISION"]);
            Assert.Equal(4, amountRow["NUMERIC_SCALE"]);

            Assert.Equal("adhoc", nameRow!["TABLE_SCHEMA"]);
            Assert.Equal("schema_columns_test", nameRow["TABLE_NAME"]);
            Assert.Equal(2, nameRow["ORDINAL_POSITION"]);
            Assert.Equal("YES", nameRow["IS_NULLABLE"]);
            Assert.Equal("VARCHAR", nameRow["DATA_TYPE"]);
            Assert.Equal(100, nameRow["CHARACTER_MAXIMUM_LENGTH"]);
            Assert.Equal(System.DBNull.Value, nameRow["NUMERIC_PRECISION"]);
            Assert.Equal(System.DBNull.Value, nameRow["NUMERIC_SCALE"]);
        }

        [Fact]
        public async Task ExecuteNonQueryAsync_insert_should_return_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"dmltest_async\" (\"id\" INTEGER NOT NULL)";
            await cmd.ExecuteNonQueryAsync();

            cmd.CommandText = "INSERT INTO \"dmltest_async\" VALUES (2)";
            var affected = await cmd.ExecuteNonQueryAsync();
            Assert.Equal(1, affected);
        }

        /// <summary>
        /// Creates a table without a schema qualifier and confirms that unqualified INSERT and
        /// SELECT work correctly, verifying the inserted data is readable via <see cref="System.Data.Common.DbDataReader"/>.
        /// </summary>
        [Fact]
        public void CreateTable_without_schema_should_support_insert_and_select()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"noschema_tbl\" (\"id\" INTEGER NOT NULL, \"val\" VARCHAR(100))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"noschema_tbl\" VALUES (1, 'hello')";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "INSERT INTO \"noschema_tbl\" VALUES (2, 'world')";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT \"id\", \"val\" FROM \"noschema_tbl\" ORDER BY \"id\"";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(1, reader.GetInt32(0));
            Assert.Equal("hello", reader.GetString(1));
            Assert.True(reader.Read());
            Assert.Equal(2, reader.GetInt32(0));
            Assert.Equal("world", reader.GetString(1));
            Assert.False(reader.Read());
        }

        [Fact]
        public void Insert_then_update_then_select_should_reflect_updated_value()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"update_tbl\" (\"id\" INTEGER NOT NULL, \"val\" VARCHAR(100))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"update_tbl\" VALUES (1, 'original')";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "UPDATE \"update_tbl\" SET \"val\" = 'updated' WHERE \"id\" = 1";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT \"val\" FROM \"update_tbl\" WHERE \"id\" = 1";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("updated", reader.GetString(0));
            Assert.False(reader.Read());
        }

        [Fact]
        public void Insert_then_update_decimal_then_select_should_reflect_updated_value()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"decimal_update_tbl\" (\"id\" INTEGER NOT NULL, \"amount\" DECIMAL(18, 4))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"decimal_update_tbl\" VALUES (1, 9.99)";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "UPDATE \"decimal_update_tbl\" SET \"amount\" = 19.99 WHERE \"id\" = 1";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT \"amount\" FROM \"decimal_update_tbl\" WHERE \"id\" = 1";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(19.99m, reader.GetDecimal(0));
            Assert.False(reader.Read());
        }

        [Fact]
        public void Insert_then_update_string_decimal_int_then_select_should_reflect_updated_values()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"multi_update_tbl\" (\"id\" INTEGER NOT NULL, \"label\" VARCHAR(100), \"amount\" DECIMAL(18, 4), \"qty\" INTEGER)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"multi_update_tbl\" VALUES (1, 'before', 7.5, 10)";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "UPDATE \"multi_update_tbl\" SET \"label\" = 'after', \"amount\" = 2.50, \"qty\" = 20 WHERE \"id\" = 1";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT \"label\", \"amount\", \"qty\" FROM \"multi_update_tbl\" WHERE \"id\" = 1";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("after", reader.GetString(0));
            Assert.Equal(2.50m, reader.GetDecimal(1));
            Assert.Equal(20, reader.GetInt32(2));
            Assert.False(reader.Read());
        }

    }

}
