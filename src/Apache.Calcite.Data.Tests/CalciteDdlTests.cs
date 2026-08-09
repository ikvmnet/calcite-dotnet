using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

using Xunit;
using Apache.Calcite.Extensions.Adapter.Enumerable;

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
        };

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
        };

        static readonly string ModelDefaultSchemaOnlyConnectionString = new CalciteConnectionStringBuilder
        {
            // Model carries defaultSchema — no Schema property on the connection string.
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
        };

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

        /// <remarks>
        /// <para><b>Fails on calcite-core 1.43.0-SNAPSHOT, and the defect is upstream and one line.</b>
        /// <c>EnumerableTableModify.deleteFromCollection</c> (CALCITE-7510, commit 5cdc09b8c) declares the
        /// sink row as <c>Object</c> and then writes
        /// <c>Expressions.convert_(sinkRow, tablePhysType.getJavaRowType())</c>. For a one-column table that
        /// row type is a primitive — <c>deduceFormat</c> says ARRAY because the table's element type is
        /// <c>Object[]</c>, and the optimising <c>PhysTypeImpl.of</c> turns ARRAY into SCALAR for one field —
        /// so the generated source says <c>(int) sinkRow</c>.</para>
        /// <para><c>(int) someObject</c> is legal Java: JLS 5.5 allows a narrowing reference conversion
        /// followed by unboxing, and javac compiles it. <b>Janino does not implement it</b>, measured against
        /// the Janino on this classpath: <c>(int) o</c> gives "Cannot cast "java.lang.Object" to "int"",
        /// while <c>(java.lang.Integer) o</c> compiles. Calcite compiles with Janino, so it must not emit
        /// the first form. The fix is to box the target type —
        /// <c>Expressions.convert_(sinkRow, Primitive.box(tablePhysType.getJavaRowType()))</c> — which is a
        /// no-op for the multi-column <c>Object[]</c> case and so changes nothing that works today.</para>
        /// <para>Every test CALCITE-7510 added uses a two-column table, which is why this shape was never
        /// seen. Both of these tables are one column. They pass on 1.42.0 — where four UPDATE tests fail
        /// instead, because 1.42 is what CALCITE-7510 fixes.</para>
        /// <para>That was the whole story while Janino compiled the plan, and it no longer does: the default
        /// prepare is <c>ClrEnumerablePrepare</c>, which translates Calcite's tree rather than compiling it,
        /// so the cast Janino refuses costs nothing here. Measured — what remains is a different defect, and
        /// ours. A one-column table gives the scan a SCALAR physical type, because <c>PhysTypeImpl.of</c>
        /// collapses ARRAY to SCALAR for a single field, while the table still yields <c>Object[]</c> rows;
        /// an aggregate over it is built as <c>IEnumerable&lt;int&gt;</c> and handed
        /// <c>IEnumerable&lt;object[]&gt;</c>.</para>
        /// <para>Skipped, not deleted, and the skip message names the defect that holds it.</para>
        /// </remarks>
        [Fact]
        public void Delete_should_return_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"delete_tbl\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"delete_tbl\" VALUES (1)";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "INSERT INTO \"delete_tbl\" VALUES (2)";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "DELETE FROM \"delete_tbl\" WHERE \"id\" = 1";
            Assert.Equal(1, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT COUNT(*) FROM \"delete_tbl\"";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        }

        [Fact]
        public void MultiRow_insert_should_return_correct_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"multi_insert_tbl\" (\"id\" INTEGER NOT NULL, \"val\" VARCHAR(50))";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"multi_insert_tbl\" VALUES (1, 'a'), (2, 'b'), (3, 'c')";
            Assert.Equal(3, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT COUNT(*) FROM \"multi_insert_tbl\"";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(3L, reader.GetInt64(0));
        }

        [Fact]
        public void MultiRow_update_should_return_correct_row_count()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"multi_update_rows_tbl\" (\"id\" INTEGER NOT NULL, \"flag\" INTEGER)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"multi_update_rows_tbl\" VALUES (1, 0), (2, 0), (3, 1)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "UPDATE \"multi_update_rows_tbl\" SET \"flag\" = 1 WHERE \"flag\" = 0";
            Assert.Equal(2, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT COUNT(*) FROM \"multi_update_rows_tbl\" WHERE \"flag\" = 1";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(3L, reader.GetInt64(0));
        }

        /// <summary>
        /// Verifies that DELETE removes multiple rows and returns the correct affected-row count
        /// for a single-column table.
        ///
        /// <para>
        /// Note: Calcite's <c>EnumerableTableModify</c> uses <c>Collection.removeAll</c> backed by
        /// Java <c>Object[].equals</c> (reference equality) to locate rows to remove.  For
        /// single-column tables Calcite optimises the physical row format from ARRAY to SCALAR, so
        /// each row is a plain scalar value whose <c>equals</c> is value-based — reference identity
        /// is preserved through the scan, and <c>removeAll</c> succeeds.  Multi-column tables stay
        /// in ARRAY format and the conversion branch in <c>EnumerableTableModify.implement</c>
        /// creates brand-new <c>Object[]</c> instances, so <c>removeAll</c> finds no matches;
        /// this is a known Calcite limitation (CALCITE-style bug in the enumerable DELETE path).
        /// </para>
        /// </summary>
        /// <remarks>
        /// <para><b>Fails on calcite-core 1.43.0-SNAPSHOT, and the defect is upstream and one line.</b>
        /// <c>EnumerableTableModify.deleteFromCollection</c> (CALCITE-7510, commit 5cdc09b8c) declares the
        /// sink row as <c>Object</c> and then writes
        /// <c>Expressions.convert_(sinkRow, tablePhysType.getJavaRowType())</c>. For a one-column table that
        /// row type is a primitive — <c>deduceFormat</c> says ARRAY because the table's element type is
        /// <c>Object[]</c>, and the optimising <c>PhysTypeImpl.of</c> turns ARRAY into SCALAR for one field —
        /// so the generated source says <c>(int) sinkRow</c>.</para>
        /// <para><c>(int) someObject</c> is legal Java: JLS 5.5 allows a narrowing reference conversion
        /// followed by unboxing, and javac compiles it. <b>Janino does not implement it</b>, measured against
        /// the Janino on this classpath: <c>(int) o</c> gives "Cannot cast "java.lang.Object" to "int"",
        /// while <c>(java.lang.Integer) o</c> compiles. Calcite compiles with Janino, so it must not emit
        /// the first form. The fix is to box the target type —
        /// <c>Expressions.convert_(sinkRow, Primitive.box(tablePhysType.getJavaRowType()))</c> — which is a
        /// no-op for the multi-column <c>Object[]</c> case and so changes nothing that works today.</para>
        /// <para>Every test CALCITE-7510 added uses a two-column table, which is why this shape was never
        /// seen. Both of these tables are one column. They pass on 1.42.0 — where four UPDATE tests fail
        /// instead, because 1.42 is what CALCITE-7510 fixes.</para>
        /// <para>That was the whole story while Janino compiled the plan, and it no longer does: the default
        /// prepare is <c>ClrEnumerablePrepare</c>, which translates Calcite's tree rather than compiling it,
        /// so the cast Janino refuses costs nothing here. Measured — what remains is a different defect, and
        /// ours. A one-column table gives the scan a SCALAR physical type, because <c>PhysTypeImpl.of</c>
        /// collapses ARRAY to SCALAR for a single field, while the table still yields <c>Object[]</c> rows;
        /// an aggregate over it is built as <c>IEnumerable&lt;int&gt;</c> and handed
        /// <c>IEnumerable&lt;object[]&gt;</c>.</para>
        /// <para>Skipped, not deleted, and the skip message names the defect that holds it.</para>
        /// </remarks>
        [Fact]
        public void MultiRow_delete_should_return_correct_row_count_for_single_column_table()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();

            // Single-column table: Calcite uses SCALAR row format, so Object.equals is value-based
            // and Collection.removeAll correctly locates and removes the matching rows.
            cmd.CommandText = "CREATE TABLE IF NOT EXISTS \"multi_delete_sc_tbl\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"multi_delete_sc_tbl\" VALUES (1), (2), (3)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "DELETE FROM \"multi_delete_sc_tbl\" WHERE \"id\" < 3";
            Assert.Equal(2, cmd.ExecuteNonQuery());

            cmd.CommandText = "SELECT COUNT(*) FROM \"multi_delete_sc_tbl\"";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        }

        /// <summary>
        /// <c>CREATE TABLE ... AS SELECT</c> is refused, and the empty table is left behind.
        /// </summary>
        /// <remarks>
        /// <c>ServerDdlExecutor</c> adds the table and then calls <c>populate</c> to fill it, and
        /// <c>populate</c> loads rows through <c>context.getRelRunner().prepareStatement(rel)</c> — a
        /// <c>java.sql.PreparedStatement</c>, which this provider does not implement. So the column
        /// definitions survive and the rows do not. The ordering is upstream's; the refusal is
        /// <c>PrepareContext.getRelRunner</c>'s, and its message says which statements are affected.
        ///
        /// <para><c>CREATE MATERIALIZED VIEW</c> is the same path — see
        /// <c>CalciteViewTests.Create_materialized_view_should_be_refused_with_a_reason</c>. Asserted
        /// rather than skipped so that implementing a runner shows up here as a failure.</para>
        /// </remarks>
        [Fact]
        public void CreateTableAsSelect_should_be_refused_leaving_an_empty_table()
        {
            var rootDdl = new CalciteConnectionStringBuilder
            {
                ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            };

            using var c = new CalciteConnection(rootDdl);
            c.Open();
            using var cmd = c.CreateCommand();

            cmd.CommandText = "CREATE TABLE \"ctas_src\" (\"id\" INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO \"ctas_src\" VALUES (3), (4)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "CREATE TABLE \"ctas_dst\" AS SELECT \"id\" FROM \"ctas_src\"";
            var ex = Assert.ThrowsAny<System.Exception>(() => cmd.ExecuteNonQuery());
            Assert.Contains("CREATE TABLE ... AS SELECT", ex.ToString(), System.StringComparison.Ordinal);

            cmd.CommandText = "SELECT COUNT(*) FROM \"ctas_dst\"";
            using var reader2 = cmd.ExecuteReader();
            Assert.True(reader2.Read());
            Assert.Equal(0L, reader2.GetInt64(0));
        }

    }

}
