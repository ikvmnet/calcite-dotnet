using System;
using System.Data;
using System.Data.Common;


using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteDataReaderTests
    {

        static CalciteDataReaderTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.server.ServerDdlExecutor).Assembly);
        }

        const string MultiRowQuery = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(x, y)";

        /// <summary>
        /// A column origin needs a real table, and DDL is how this suite gets one; see
        /// <see cref="CalciteDdlTests"/>. Everything else here queries <c>VALUES</c>, which is nothing's
        /// origin: Calcite gives such a column the alias as its <c>columnName</c> as well as its label,
        /// so the two agree and a name test over a derived table cannot tell them apart.
        /// </summary>
        static readonly string ServerDdlConnectionString = new CalciteConnectionStringBuilder
        {
            Model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}",
            ParserFactory = "org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY",
            Schema = "adhoc",
        };

        [Fact]
        public void Should_enumerate_all_rows()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            var rows = 0;
            while (r.Read())
                rows++;

            Assert.Equal(3, rows);
        }

        [Fact]
        public void Should_expose_field_count_and_names()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            Assert.Equal(2, r.FieldCount);
            Assert.Equal("X", r.GetName(0), ignoreCase: true);
            Assert.Equal("Y", r.GetName(1), ignoreCase: true);
        }

        [Fact]
        public void GetOrdinal_should_resolve_column_name_case_insensitively()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            Assert.Equal(0, r.GetOrdinal("X"));
            Assert.Equal(0, r.GetOrdinal("x"));
        }

        [Fact]
        public void GetOrdinal_should_throw_for_unknown_column()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            Assert.Throws<IndexOutOfRangeException>(() => r.GetOrdinal("missing"));
        }

        [Fact]
        public void Indexer_by_name_should_return_value()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            r.Read();
            Assert.Equal(1, Convert.ToInt32(r["X"]));
            Assert.Equal("a", Convert.ToString(r["Y"]));
        }

        [Fact]
        public void GetValues_should_fill_array()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            r.Read();
            var buf = new object[2];
            var n = r.GetValues(buf);
            Assert.Equal(2, n);
            Assert.Equal(1, Convert.ToInt32(buf[0]));
            Assert.Equal("a", Convert.ToString(buf[1]));
        }

        [Fact]
        public void IsDBNull_should_be_true_for_null_value()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(NULL AS INTEGER))";
            using var r = cmd.ExecuteReader();

            Assert.True(r.Read());
            Assert.True(r.IsDBNull(0));
        }

        [Fact]
        public void GetSchemaTable_should_describe_columns()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            var t = r.GetSchemaTable();
            Assert.NotNull(t);
            Assert.Equal(2, t!.Rows.Count);
        }

        [Fact]
        public void NextResult_should_return_false()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 1";
            using var r = cmd.ExecuteReader();

            Assert.False(r.NextResult());
        }

        [Fact]
        public void Reading_after_close_should_throw()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 1";
            var r = cmd.ExecuteReader();
            r.Close();

            Assert.True(r.IsClosed);
            Assert.Throws<InvalidOperationException>(() => r.Read());
        }

        [Fact]
        public void GetValue_before_read_should_throw()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES 1";
            using var r = cmd.ExecuteReader();

            Assert.Throws<InvalidOperationException>(() => r.GetValue(0));
        }

        [Fact]
        public void GetFieldType_should_return_clr_type()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = MultiRowQuery;
            using var r = cmd.ExecuteReader();

            Assert.Equal(typeof(int), r.GetFieldType(0));
            Assert.Equal(typeof(string), r.GetFieldType(1));
        }

        [Fact]
        public void GetChar_should_convert_single_character_char_column()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES CAST('a' AS CHAR(1))";
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());

            Assert.Equal('a', r.GetChar(0));
        }

        [Fact]
        public void GetChar_should_refuse_char_column_longer_than_one()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES CAST('ab' AS CHAR(2))";
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());

            Assert.Throws<InvalidCastException>(() => r.GetChar(0));
        }

        [Fact]
        public void GetChar_should_refuse_varchar_column_of_length_one()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES CAST('a' AS VARCHAR(1))";
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());

            Assert.Throws<InvalidCastException>(() => r.GetChar(0));
        }

        [Fact]
        public void GetName_should_return_alias_label()
        {
            using var c = new CalciteConnection(ServerDdlConnectionString);
            c.Open();

            using (var ddl = c.CreateCommand())
            {
                ddl.CommandText = "CREATE TABLE IF NOT EXISTS \"labeltest\" (\"PostalCode\" INTEGER NOT NULL, \"City\" VARCHAR(64))";
                ddl.ExecuteNonQuery();
            }

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT \"PostalCode\", \"PostalCode\" AS \"Foo\", \"City\" FROM \"labeltest\"";
            using var r = cmd.ExecuteReader();

            // both projections have PostalCode as their origin column, so the origin name reports the
            // first two as one name; the label is what tells them apart, and a consumer keying the
            // schema by name — EF Core's FromSqlQueryingEnumerable.BuildIndexMap — throws on the duplicate
            Assert.Equal("PostalCode", r.GetName(0));
            Assert.Equal("Foo", r.GetName(1));
            Assert.Equal("City", r.GetName(2));

            // GetOrdinal and GetSchemaTable read the same accessor
            Assert.Equal(1, r.GetOrdinal("Foo"));
            Assert.Equal("Foo", r.GetSchemaTable().Rows[1][SchemaTableColumn.ColumnName]);
        }

    }

}
