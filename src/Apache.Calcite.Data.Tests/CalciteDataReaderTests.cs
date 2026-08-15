using System;
using System.Data;


using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteDataReaderTests
    {

        const string MultiRowQuery = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(x, y)";

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

    }

}
