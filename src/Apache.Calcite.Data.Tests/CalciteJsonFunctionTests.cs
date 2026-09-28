using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Verifies the SQL JSON functions execute end to end.
    /// </summary>
    /// <remarks>
    /// Calcite implements these through json-path's jackson providers. json-path's POM does not declare the
    /// jackson dependencies; the <c>Dependencies</c> element on the calcite-core reference supplies them. A
    /// failure here usually means json-path was compiled without jackson available.
    /// </remarks>
    public class CalciteJsonFunctionTests
    {

        [Fact]
        public void JsonValue_should_extract_scalar()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES JSON_VALUE('{\"a\": 7}', '$.a')";

            var v = cmd.ExecuteScalar();
            Assert.Equal("7", v);
        }

        [Fact]
        public void JsonExists_should_return_true_for_present_path()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES JSON_EXISTS('{\"a\": {\"b\": 1}}', '$.a.b')";

            var v = cmd.ExecuteScalar();
            Assert.Equal(true, v);
        }

        [Fact]
        public void JsonQuery_should_return_object()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES JSON_QUERY('{\"a\": {\"b\": 1}}', '$.a')";

            var v = cmd.ExecuteScalar();
            Assert.Equal("{\"b\":1}", v);
        }

        /// <summary>
        /// <c>JSON_QUERY</c> with a <c>RETURNING</c> clause naming an array type reads a JSON array as a
        /// collection rather than as text.
        /// </summary>
        /// <remarks>
        /// <c>JSON_VALUE</c> accepts the same clause but returns null for an array: its runtime handles only
        /// scalars, and the default <c>NULL ON ERROR</c> suppresses the error. That is Calcite's behaviour.
        /// </remarks>
        [Fact]
        public void JsonQuery_should_return_an_array()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT JSON_QUERY('{\"c\":[\"a\",\"b\",\"c\"]}', '$.c' RETURNING VARCHAR ARRAY) AS A";

            using var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(typeof(string[]), r.GetFieldType(0));
            Assert.Equal(new[] { "a", "b", "c" }, Assert.IsType<string[]>(r.GetArray(0)));
        }

    }

}
