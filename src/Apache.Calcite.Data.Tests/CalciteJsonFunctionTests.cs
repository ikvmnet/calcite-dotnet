using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Verifies the SQL JSON functions execute end-to-end. Calcite implements these through json-path's
    /// jackson providers, whose dependencies are undeclared in json-path's published POM and are supplied
    /// by the <c>Dependencies</c> declaration on the calcite-core reference; a regression here most likely
    /// means json.path.dll is once again compiling against jackson stubs.
    /// </summary>
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
        /// A <c>RETURNING</c> clause naming an array type, which is the only spelling that reads a JSON
        /// array as a collection rather than as its text.
        /// </summary>
        /// <remarks>
        /// <c>JSON_VALUE</c> takes the same clause and answers null for an array, because its runtime is
        /// scalar-only and the default <c>NULL ON ERROR</c> swallows the refusal. That is Calcite's, not
        /// ours — both conventions agree on it, and so does Calcite's own JDBC driver on a JVM.
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
