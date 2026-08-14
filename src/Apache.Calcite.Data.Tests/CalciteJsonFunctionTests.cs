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

    }

}
