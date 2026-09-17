using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Verifies BigQuery's <c>CONTAINS_SUBSTR</c> executes end-to-end. Calcite normalizes both operands
    /// through commons-text's <c>StringEscapeUtils.unescapeJava</c>, which calls commons-lang3; a regression
    /// here most likely means the closure has resolved a commons-lang3 older than the one commons-text was
    /// built against, and org.apache.commons.text.dll is once again compiling against lang3 stubs.
    /// </summary>
    public class CalciteContainsSubstrTests
    {

        const string ConnectionString = TestModels.InlineEmptyModelConnectionString + ";Fun=bigquery";

        [Fact]
        public void ContainsSubstr_should_match_case_insensitively()
        {
            using var c = new CalciteConnection(ConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES CONTAINS_SUBSTR('Hello World', 'hello')";

            var v = cmd.ExecuteScalar();
            Assert.Equal(true, v);
        }

        [Fact]
        public void ContainsSubstr_should_not_match_absent_substring()
        {
            using var c = new CalciteConnection(ConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES CONTAINS_SUBSTR('Hello World', 'goodbye')";

            var v = cmd.ExecuteScalar();
            Assert.Equal(false, v);
        }

        [Fact]
        public void ContainsSubstr_should_filter_rows()
        {
            using var c = new CalciteConnection(ConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText =
                "SELECT \"s\" FROM (VALUES (CAST('alpha' AS VARCHAR)), ('beta'), ('alphabet')) AS \"t\" (\"s\") " +
                "WHERE CONTAINS_SUBSTR(\"s\", 'ALPHA') ORDER BY \"s\"";

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal("alpha", r.GetString(0));
            Assert.True(r.Read());
            Assert.Equal("alphabet", r.GetString(0));
            Assert.False(r.Read());
        }

    }

}
