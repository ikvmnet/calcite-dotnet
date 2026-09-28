using System.Data.Common;

using Apache.Calcite.Data.Internal;

using org.apache.calcite.config;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteConnectionStringBuilderTests
    {

        [Fact]
        public void Should_round_trip_known_keys()
        {
            var b = new CalciteConnectionStringBuilder
            {
                Model = "inline:{}",
                Schema = "FOO",
                CaseSensitive = true,
                Conformance = "DEFAULT",
            };

            var rebuilt = new CalciteConnectionStringBuilder(b.ConnectionString);

            Assert.Equal("inline:{}", rebuilt.Model);
            Assert.Equal("FOO", rebuilt.Schema);
            Assert.True(rebuilt.CaseSensitive);
            Assert.Equal("DEFAULT", rebuilt.Conformance);
        }

        [Fact]
        public void Should_clear_value_when_set_to_null()
        {
            var b = new CalciteConnectionStringBuilder { Model = "inline:{}" };
            b.Model = null;

            Assert.Null(b.Model);
            Assert.False(b.ContainsKey(CalciteConnectionStringBuilder.ModelKey));
        }

        [Fact]
        public void Should_clear_case_sensitive_when_set_to_null()
        {
            var b = new CalciteConnectionStringBuilder { CaseSensitive = true };
            b.CaseSensitive = null;

            Assert.Null(b.CaseSensitive);
            Assert.False(b.ContainsKey(CalciteConnectionStringBuilder.CaseSensitiveKey));
        }

        [Fact]
        public void Should_parse_case_sensitive_from_string()
        {
            var b = new CalciteConnectionStringBuilder("CaseSensitive=true");
            Assert.True(b.CaseSensitive);
        }

        [Fact]
        public void Should_preserve_unknown_keys()
        {
            var b = new CalciteConnectionStringBuilder("Model=inline:{};CustomKey=hello");
            Assert.True(b.ContainsKey("CustomKey"));
            Assert.Equal("hello", b["CustomKey"]);
        }

        [Fact]
        public void EnumerateKeys_should_include_all_set_keys()
        {
            var b = new CalciteConnectionStringBuilder
            {
                Model = "inline:{}",
                Schema = "S",
            };

            Assert.Contains(CalciteConnectionStringBuilder.ModelKey, b.EnumerateKeys());
            Assert.Contains(CalciteConnectionStringBuilder.SchemaKey, b.EnumerateKeys());
        }

        [Fact]
        public void Empty_builder_should_have_no_known_values()
        {
            var b = new CalciteConnectionStringBuilder();

            Assert.Null(b.Model);
            Assert.Null(b.Schema);
            Assert.Null(b.CaseSensitive);
            Assert.Null(b.Conformance);
        }

        [Fact]
        public void Should_be_assignable_to_DbConnectionStringBuilder()
        {
            DbConnectionStringBuilder b = new CalciteConnectionStringBuilder();
            Assert.NotNull(b);
        }

        [Fact]
        public void Pooling_should_round_trip_and_default_to_unset()
        {
            var b = new CalciteConnectionStringBuilder();
            Assert.Null(b.Pooling);

            b.Pooling = false;
            var rebuilt = new CalciteConnectionStringBuilder(b.ConnectionString);
            Assert.False(rebuilt.Pooling);

            rebuilt.Pooling = null;
            Assert.False(rebuilt.ContainsKey(CalciteConnectionStringBuilder.PoolingKey));
        }

        /// <summary>
        /// The key a data source is looked up by is the same for every spelling of one connection string.
        /// </summary>
        [Fact]
        public void DataSourceKey_should_ignore_order_and_casing()
        {
            var a = new CalciteConnectionStringBuilder("Model=inline:{};Schema=S;Lex=MYSQL");
            var b = new CalciteConnectionStringBuilder("lex=MYSQL;SCHEMA=S;model=inline:{}");
            var c = new CalciteConnectionStringBuilder("Model=inline:{};Schema=T;Lex=MYSQL");

            Assert.Equal(a.DataSourceKey, b.DataSourceKey);
            Assert.NotEqual(a.DataSourceKey, c.DataSourceKey);
        }

        /// <summary>
        /// The engine properties carry the setting under the name Calcite reads it by.
        /// </summary>
        /// <remarks>
        /// That name is the property's <c>camelName()</c>, not the connection string key:
        /// <c>DbConnectionStringBuilder</c> lower-cases keywords it parses, and <c>CalciteEngineProperties</c>
        /// maps them back to Calcite's names. The value is checked with Calcite's own read,
        /// <c>wrap(properties).getBoolean()</c>, rather than as a string, since <c>Build</c> writes a
        /// <see cref="bool"/> as <c>True</c> and Calcite parses it case-insensitively.
        /// </remarks>
        [Fact]
        public void TopDownGeneralDecorrelationEnabled_should_round_trip_and_default_to_unset()
        {
            var b = new CalciteConnectionStringBuilder();
            Assert.Null(b.TopDownGeneralDecorrelationEnabled);

            b.TopDownGeneralDecorrelationEnabled = true;
            var rebuilt = new CalciteConnectionStringBuilder(b.ConnectionString);
            Assert.True(rebuilt.TopDownGeneralDecorrelationEnabled);

            var engine = CalciteEngineProperties.Build(rebuilt);
            Assert.NotNull(engine.getProperty(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED.camelName()));
            Assert.True(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED.wrap(engine).getBoolean());

            rebuilt.TopDownGeneralDecorrelationEnabled = null;
            Assert.False(rebuilt.ContainsKey(CalciteConnectionStringBuilder.TopDownGeneralDecorrelationEnabledKey));
        }

        [Fact]
        public void Pool_lifetimes_should_round_trip_and_default_to_unset()
        {
            var b = new CalciteConnectionStringBuilder();
            Assert.Null(b.ConnectionIdleLifetime);
            Assert.Null(b.ConnectionPruningInterval);

            b.ConnectionIdleLifetime = 60;
            b.ConnectionPruningInterval = 5;
            var rebuilt = new CalciteConnectionStringBuilder(b.ConnectionString);
            Assert.Equal(60, rebuilt.ConnectionIdleLifetime);
            Assert.Equal(5, rebuilt.ConnectionPruningInterval);

            var spelled = new CalciteConnectionStringBuilder("Connection Idle Lifetime=60;Connection Pruning Interval=5");
            Assert.Equal(60, spelled.ConnectionIdleLifetime);
            Assert.Equal(5, spelled.ConnectionPruningInterval);
        }

    }

}
